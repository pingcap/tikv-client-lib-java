/*
 *
 * Copyright 2017 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.pingcap.tikv.region;

import static com.pingcap.tikv.util.KeyRangeUtils.makeRange;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
import com.google.protobuf.ByteString;
import com.pingcap.tikv.ReadOnlyPDClient;
import com.pingcap.tikv.TiSession;
import com.pingcap.tikv.exception.GrpcException;
import com.pingcap.tikv.exception.TiClientInternalException;
import com.pingcap.tikv.kvproto.Kvrpcpb.CommandPri;
import com.pingcap.tikv.kvproto.Kvrpcpb.IsolationLevel;
import com.pingcap.tikv.kvproto.Metapb.Peer;
import com.pingcap.tikv.kvproto.Metapb.Region;
import com.pingcap.tikv.kvproto.Metapb.Store;
import com.pingcap.tikv.kvproto.Metapb.StoreState;
import com.pingcap.tikv.util.Comparables;
import com.pingcap.tikv.util.Pair;
import java.util.List;

public class RegionManager {
  private RegionCache cache;
  private final ReadOnlyPDClient pdClient;

  // To avoid double retrieval, we used the async version of grpc
  // When rpc not returned, instead of call again, it wait for previous one done
  public RegionManager(ReadOnlyPDClient pdClient) {
    this.cache = new RegionCache(pdClient);
    this.pdClient = pdClient;
  }

  public static class RegionCache {
    private static final int MAX_CACHE_CAPACITY =     4096;
    private final Cache<Long, TiRegion>               regionCache;
    private final Cache<Long, Store>                  storeCache;
    private final RangeMap<Comparable, Long>          keyToRegionIdCache;
    private final ReadOnlyPDClient pdClient;

    public RegionCache(ReadOnlyPDClient pdClient) {
      regionCache =
          CacheBuilder.newBuilder()
              .maximumSize(MAX_CACHE_CAPACITY)
              .build();

      storeCache =
          CacheBuilder.newBuilder()
              .maximumSize(MAX_CACHE_CAPACITY)
              .build();

      keyToRegionIdCache = TreeRangeMap.create();
      this.pdClient = pdClient;
    }

    public synchronized TiRegion getRegionByKey(ByteString key) {
      Long regionId;
      regionId = keyToRegionIdCache.get(Comparables.wrap(key));

      if (regionId == null) {
        TiRegion region = pdClient.getRegionByKey(key);
        if (!putRegion(region)) {
          throw new TiClientInternalException("Invalid Region: " + region.toString());
        }
        return region;
      }
      return regionCache.getIfPresent(regionId);
    }

    @SuppressWarnings("unchecked")
    private synchronized boolean putRegion(TiRegion region) {
      if (!region.hasStartKey() || !region.hasEndKey()) return false;

      regionCache.put(region.getId(), region);
      keyToRegionIdCache.put(makeRange(region.getStartKey(), region.getEndKey()), region.getId());
      return true;
    }

    private synchronized TiRegion getRegionById(long regionId) {
      TiRegion region = regionCache.getIfPresent(regionId);
      if (region == null) {
        region = pdClient.getRegionByID(regionId);
        if (!putRegion(region)) {
          throw new TiClientInternalException("Invalid Region: " + region.toString());
        }
      }
      return region;
    }

    @SuppressWarnings("unchecked")
    /**
     * Removes region associated with regionId from regionCache.
     */
    public synchronized void invalidateRegion(long regionId) {
      try {
        TiRegion region = regionCache.getIfPresent(regionId);
        keyToRegionIdCache.remove(makeRange(region.getStartKey(), region.getEndKey()));
      } catch (Exception ignore) {
      } finally {
        regionCache.invalidate(regionId);
      }
    }

    public synchronized void invalidateAllRegionForStore(long storeId) {
      for (TiRegion r : regionCache.asMap().values()) {
        if(r.getLeader().getStoreId() == storeId) {
          regionCache.invalidate(r.getId());
          keyToRegionIdCache.remove(makeRange(r.getStartKey(), r.getEndKey()));
        }
      }
    }

    public void invalidateStore(long storeId) {
      storeCache.invalidate(storeId);
    }


    public synchronized Store getStoreById(long id) {
      try {
        Store store = storeCache.getIfPresent(id);
        if (store == null) {
          store = pdClient.getStore(id);
        }
        if (store.getState().equals(StoreState.Tombstone)) {
          return null;
        }
        storeCache.put(id, store);
        return store;
      } catch (Exception e) {
        throw new GrpcException(e);
      }
    }
  }

  public TiSession getSession() {
    return pdClient.getSession();
  }

  public TiRegion getRegionByKey(ByteString key) {
    return cache.getRegionByKey(key);
  }

  public TiRegion getRegionById(long regionId) {
    return cache.getRegionById(regionId);
  }

  public Pair<TiRegion, Store> getRegionStorePairByKey(ByteString key) {
    TiRegion region = cache.getRegionByKey(key);
    if (region == null) {
      throw new TiClientInternalException("Region not exist for key:" + key);
    }
    if (!region.isValid()) {
      throw new TiClientInternalException("Region invalid: " + region.toString());
    }
    Peer leader = region.getLeader();
    long storeId = leader.getStoreId();
    return Pair.create(region, cache.getStoreById(storeId));
  }

  public Pair<TiRegion, Store> getRegionStorePairByRegionId(long id) {
    TiRegion region = cache.getRegionById(id);
    if (!region.isValid()) {
      throw new TiClientInternalException("Region invalid: " + region.toString());
    }
    Peer leader = region.getLeader();
    long storeId = leader.getStoreId();
    return Pair.create(region, cache.getStoreById(storeId));
  }

  public Store getStoreById(long id) {
    return cache.getStoreById(id);
  }

  public void onRegionStale(long regionID, Peer peer, List<Region> regions) {
    cache.invalidateRegion(regionID);
    for (Region r : regions) {
      cache.putRegion(new TiRegion(r, peer, IsolationLevel.RC, CommandPri.Low));
    }
  }

  public void updateLeader(long regionID, long storeID) {
    TiRegion r = cache.getRegionById(regionID);
    if (r != null) {
      if (!r.switchPeer(storeID)) {
        // drop region cache using verID
        cache.invalidateRegion(regionID);
      }
    }
  }

  /**
   * Clears all cache when a TiKV server does not respond
   * @param regionID region's id
   * @param storeID TiKV store's id
   */
  public void onRequestFail(long regionID, long storeID) {
    TiRegion r = cache.getRegionById(regionID);
    if (r != null) {
      if (!r.onRequestFail(storeID)) {
        cache.invalidateRegion(regionID);
      }
    }

    cache.invalidateAllRegionForStore(storeID);
  }

  public void invalidateStore(long storeId) {
    cache.invalidateStore(storeId);
  }

  public void invalidateRegion(long regionID) {
    cache.invalidateRegion(regionID);
  }
}
