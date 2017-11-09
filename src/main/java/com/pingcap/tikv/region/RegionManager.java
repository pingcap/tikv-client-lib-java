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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.ByteString;
import com.pingcap.tikv.ReadOnlyPDClient;
import com.pingcap.tikv.TiSession;
import com.pingcap.tikv.exception.GrpcException;
import com.pingcap.tikv.exception.TiClientInternalException;
import com.pingcap.tikv.kvproto.Kvrpcpb.IsolationLevel;
import com.pingcap.tikv.kvproto.Metapb.Peer;
import com.pingcap.tikv.kvproto.Metapb.Region;
import com.pingcap.tikv.kvproto.Metapb.Store;
import com.pingcap.tikv.meta.TiKey;
import com.pingcap.tikv.util.Pair;

import javax.annotation.ParametersAreNonnullByDefault;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.pingcap.tikv.meta.TiKey.makeRange;

public class RegionManager {
  private final ReadOnlyPDClient pdClient;
  private final LoadingCache<Long, Future<TiRegion>> regionCache;
  private final LoadingCache<Long, Future<Store>> storeCache;
  private final RangeMap<TiKey, Long> keyToRegionIdCache;
  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  private static final int MAX_CACHE_CAPACITY = 4096;

  // To avoid double retrieval, we used the async version of grpc
  // When rpc not returned, instead of call again, it wait for previous one done
  public RegionManager(ReadOnlyPDClient pdClient) {
    this.pdClient = pdClient;
    regionCache =
        CacheBuilder.newBuilder()
            .maximumSize(MAX_CACHE_CAPACITY)
            .build(
                new CacheLoader<Long, Future<TiRegion>>() {
                  @ParametersAreNonnullByDefault
                  public Future<TiRegion> load(Long key) {
                    return pdClient.getRegionByIDAsync(key);
                  }
                });

    storeCache =
        CacheBuilder.newBuilder()
            .maximumSize(MAX_CACHE_CAPACITY)
            .build(
                new CacheLoader<Long, Future<Store>>() {
                  @ParametersAreNonnullByDefault
                  public Future<Store> load(Long id) {
                    return pdClient.getStoreAsync(id);
                  }
                });
    keyToRegionIdCache = TreeRangeMap.create();
  }

  public TiSession getSession() {
    return pdClient.getSession();
  }

  public TiRegion getRegionByKey(ByteString key) {
    Long regionId;
    lock.readLock().lock();
    try {
      regionId = keyToRegionIdCache.get(TiKey.create(key));
    } finally {
      lock.readLock().unlock();
    }

    if (regionId == null) {
      TiRegion region = pdClient.getRegionByKey(key);
      if (!putRegion(region)) {
        throw new TiClientInternalException("Invalid Region: " + region.toString());
      }
      return region;
    }
    return getRegionById(regionId);
  }

  public void invalidateRegion(long regionId) {
    lock.writeLock().lock();
    try {
      TiRegion region = regionCache.getUnchecked(regionId).get();
      keyToRegionIdCache.remove(makeRange(region.getStartKey(), region.getEndKey()));
    } catch (Exception ignore) {
    } finally {
      lock.writeLock().unlock();
      regionCache.invalidate(regionId);
    }
  }

  public void invalidateStore(long storeId) {
    storeCache.invalidate(storeId);
  }

  public Pair<TiRegion, Store> getRegionStorePairByKey(ByteString key) {
    TiRegion region = getRegionByKey(key);
    if (!region.isValid()) {
      throw new TiClientInternalException("Region invalid: " + region.toString());
    }
    Peer leader = region.getLeader();
    long storeId = leader.getStoreId();
    return Pair.create(region, getStoreById(storeId));
  }

  public TiRegion getRegionById(long id) {
    try {
      return regionCache.getUnchecked(id).get();
    } catch (Exception e) {
      throw new GrpcException(e);
    }
  }

  public Store getStoreById(long id) {
    try {
      return storeCache.getUnchecked(id).get();
    } catch (Exception e) {
      throw new GrpcException(e);
    }
  }


  @SuppressWarnings("unchecked")
  private boolean putRegion(TiRegion region) {
    if (!region.hasStartKey() || !region.hasEndKey()) return false;

    SettableFuture<TiRegion> regionFuture = SettableFuture.create();
    regionFuture.set(region);
    regionCache.put(region.getId(), regionFuture);

    lock.writeLock().lock();
    try {
      keyToRegionIdCache.put(makeRange(region.getStartKey(), region.getEndKey()), region.getId());
    } finally {
      lock.writeLock().unlock();
    }
    return true;
  }

  public void onRegionStale(long regionID, List<Region> regions) {
    invalidateRegion(regionID);
    regions.stream().map(r -> new TiRegion(r, r.getPeers(0), IsolationLevel.RC)).forEach(this::putRegion);
  }

  public void updateLeader(long regionID, long storeID) {
    Optional<Future<TiRegion>> region = Optional.of(regionCache.getUnchecked(regionID));
    region.ifPresent(
        r -> {
          try {
            if (r.get().switchPeer(storeID)) {
              invalidateRegion(regionID);
            }
          } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
          }
        });
  }

  public void onRequestFail(long regionID, long storeID) {
    Optional<Future<TiRegion>> region = Optional.ofNullable(regionCache.getIfPresent(regionID));
    region.ifPresent(
        r -> {
          try {
            if (!r.get().onRequestFail(storeID)) {
              invalidateRegion(regionID);
            }
          } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
          }
          // store's meta may be out of date.
          invalidateStore(storeID);
        });
  }
}
