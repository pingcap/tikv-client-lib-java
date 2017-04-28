/*
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
 */

package com.pingcap.tikv;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.ByteString;
import com.pingcap.tikv.exception.GrpcException;
import com.pingcap.tikv.exception.TiClientInternalException;
import com.pingcap.tikv.grpc.Metapb.Region;
import com.pingcap.tikv.grpc.Metapb.Store;
import com.pingcap.tikv.grpc.Metapb.Peer;
import com.pingcap.tikv.util.Pair;

import java.nio.ByteBuffer;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReentrantReadWriteLock;


public class RegionManager {
    private final ReadOnlyPDClient pdClient;
    private final LoadingCache<Long, Future<Region>> regionCache;
    private final LoadingCache<Long, Future<Store>> storeCache;
    private final RangeMap<ByteBuffer, Long> keyToRegionIdCache;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public static final int MAX_CACHE_CAPACITY = 4096;

    // To avoid double retrieval, we used the async version of grpc
    // When rpc not returned, instead of call again, it wait for previous one done
    public RegionManager(ReadOnlyPDClient pdClient) {
        this.pdClient = pdClient;
        regionCache = CacheBuilder.newBuilder()
                .maximumSize(MAX_CACHE_CAPACITY)
                .build(new CacheLoader<Long, Future<Region>>() {
                    public Future load(Long key) {
                        return pdClient.getRegionByIDAsync(key);
                    }
                });

        storeCache = CacheBuilder.newBuilder()
                .maximumSize(MAX_CACHE_CAPACITY)
                .build(new CacheLoader<Long, Future<Store>>() {
                    public Future load(Long id) {
                        return pdClient.getStoreAsync(id);
                    }
                });
        keyToRegionIdCache =  TreeRangeMap.create();
    }

    public TiSession getSession() {
        return pdClient.getSession();
    }

    public Region getRegionByKey(ByteString key) {
        Long regionId;
        lock.readLock().lock();
        try {
            regionId = keyToRegionIdCache.get(key.asReadOnlyByteBuffer());
        } finally {
            lock.readLock().unlock();
        }

        if (regionId == null) {
            Region region = pdClient.getRegionByKey(key);
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
            Region region = regionCache.getUnchecked(regionId).get();
            keyToRegionIdCache.remove(makeRange(region.getStartKey(),
                                                region.getEndKey()));
        } catch (Exception ignore) {
        } finally {
            lock.writeLock().unlock();
            regionCache.invalidate(regionId);
        }
    }

    public void invalidateStore(long storeId) {
        storeCache.invalidate(storeId);
    }

    public Pair<Region, Store> getRegionStorePairByKey(ByteString key) {
        Region region = getRegionByKey(key);
        if (!ifValidRegion(region)) {
            throw new TiClientInternalException("Region invalid: " + region.toString());
        }
        Peer leader = region.getPeers(0);
        long storeId = leader.getStoreId();
        return Pair.create(region, getStoreById(storeId));
    }

    public Region getRegionById(long id) {
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

    private boolean ifValidRegion(Region region) {
        if (region.getPeersCount() == 0 ||
                !region.hasStartKey() ||
                !region.hasEndKey()) {
            return false;
        }
        return true;
    }

    private boolean putRegion(Region region) {
        if (!region.hasStartKey() || !region.hasEndKey()) return false;

        SettableFuture<Region> regionFuture = SettableFuture.create();
        regionFuture.set(region);
        regionCache.put(region.getId(), regionFuture);

        lock.writeLock().lock();
        try {
            keyToRegionIdCache.put(makeRange(region.getStartKey(), region.getEndKey()),
                                             region.getId());
        } finally {
            lock.writeLock().lock();
        }
        return true;
    }

    private static Range<ByteBuffer> makeRange(ByteString startKey, ByteString endKey) {
        return Range.closedOpen(startKey.asReadOnlyByteBuffer(),
                                endKey.asReadOnlyByteBuffer());
    }
}
