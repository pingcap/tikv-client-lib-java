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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import com.google.protobuf.ByteString;
import com.pingcap.tidb.tipb.SelectRequest;
import com.pingcap.tikv.codec.TableCodec;
import com.pingcap.tikv.exception.TiClientInternalException;
import com.pingcap.tikv.grpc.Kvrpcpb.KvPair;
import com.pingcap.tikv.grpc.Metapb.Region;
import com.pingcap.tikv.grpc.Metapb.Store;
import com.pingcap.tikv.meta.Row;
import com.pingcap.tikv.meta.TiRange;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.operation.ScanIterator;
import com.pingcap.tikv.operation.SelectIterator;
import com.pingcap.tikv.util.Pair;
import com.pingcap.tikv.util.RangeSplitter;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Snapshot {
    private final Version version;
    private final RegionManager regionCache;
    private final TiSession session;
    private static final int EPOCH_SHIFT_BITS = 18;
    private final TiConfiguration conf;

    public Snapshot(Version version, RegionManager regionCache, TiSession session) {
        this.version = version;
        this.regionCache = regionCache;
        this.session = session;
        this.conf = session.getConf();
    }

    public Snapshot(RegionManager regionCache, TiSession session) {
        this(Version.getCurrentTSAsVersion(), regionCache, session);
    }

    public TiSession getSession() {
        return session;
    }

    public long getVersion() {
        return version.getVersion();
    }

    public byte[] get(byte[] key) {
        ByteString keyString = ByteString.copyFrom(key);
        ByteString value = get(keyString);
        return value.toByteArray();
    }

    public ByteString get(ByteString key) {
        Pair<Region, Store> pair = regionCache.getRegionStorePairByKey(key);
        RegionStoreClient client = RegionStoreClient.create(pair.first, pair.second, getSession());
        // TODO: Need to deal with lock error after grpc stable
        return client.get(key, version.getVersion());
    }

    static public List<TiRange<ByteString>> convertHandleRangeToKeyRange(TiTableInfo table,
                                                                   List<TiRange<Long>> ranges) {
        ImmutableList.Builder<TiRange<ByteString>> builder = ImmutableList.builder();
        for (TiRange<Long> r : ranges) {
            ByteString startKey = TableCodec.encodeRowKeyWithHandle(table.getId(), r.getLowValue());
            ByteString endKey = TableCodec.encodeRowKeyWithHandle(table.getId(),
                    Math.max(r.getHighValue() + 1, Long.MAX_VALUE));
            builder.add(TiRange.createByteStringRange(startKey, endKey));
        }
        return builder.build();
    }

    public Iterator<Row> select(TiTableInfo table, SelectRequest req, List<TiRange<Long>> ranges) {
        return new SelectIterator(req, convertHandleRangeToKeyRange(table, ranges), getSession(), regionCache);
    }

    /*
     * Below method is lower level interface for distributed environment
     * which avoids calling PD on slave nodes
     */
    public Iterator<Row> select(SelectRequest req,
                                Region region,
                                Store store,
                                TiRange<ByteString> range) {
        Pair<Region, Store> regionStorePair = Pair.create(region, store);
        Pair<Pair<Region, Store>,
             TiRange<ByteString>> regionToRangePair = Pair.create(regionStorePair, range);

        return new SelectIterator(req, ImmutableList.of(regionToRangePair), getSession());
    }

    public Iterator<KvPair> scan(ByteString startKey) {
        return new ScanIterator(
                startKey, conf.getScanBatchSize(), null, session, regionCache, version.getVersion());
    }

    // TODO: Need faster implementation, say concurrent version
    // Assume keys sorted
    public List<KvPair> batchGet(List<ByteString> keys) {
        Region curRegion = null;
        Range<ByteBuffer> curKeyRange = null;
        Pair<Region, Store> lastPair = null;
        List<ByteString> keyBuffer = new ArrayList<>();
        List<KvPair> result = new ArrayList<>(keys.size());
        for (ByteString key : keys) {
            if (curRegion == null || !curKeyRange.contains(key.asReadOnlyByteBuffer())) {
                Pair<Region, Store> pair = regionCache.getRegionStorePairByKey(key);
                lastPair = pair;
                curRegion = pair.first;
                curKeyRange = Range.closedOpen(
                        curRegion.getStartKey().asReadOnlyByteBuffer(),
                        curRegion.getEndKey().asReadOnlyByteBuffer());
                try (RegionStoreClient client = RegionStoreClient.create(lastPair.first, lastPair.second, getSession())) {
                    List<KvPair> partialResult = client.batchGet(keyBuffer, version.getVersion());
                    for (KvPair kv : partialResult) {
                        // TODO: Add lock check
                        result.add(kv);
                    }
                } catch (Exception e) {
                    throw new TiClientInternalException("Error Closing Store client.", e);
                }
                keyBuffer = new ArrayList<>();
                keyBuffer.add(key);
            }
        }
        return result;
    }

    public SelectBuilder newSelect(TiTableInfo table) {
        return SelectBuilder.newBuilder(this, table);
    }

    public static class Version {
        public static Version getCurrentTSAsVersion() {
            long t = System.currentTimeMillis() << EPOCH_SHIFT_BITS;
            return new Version(t);
        }

        private final long version;

        private Version(long ts) {
            version = ts;
        }

        public long getVersion() {
            return version;
        }
    }
}
