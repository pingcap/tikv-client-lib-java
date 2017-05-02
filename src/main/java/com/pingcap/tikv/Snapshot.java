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


import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkArgument;
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

import java.nio.ByteBuffer;
import java.util.*;

public class Snapshot {
    private final Version version;
    private final RegionManager regionCache;
    private final TiSession session;
    private final static int EPOCH_SHIFT_BITS = 18;
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
        RegionStoreClient client = RegionStoreClient
                .create(pair.first, pair.second, getSession());
        // TODO: Need to deal with lock error after grpc stable
        return client.get(key, version.getVersion());
    }

    public Iterator<Row> select(TiTableInfo table, SelectRequest req, List<TiRange<Long>> ranges) {
        ImmutableList.Builder<TiRange<ByteString>> builder = ImmutableList.builder();
        for (TiRange<Long> r : ranges) {
            ByteString startKey = TableCodec.encodeRowKeyWithHandle(table.getId(), r.getLowValue());
            ByteString endKey = TableCodec.encodeRowKeyWithHandle(table.getId(),
                                                                  Math.max(r.getHighValue() + 1, Long.MAX_VALUE));
            builder.add(TiRange.createByteStringRange(startKey, endKey));
        }
        List<TiRange<ByteString>> keyRanges = builder.build();
        return new SelectIterator(req, keyRanges, getSession(), regionCache);
    }

    public Iterator<KvPair> scan(ByteString startKey) {
        return new ScanIterator(startKey,
                conf.getScanBatchSize(),
                null,
                session,
                regionCache,
                version.getVersion());
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
                curKeyRange = Range.closedOpen(curRegion.getStartKey().asReadOnlyByteBuffer(),
                                               curRegion.getEndKey().asReadOnlyByteBuffer());
                if (lastPair != null) {
                    try (RegionStoreClient client = RegionStoreClient
                            .create(lastPair.first, lastPair.second, getSession())) {
                        List<KvPair> partialResult = client.batchGet(keyBuffer, version.getVersion());
                        for (KvPair kv : partialResult) {
                            // TODO: Add lock check
                            result.add(kv);
                        }
                    } catch (Exception e) {
                        throw new TiClientInternalException("Error Closing Store client.", e);
                    }
                    keyBuffer = new ArrayList<>();
                }
                keyBuffer.add(key);
            }
        }
        return result;
    }

    public SelectBuilder newSelect(TiTableInfo table) {
        return new SelectBuilder(this, table);
    }

    public static class SelectBuilder {
        private static long MASK_IGNORE_TRUNCATE     = 0x1;
        private static long MASK_TRUNC_AS_WARNING    = 0x2;

        private final Snapshot snapshot;
        private final SelectRequest.Builder builder;
        private final ImmutableList.Builder<TiRange<Long>> rangeListBuilder;
        private TiTableInfo table;

        private TiSession getSession() {
            return snapshot.getSession();
        }

        private TiConfiguration getConf() {
            return getSession().getConf();
        }

        private SelectBuilder(Snapshot snapshot, TiTableInfo table) {
            this.snapshot = snapshot;
            this.builder = SelectRequest.newBuilder();
            this.rangeListBuilder = ImmutableList.builder();
            this.table = table;

            long flags = 0;
            if (getConf().isIgnoreTruncate()) {
                flags |= MASK_IGNORE_TRUNCATE;
            } else if (getConf().isTruncateAsWarning()) {
                flags |= MASK_TRUNC_AS_WARNING;
            }
            builder.setFlags(flags);
            builder.setStartTs(snapshot.getVersion());
            // Set default timezone offset
            TimeZone tz = TimeZone.getDefault();
            builder.setTimeZoneOffset(tz.getOffset(new Date().getTime()) / 1000);
        }

        public SelectBuilder setTimeZoneOffset(long offset) {
            builder.setTimeZoneOffset(offset);
            return this;
        }

        public SelectBuilder addRange(TiRange<Long> keyRange) {
            rangeListBuilder.add(keyRange);
            return this;
        }

        public Iterator<Row> doSelect() {
            checkNotNull(table);
            List<TiRange<Long>> ranges = rangeListBuilder.build();
            checkArgument(ranges.size() > 0);
            return snapshot.select(table, builder.build(), ranges);
        }
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
