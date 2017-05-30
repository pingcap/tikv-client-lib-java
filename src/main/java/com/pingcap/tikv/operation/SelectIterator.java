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

package com.pingcap.tikv.operation;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.pingcap.tidb.tipb.Chunk;
import com.pingcap.tidb.tipb.SelectRequest;
import com.pingcap.tidb.tipb.SelectResponse;
import com.pingcap.tikv.RegionManager;
import com.pingcap.tikv.RegionStoreClient;
import com.pingcap.tikv.exception.TiClientInternalException;
import com.pingcap.tikv.TiSession;
import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.codec.RowReader;
import com.pingcap.tikv.codec.RowReaderFactory;
import com.pingcap.tikv.grpc.Metapb.Region;
import com.pingcap.tikv.grpc.Metapb.Store;
import com.pingcap.tikv.meta.Row;
import com.pingcap.tikv.meta.TiColumnInfo;
import com.pingcap.tikv.meta.TiRange;
import com.pingcap.tikv.type.FieldType;
import com.pingcap.tikv.util.Pair;
import com.pingcap.tikv.util.RangeSplitter;
import com.pingcap.tikv.util.TiFluentIterable;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Comparator;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;

public class SelectIterator implements Iterator<Row> {
    protected final SelectRequest                               req;
    protected final TiSession                                   session;
    protected final List<Pair<Pair<Region, Store>,
                              TiRange<ByteString>>>             rangeToRegions;
    protected final FieldType[]                                 fieldTypes;


    protected ChunkIterator                         chunkIterator;
    protected int                                   index = 0;
    protected boolean                               eof = false;
    private Function<List<Pair<Pair<Region, Store>,
                              TiRange<ByteString>>>, Boolean>  readNextRegionFn;

    @VisibleForTesting
    public SelectIterator(List<Chunk> chunks, FieldType[] fieldTypes) {
        this.req = null;
        this.session = null;
        this.fieldTypes = fieldTypes;
        this.rangeToRegions = null;
        this.readNextRegionFn = rangeToRegions -> {
            chunkIterator = new ChunkIterator(chunks);
            return true;
        };
    }

    public SelectIterator(SelectRequest req,
                          List<Pair<Pair<Region, Store> ,
                               TiRange<ByteString>>> rangeToRegionsIn,
                          TiSession session) {
        this.req = req;
        this.rangeToRegions = rangeToRegionsIn;
        this.session = session;
        fieldTypes = TypeInferer.toFieldTypes(req);
        this.readNextRegionFn  = (rangeToRegions) -> {
            if (eof || index >= rangeToRegions.size()) {
                return false;
            }

            Pair<Pair<Region, Store>, TiRange<ByteString>> reqPair =
                    rangeToRegions.get(index++);
            Pair<Region, Store> pair = reqPair.first;
            TiRange<ByteString> range = reqPair.second;
            Region region = pair.first;
            Store store = pair.second;
            try (RegionStoreClient client = RegionStoreClient.create(region, store, session)) {
                SelectResponse resp = client.coprocess(req, ImmutableList.of(range));
                if (resp == null) {
                    eof = true;
                    return false;
                }
                chunkIterator = new ChunkIterator(resp.getChunksList());
            } catch (Exception e) {
                eof = true;
                throw new TiClientInternalException("Error Closing Store client.", e);
            }
            return true;
        };
    }

    public SelectIterator(SelectRequest req,
                          List<TiRange<ByteString>> ranges,
                          TiSession session,
                          RegionManager rm) {
        this(req, RangeSplitter.newSplitter(rm).splitRangeByRegion(ranges), session);
    }

    private boolean readNextRegion() {
        return this.readNextRegionFn.apply(rangeToRegions);
    }

    @Override
    public boolean hasNext() {
        if (eof) return false;
        while (chunkIterator == null || !chunkIterator.hasNext()) {
            // Skip empty region until found one or EOF
            if (!readNextRegion()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public Row next() {
        if (hasNext()) {
            ByteString rowData = chunkIterator.next();
            RowReader reader = RowReaderFactory
                                .createRowReader(new CodecDataInput(rowData));
            return reader.readRow(fieldTypes);
        } else {
            throw new NoSuchElementException();
        }
    }

    private static class ChunkIterator implements Iterator<ByteString> {
        private final List<Chunk> chunks;
        private int chunkIndex;
        private int metaIndex;
        private int bufOffset;
        private boolean eof;

        public ChunkIterator(List<Chunk> chunks) {
            // Read and then advance semantics
            this.chunks = chunks;
            chunkIndex = 0;
            metaIndex = 0;
            bufOffset = 0;
            if (chunks.size() == 0 ||
                chunks.get(0).getRowsMetaCount() == 0 ||
                chunks.get(0).getRowsData().size() == 0) {
                eof = true;
            }
        }

        @Override
        public boolean hasNext() {
            return !eof;
        }

        private void advance() {
            if (eof) return;
            Chunk c = chunks.get(chunkIndex);
            bufOffset += c.getRowsMeta(metaIndex++).getLength();
            if (metaIndex >= c.getRowsMetaCount()) {
                // seek for next non-empty chunk
                while (++chunkIndex < chunks.size() &&
                       chunks.get(chunkIndex).getRowsMetaCount() == 0) {
                    ;
                }
                if (chunkIndex >= chunks.size()) {
                    eof = true;
                    return;
                }
                metaIndex = 0;
                bufOffset = 0;
            }
        }

        @Override
        public ByteString next() {
            Chunk c = chunks.get(chunkIndex);
            long endOffset = c.getRowsMeta(metaIndex).getLength() + bufOffset;
            if (endOffset > Integer.MAX_VALUE) {
                throw new TiClientInternalException("Offset exceeded MAX_INT.");
            }
            ByteString rowData = c.getRowsData();
            ByteString result = rowData.substring(bufOffset, (int)endOffset);
            advance();
            return result;
        }
    }
}
