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

package com.pingcap.tikv.operation;

import com.google.protobuf.ByteString;
import com.pingcap.tidb.tipb.Chunk;
import com.pingcap.tikv.exception.TiClientInternalException;

import java.util.Iterator;
import java.util.List;

public class ChunkIterator implements Iterator<ByteString> {
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
      if (chunks.size() == 0
          || chunks.get(0).getRowsMetaCount() == 0
          || chunks.get(0).getRowsData().size() == 0) {
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
        do {
          chunkIndex += 1;
        } while (chunkIndex < chunks.size() && chunks.get(chunkIndex).getRowsMetaCount() == 0);
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
      ByteString result = rowData.substring(bufOffset, (int) endOffset);
      advance();
      return result;
    }
}
