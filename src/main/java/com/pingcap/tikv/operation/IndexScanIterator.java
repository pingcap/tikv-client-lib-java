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

import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.pingcap.tikv.Snapshot;
import com.pingcap.tikv.codec.KeyUtils;
import com.pingcap.tikv.codec.TableCodec;
import com.pingcap.tikv.kvproto.Coprocessor.KeyRange;
import com.pingcap.tikv.meta.TiSelectRequest;
import com.pingcap.tikv.row.Row;
import java.util.Iterator;

// A very bad implementation of Index Scanner barely made work
// TODO: need to make it parallel and group indexes
public class IndexScanIterator implements Iterator<Row> {
  private final Iterator<Row> iter;
  private final TiSelectRequest selReq;
  private final Snapshot snapshot;

  public IndexScanIterator(Snapshot snapshot, TiSelectRequest req, Iterator<Row> iter) {
    this.iter = iter;
    this.selReq = req;
    this.snapshot = snapshot;
  }

  @Override
  public boolean hasNext() {
    return iter.hasNext();
  }

  @Override
  public Row next() {
    Row r = iter.next();
    long handle = r.getLong(0);
    ByteString startKey = TableCodec.encodeRowKeyWithHandle(selReq.getTableInfo().getId(), handle);
    ByteString endKey = ByteString.copyFrom(KeyUtils.prefixNext(startKey.toByteArray()));
    selReq.resetRanges(
        ImmutableList.of(KeyRange.newBuilder().setStart(startKey).setEnd(endKey).build()));
    Iterator<Row> it = snapshot.select(selReq);
    return it.next();
  }
}
