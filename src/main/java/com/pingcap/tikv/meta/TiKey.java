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

package com.pingcap.tikv.meta;

import static java.util.Objects.requireNonNull;

import com.google.common.collect.Range;
import com.google.common.primitives.UnsignedBytes;
import com.google.protobuf.ByteString;
import com.pingcap.tikv.kvproto.Coprocessor;
import java.util.Comparator;
import javax.annotation.Nonnull;

public class TiKey<T> implements Comparable<TiKey<T>> {

  // below might uses UnsafeComparator if possible
  private static final Comparator<byte[]> comparator = UnsignedBytes.lexicographicalComparator();
  private T data;

  public TiKey(@Nonnull T data) {
    this.data = data;
  }

  private int compareTo(@Nonnull ByteString o) {
    requireNonNull(o, "other is null");
    ByteString data = (ByteString)this.data;
    int n = Math.min(data.size(), o.size());
    for (int i = 0, j = 0; i < n; i++, j++) {
      int cmp = UnsignedBytes.compare(data.byteAt(i), o.byteAt(j));
      if (cmp != 0) {
        return cmp;
      }
    }
    // one is the prefix of other then the longer is larger
    return data.size() - o.size();
  }

  private int compareTo(@Nonnull byte[] o) {
    // in context of range compare and bytes compare
    // null order is not defined and causes exception
    requireNonNull(o, "other is null");
    return comparator.compare((byte[]) data, o);
  }

  @Override
  @SuppressWarnings("unchecked")
  public int compareTo(@Nonnull TiKey<T> o) {
    if (data instanceof Comparable) {
      return ((Comparable<T>)data).compareTo(o.data);
    } else if (data instanceof byte[]) {
      return compareTo((byte[]) o.data);
    } else if (data instanceof ByteString) {
      return compareTo((ByteString) o.data);
    }
    return 0;
  }

  @Override
  public String toString() {
    return data.toString();
  }

  public static Range<TiKey> toRange(Coprocessor.KeyRange range) {
    if (range == null || (range.getStart().isEmpty() && range.getEnd().isEmpty())) {
      return Range.all();
    }
    if (range.getStart().isEmpty()) {
      return Range.lessThan(new TiKey<>(range.getEnd()));
    }
    if (range.getEnd().isEmpty()) {
      return Range.atLeast(new TiKey<>(range.getStart()));
    }
    return Range.closedOpen(new TiKey<>(range.getStart()), new TiKey<>(range.getEnd()));
  }

  public static Range<TiKey> makeRange(ByteString startKey, ByteString endKey) {
    if (startKey.isEmpty() && endKey.isEmpty()) {
      return Range.all();
    }
    if (startKey.isEmpty()) {
      return Range.lessThan(new TiKey<>(endKey));
    } else if (endKey.isEmpty()) {
      return Range.atLeast(new TiKey<>(startKey));
    }
    return Range.closedOpen(new TiKey<>(startKey), new TiKey<>(endKey));
  }

  public static String formatByteString(ByteString key) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < key.size(); i++) {
      sb.append(key.byteAt(i) & 0xff);
      if (i < key.size() - 1) {
        sb.append(",");
      }
    }
    return sb.toString();
  }
}
