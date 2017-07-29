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

import com.google.common.primitives.UnsignedBytes;
import com.google.protobuf.ByteString;
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
}
