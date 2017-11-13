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

import com.google.common.primitives.UnsignedBytes;
import com.google.protobuf.ByteString;
import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.codec.CodecDataOutput;
import com.pingcap.tikv.exception.HistogramException;
import com.pingcap.tikv.types.DataType;
import com.pingcap.tikv.types.DataTypeFactory;

import javax.annotation.Nonnull;
import java.util.Comparator;

import static com.pingcap.tikv.types.Types.*;
import static java.util.Objects.requireNonNull;

public class TiKey<T> implements Comparable<TiKey<T>> {

  // below might uses UnsafeComparator if possible
  private static final Comparator<byte[]> comparator = UnsignedBytes.lexicographicalComparator();
  private final T data;

  private TiKey(@Nonnull T data) {
    this.data = data;
  }

  public static TiKey<byte[]> create(byte[] data) {
    return new TiKey<>(data);
  }

  public static TiKey<ByteString> create(ByteString data) {
    return new TiKey<>(data);
  }

  public static TiKey<Long> create(Number data) {
    return new TiKey<>(data.longValue());
  }

  public static TiKey<byte[]> create(@Nonnull Object data) {
    data = unwrap(data);
    if(data instanceof Number) {
      return new TiKey<>(toByteArray(data));
    } else if(data instanceof ByteString) {
      return new TiKey<>(((ByteString) data).toByteArray());
    } else if(data instanceof byte[]) {
      return new TiKey<>(((byte[]) data));
    } else {
      return new TiKey<>(toByteArray(data));
    }
  }

  public static Object unwrap(Object a) {
    if(a instanceof TiKey) {
      return unwrap(((TiKey) a).data);
    } else {
      return a;
    }
  }

  public static TiKey<byte[]> encode(Object o) {
    CodecDataOutput cdo = new CodecDataOutput();
    o = unwrap(o);
    DataType tp;
    if(o instanceof Number) {
      tp = DataTypeFactory.of(TYPE_LONG);
    } else if(o instanceof String) {
      tp = DataTypeFactory.of(TYPE_STRING);
    } else {
      tp = DataTypeFactory.of(TYPE_BLOB);
    }
    tp.encode(cdo, DataType.EncodeType.KEY, o);
    return create(cdo.toBytes());
  }

  public static int Compare(TiKey<Object> a, TiKey<Object> b) {
    return a.compareTo(b);
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
      return TiKey.create(toByteArray(data)).compareTo(toByteArray(o.data));
    } else if (data instanceof byte[]) {
      return compareTo(toByteArray(o.data));
    } else if (data instanceof ByteString) {
      return TiKey.create(toByteArray(data)).compareTo(toByteArray(o.data));
    } else {
      throw new HistogramException("data type not supported to compare: " +
          data.getClass() + " against " + o.data.getClass());
    }
  }

  private static byte[] toByteArray(Object o) {
    if(o instanceof byte[]) {
      return (byte[]) o;
    } else if(o instanceof Comparable) {
      return encode(o).getBytes();
    } else if(o instanceof ByteString) {
      return ((ByteString) o).toByteArray();
    } else {
      throw new HistogramException("data type not supported to compare: " +
          byte[].class + " against " + o.getClass());
    }
  }

  public ByteString getByteString() {
    return (ByteString) data;
  }

  public byte[] getBytes() {
    return ((byte[]) data);
  }

  private String getStringFromByteString(ByteString string) {
    if (string.isValidUtf8()) {
      return string.toStringUtf8();
    } else {
      DataType tp = DataTypeFactory.of(TYPE_LONG);
      CodecDataInput cdi = new CodecDataInput(string);
      long ans = (long) tp.decode(cdi);
      if (ans == Long.MAX_VALUE) {
        return "+∞";
      } else if(ans == Long.MIN_VALUE) {
        return "-∞";
      } else {
        return String.valueOf(ans);
      }
    }
  }

  @Override
  public String toString() {
    CodecDataOutput cdoMax = new CodecDataOutput();
    DataTypeFactory.of(TYPE_BLOB).encode(cdoMax, DataType.EncodeType.KEY, DataType.encodeIndexMaxValue());
    CodecDataOutput cdoMin = new CodecDataOutput();
    DataTypeFactory.of(TYPE_BLOB).encode(cdoMin, DataType.EncodeType.KEY, DataType.encodeIndexMaxValue());
    Object d = unwrap(data);
    if (d.equals(cdoMax.toBytes())) {
      return "∞";
    } else if(d.equals(cdoMin.toBytes())) {
      return "-∞";
    } else if(d instanceof ByteString) {
      return getStringFromByteString(((ByteString) d));
    } else if (d instanceof byte[]) {
      return getStringFromByteString(ByteString.copyFrom(((byte[]) d)));
    } else {
      return d.toString();
    }
  }
}
