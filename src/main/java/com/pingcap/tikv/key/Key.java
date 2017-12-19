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

package com.pingcap.tikv.key;


import static com.pingcap.tikv.codec.KeyUtils.formatBytes;
import static java.util.Objects.requireNonNull;

import com.google.protobuf.ByteString;
import com.pingcap.tikv.codec.CodecDataOutput;
import com.pingcap.tikv.types.DataType;
import com.pingcap.tikv.util.FastByteComparisons;
import java.util.Arrays;

public class Key implements Comparable<Key> {
  protected static final byte[] TBL_PREFIX = new byte[] {'t'};

  protected final byte[] value;
  protected final int infFlag;

  public final static Key NULL = createNull();
  public final static Key MIN = createTypelessMin();
  public final static Key MAX = createTypelessMax();

  private Key(byte[] value, boolean negative) {
    this.value = requireNonNull(value, "value is null");
    this.infFlag = (value.length == 0 ? 1 : 0) * (negative ? -1 : 1);
  }

  protected Key(byte[] value) {
    this(value, false);
  }

  public static Key toKey(ByteString bytes, boolean negative) {
    return new Key(bytes.toByteArray(), negative);
  }

  public static Key toKey(ByteString bytes) {
    return new Key(bytes.toByteArray());
  }

  public static Key toKey(byte[] bytes, boolean negative) {
    return new Key(bytes, negative);
  }

  public static Key toKey(byte[] bytes) {
    return new Key(bytes);
  }

  private static Key createNull() {
    CodecDataOutput cdo = new CodecDataOutput();
    DataType.encodeNull(cdo);
    return new Key(cdo.toBytes()) {
      @Override
      public String toString() {
        return "Null";
      }
    };
  }

  private static Key createTypelessMin() {
    CodecDataOutput cdo = new CodecDataOutput();
    DataType.encodeIndexMinValue(cdo);
    return new Key(cdo.toBytes()) {
      @Override
      public String toString() {
        return "MIN";
      }
    };
  }

  private static Key createTypelessMax() {
    CodecDataOutput cdo = new CodecDataOutput();
    DataType.encodeIndexMaxValue(cdo);
    return new Key(cdo.toBytes()) {
      @Override
      public String toString() {
        return "MAX";
      }
    };
  }

  /**
   * The next key for bytes domain It first plus one at LSB and if LSB overflows, a zero byte is
   * appended at the end Original bytes will be reused if possible
   *
   * @return encoded results
   */
  public Key next() {
    int i;
    byte[] newVal = Arrays.copyOf(value, value.length);
    for (i = newVal.length - 1; i >= 0; i--) {
      newVal[i] ++;
      if (newVal[i] != 0) {
        break;
      }
    }
    if (i == -1) {
      return toKey(Arrays.copyOf(newVal, value.length + 1));
    } else {
      return toKey(newVal);
    }
  }

  @Override
  public int compareTo(Key other) {
    requireNonNull(other, "other is null");
    if ((this.infFlag | other.infFlag) != 0) {
      return this.infFlag - other.infFlag;
    }
    return FastByteComparisons.compareTo(value, other.value);
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }
    if (other instanceof Key) {
      return ((Key) other).compareTo(this) == 0;
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(value) * infFlag;
  }

  public byte[] getBytes() {
    return value;
  }

  public ByteString toByteString() {
    return ByteString.copyFrom(value);
  }

  public int getInfFlag() {
    return infFlag;
  }

  @Override
  public String toString() {
    if (infFlag < 0) {
      return "-INF";
    } else if (infFlag > 0) {
      return "+INF";
    } else {
      return formatBytes(value);
    }
  }
}
