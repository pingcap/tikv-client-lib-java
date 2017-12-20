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

package com.pingcap.tikv.types;

import com.pingcap.tikv.codec.Codec;
import com.pingcap.tikv.codec.Codec.BytesCodec;
import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.codec.CodecDataOutput;
import com.pingcap.tikv.codec.InvalidCodecFormatException;
import com.pingcap.tikv.meta.TiColumnInfo;

/**
 * TODO: if we need to unify string type and binary types? Indeed they are encoded as the same
 * However, decode to string actually going through encoding/decoding by whatever charset.encoding
 * format we set, and essentially changed underlying data
 */
public class RawBytesType extends BytesType {
  public static final RawBytesType BLOB = new RawBytesType(MySQLType.TypeBlob);
  public static final RawBytesType LONG_TEXT = new RawBytesType(MySQLType.TypeLongBlob);
  public static final RawBytesType MEDIUM_TEXT = new RawBytesType(MySQLType.TypeMediumBlob);
  public static final RawBytesType TEXT = new RawBytesType(MySQLType.TypeBlob);
  public static final RawBytesType TINY_BLOB = new RawBytesType(MySQLType.TypeTinyBlob);

  public static final MySQLType[] subTypes = new MySQLType[] {
      MySQLType.TypeBlob, MySQLType.TypeLongBlob,
      MySQLType.TypeMediumBlob, MySQLType.TypeTinyBlob
  };

  private RawBytesType(MySQLType tp) {
    super(tp);
  }

  protected RawBytesType(TiColumnInfo.InternalTypeHolder holder) {
    super(holder);
  }

  @Override
  protected Object decodeNotNull(int flag, CodecDataInput cdi) {
    if (flag == Codec.COMPACT_BYTES_FLAG) {
      return BytesCodec.readCompactBytes(cdi);
    } else if (flag == Codec.BYTES_FLAG) {
      return BytesCodec.readBytes(cdi);
    } else {
      throw new InvalidCodecFormatException("Invalid Flag type for : " + flag);
    }
  }

  /**
   * encode value to cdo per type. If key, then it is memory comparable. If value, no guarantee.
   *  @param cdo destination of data.
   * @param encodeType Key or Value.
   * @param value need to be encoded.
   */
  @Override
  protected void encodeNotNull(CodecDataOutput cdo, EncodeType encodeType, Object value) {
    byte[] bytes;
    if (value instanceof byte[]) {
      bytes = (byte[]) value;
    } else {
      throw new UnsupportedOperationException("can not cast non bytes type to bytes array");
    }
    if (encodeType == EncodeType.KEY) {
      BytesCodec.writeBytes(cdo, bytes);
    } else {
      BytesCodec.writeCompactBytes(cdo, bytes);
    }
  }
}
