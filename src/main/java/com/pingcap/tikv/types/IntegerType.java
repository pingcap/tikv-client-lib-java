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
import com.pingcap.tikv.codec.Codec.IntegerCodec;
import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.codec.CodecDataOutput;
import com.pingcap.tikv.exception.TiClientInternalException;
import com.pingcap.tikv.exception.TiExpressionException;
import com.pingcap.tikv.meta.TiColumnInfo;

public class IntegerType extends DataType {
  public static final IntegerType TINYINT = new IntegerType(MySQLType.TypeTiny);
  public static final IntegerType SMALLINT = new IntegerType(MySQLType.TypeShort);
  public static final IntegerType MEDIUMINT = new IntegerType(MySQLType.TypeInt24);
  public static final IntegerType INT = new IntegerType(MySQLType.TypeLong);
  public static final IntegerType BIGINT = new IntegerType(MySQLType.TypeLonglong);
  public static final IntegerType BOOLEAN = TINYINT;

  public static final MySQLType[] subTypes = new MySQLType[] {
      MySQLType.TypeTiny, MySQLType.TypeShort, MySQLType.TypeInt24,
      MySQLType.TypeLong, MySQLType.TypeLonglong
  };

  protected IntegerType(MySQLType tp) {
    super(tp);
  }

  @Override
  protected Object decodeNotNull(int flag, CodecDataInput cdi) {
    return decodeNotNullPrimitive(flag, cdi);
  }

  public static long decodeNotNullPrimitive(int flag, CodecDataInput cdi) {
    switch (flag) {
      case Codec.UVARINT_FLAG:
        return IntegerCodec.readUVarLong(cdi);
      case Codec.UINT_FLAG:
        return IntegerCodec.readULong(cdi);
      case Codec.VARINT_FLAG:
        return IntegerCodec.readVarLong(cdi);
      case Codec.INT_FLAG:
        return IntegerCodec.readLong(cdi);
      default:
        throw new TiClientInternalException("Invalid IntegerType flag: " + flag);
    }
  }

  /**
   * Encode a value to cdo.
   *  @param cdo destination of data.
   * @param encodeType Key or Value.
   * @param value need to be encoded.
   */
  @Override
  protected void encodeNotNull(CodecDataOutput cdo, EncodeType encodeType, Object value) {
    long val;
    if (value instanceof Number) {
      val = ((Number) value).longValue();
    } else {
      throw new TiExpressionException("Cannot cast non-number value to long");
    }
    boolean comparable = false;
    if (encodeType == EncodeType.KEY) {
      comparable = true;
    }
    switch (tp) {
      case TypeShort:
      case TypeInt24:
      case TypeLong:
        IntegerCodec.writeLongFull(cdo, val, comparable);
        break;
      case TypeLonglong:
        IntegerCodec.writeULongFull(cdo, val, comparable);
        break;
    }
  }

  /**
   * get origin default value
   * @param value a int value represents in string
   * @return a int object
   */
  @Override
  public Object getOriginDefaultValueNonNull(String value) {
    return Integer.parseInt(value);
  }

  protected IntegerType(TiColumnInfo.InternalTypeHolder holder) {
    super(holder);
  }

}
