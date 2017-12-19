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

import static com.pingcap.tikv.types.Types.TYPE_INT24;
import static com.pingcap.tikv.types.Types.TYPE_LONG;
import static com.pingcap.tikv.types.Types.TYPE_LONG_LONG;
import static com.pingcap.tikv.types.Types.TYPE_SHORT;

import com.pingcap.tikv.codec.Codec;
import com.pingcap.tikv.codec.Codec.IntegerCodec;
import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.codec.CodecDataOutput;
import com.pingcap.tikv.exception.TiClientInternalException;
import com.pingcap.tikv.exception.TiExpressionException;
import com.pingcap.tikv.meta.TiColumnInfo;

/** Base class for all integer types: Tiny, Short, Medium, Int, Long and LongLong */
public class IntegerType extends DataType {
  public static final IntegerType DEF_LONG_TYPE = new IntegerType(Types.TYPE_LONG);
  public static final IntegerType DEF_LONG_LONG_TYPE = new IntegerType(Types.TYPE_LONG_LONG);
  public static final IntegerType DEF_BOOLEAN_TYPE = new IntegerType(Types.TYPE_SHORT);

  static IntegerType of(int tp) {
    return new IntegerType(tp);
  }

  protected IntegerType(int tp) {
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
      case TYPE_SHORT:
      case TYPE_INT24:
      case TYPE_LONG:
        IntegerCodec.writeLongFull(cdo, val, comparable);
        break;
      case TYPE_LONG_LONG:
        IntegerCodec.writeULongFull(cdo, val, comparable);
        break;
    }
  }

  protected IntegerType(TiColumnInfo.InternalTypeHolder holder) {
    super(holder);
  }

}
