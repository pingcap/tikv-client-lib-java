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
import com.pingcap.tikv.codec.Codec.RealCodec;
import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.codec.CodecDataOutput;
import com.pingcap.tikv.codec.InvalidCodecFormatException;
import com.pingcap.tikv.meta.TiColumnInfo;

public class RealType extends DataType {
  public static final RealType DOUBLE = new RealType(MySQLType.TypeDouble);
  public static final RealType FLOAT = new RealType(MySQLType.TypeFloat);
  public static final RealType REAL = DOUBLE;

  public static final MySQLType[] subTypes = new MySQLType[] { MySQLType.TypeDouble, MySQLType.TypeFloat };

  private RealType(MySQLType tp) {
    super(tp);
  }

  @Override
  protected Object decodeNotNull(int flag, CodecDataInput cdi) {
    // check flag first and then read.
    if (flag != Codec.FLOATING_FLAG) {
      throw new InvalidCodecFormatException("Invalid Flag type for float type: " + flag);
    }
    return RealCodec.readDouble(cdi);
  }

  RealType(TiColumnInfo.InternalTypeHolder holder) {
    super(holder);
  }

  /**
   * encode a value to cdo.
   *  @param cdo destination of data.
   * @param encodeType Key or Value.
   * @param value need to be encoded.
   */
  @Override
  protected void encodeNotNull(CodecDataOutput cdo, EncodeType encodeType, Object value) {
    double val;
    if (value instanceof Double) {
      val = (Double) value;
    } else {
      throw new UnsupportedOperationException("Can not cast Un-number to Float");
    }

    IntegerCodec.writeULong(cdo, RealCodec.encodeDoubleToCmpLong(val));
  }

  /*
   * get origin default value
   * @param value a float value represents in string
   * @return a {@link Float} Object
   */
  @Override
  public Object getOriginDefaultValueNonNull(String value) {
    return Float.parseFloat(value);
  }
}
