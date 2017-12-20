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
import com.pingcap.tikv.codec.Codec.DecimalCodec;
import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.codec.CodecDataOutput;
import com.pingcap.tikv.codec.InvalidCodecFormatException;
import com.pingcap.tikv.meta.TiColumnInfo;

public class DecimalType extends DataType {
  public static final DecimalType DECIMAL = new DecimalType(MySQLType.TypeNewDecimal);
  public static final MySQLType[] subTypes = new MySQLType[] { MySQLType.TypeNewDecimal };

  private DecimalType(MySQLType tp) {
    super(tp);
  }

  DecimalType(TiColumnInfo.InternalTypeHolder holder) {
    super(holder);
  }

  /**
   * decode a decimal value from Cdi and return it.
   *
   * @param cdi source of data.
   */
  @Override
  protected Object decodeNotNull(int flag, CodecDataInput cdi) {
    if (flag != Codec.DECIMAL_FLAG) {
      throw new InvalidCodecFormatException("Invalid Flag type for decimal type: " + flag);
    }
    return DecimalCodec.readDecimalFully(cdi);
  }

  /**
   * Encode a Decimal to Byte String.
   *  @param cdo destination of data.
   * @param encodeType Key or Value.
   * @param value need to be encoded.
   */
  @Override
  protected void encodeNotNull(CodecDataOutput cdo, EncodeType encodeType, Object value) {
    double val;
    if (value instanceof Number) {
      val = ((Number) value).doubleValue();
    } else {
      throw new UnsupportedOperationException("can not cast non Number type to Double");
    }
    DecimalCodec.writeDouble(cdo, val);
  }

  /**
   * get origin value from string.
   * @param value a decimal value represents in string.
   * @return a Double Value
   */
  @Override
  public Object getOriginDefaultValueNonNull(String value) {
    return Double.parseDouble(value);
  }
}
