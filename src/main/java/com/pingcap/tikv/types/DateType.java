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

import com.pingcap.tidb.tipb.ExprType;
import com.pingcap.tikv.codec.Codec;
import com.pingcap.tikv.codec.Codec.DateTimeCodec;
import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.codec.CodecDataOutput;
import com.pingcap.tikv.codec.InvalidCodecFormatException;
import com.pingcap.tikv.meta.TiColumnInfo;
import org.joda.time.LocalDate;
import org.joda.time.LocalDateTime;

public class DateType extends DataType {
  public static final DateType DATE = new DateType(MySQLType.TypeDate);
  public static final MySQLType[] subTypes = new MySQLType[] { MySQLType.TypeDate };

  private DateType(MySQLType tp) {
    super(tp);
  }

  DateType(TiColumnInfo.InternalTypeHolder holder) {
    super(holder);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected LocalDate decodeNotNull(int flag, CodecDataInput cdi) {
    LocalDateTime localDateTime;
    if (flag == Codec.UVARINT_FLAG) {
      localDateTime = DateTimeCodec.readFromUVarInt(cdi);
    } else if (flag == Codec.UINT_FLAG) {
      localDateTime = DateTimeCodec.readFromUInt(cdi);
    } else {
      throw new InvalidCodecFormatException("Invalid Flag type for " + getClass().getSimpleName() + ": " + flag);
    }
    if (localDateTime == null) {
      return null;
    }
    return localDateTime.toLocalDate();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected void encodeKey(CodecDataOutput cdo, Object value) {
    LocalDate date = Converter.convertToDate(value);
    DateTimeCodec.writeDateFully(cdo, date);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected void encodeValue(CodecDataOutput cdo, Object value) {
    LocalDate date = Converter.convertToDate(value);
    DateTimeCodec.writeDateFully(cdo, date);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected void encodeProto(CodecDataOutput cdo, Object value) {
    LocalDate date = Converter.convertToDate(value);
    DateTimeCodec.writeDateProto(cdo, date);
  }

  @Override
  public ExprType getProtoExprType() {
    return ExprType.MysqlTime;
  }

  @Override
  public Object getOriginDefaultValueNonNull(String value) {
    return Converter.convertToDate(value);
  }
}
