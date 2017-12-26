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
import java.sql.Date;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

public class DateType extends DataType {
  public static final DateType DATE = new DateType(MySQLType.TypeDate);
  public static final MySQLType[] subTypes = new MySQLType[] { MySQLType.TypeDate };

  private DateType(MySQLType tp) {
    super(tp);
  }

  DateType(TiColumnInfo.InternalTypeHolder holder) {
    super(holder);
  }

  private final static DateTimeZone defaultTimeZone = DateTimeZone.getDefault();

  /**
   * {@inheritDoc}
   */
  @Override
  protected Date decodeNotNull(int flag, CodecDataInput cdi) {
    DateTime dateTime;
    if (flag == Codec.UVARINT_FLAG) {
      dateTime = DateTimeCodec.readFromUVarInt(cdi, defaultTimeZone);
    } else if (flag == Codec.UINT_FLAG) {
      dateTime = DateTimeCodec.readFromUInt(cdi, defaultTimeZone);
    } else {
      throw new InvalidCodecFormatException("Invalid Flag type for " + getClass().getSimpleName() + ": " + flag);
    }
    if (dateTime == null) {
      return null;
    }
    return new Date(dateTime.getMillis());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected void encodeKey(CodecDataOutput cdo, Object value) {
    DateTime dt = Converter.convertToDateTime(value);
    DateTimeCodec.writeDateTimeFully(cdo, dt, defaultTimeZone);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected void encodeValue(CodecDataOutput cdo, Object value) {
    encodeKey(cdo, value);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected void encodeProto(CodecDataOutput cdo, Object value) {
    DateTime dt = Converter.convertToDateTime(value);
    DateTimeCodec.writeDateTimeProto(cdo, dt, defaultTimeZone);
  }
  @Override
  public ExprType getProtoExprType() {
    return ExprType.MysqlTime;
  }

  @Override
  public Object getOriginDefaultValueNonNull(String value) {
    return Converter.convertToDateTime(value);
  }
}
