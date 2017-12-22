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
import java.sql.Timestamp;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDateTime;

public class TimestampType extends DataType {
  public static final TimestampType TIMESTAMP = new TimestampType(MySQLType.TypeTimestamp);
  public static final TimestampType TIME = new TimestampType(MySQLType.TypeDuration);

  public static final MySQLType[] subTypes = new MySQLType[] {
      MySQLType.TypeTimestamp, MySQLType.TypeDuration
  };

  TimestampType(MySQLType tp) {
    super(tp);
  }

  TimestampType(TiColumnInfo.InternalTypeHolder holder) {
    super(holder);
  }

  /**
   * {@inheritDoc}
   */
  protected DateTimeZone getTimezone() {
    return DateTimeZone.UTC;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected Object decodeNotNull(int flag, CodecDataInput cdi) {
    LocalDateTime localDateTime;
    if (flag == Codec.UVARINT_FLAG) {
      localDateTime = DateTimeCodec.readFromUVarInt(cdi);
    } else if (flag == Codec.UINT_FLAG) {
      localDateTime = DateTimeCodec.readFromUInt(cdi);
    } else {
      throw new InvalidCodecFormatException("Invalid Flag type for " + getClass().getSimpleName() + ": " + flag);
    }
    return new Timestamp(localDateTime.toDateTime(getTimezone()).getMillis());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected void encodeKey(CodecDataOutput cdo, Object value) {
    DateTimeCodec.writeDateTimeFully(cdo, Converter.convertToDateTime(value));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected void encodeValue(CodecDataOutput cdo, Object value) {
    DateTimeCodec.writeDateTimeFully(cdo, Converter.convertToDateTime(value));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected void encodeProto(CodecDataOutput cdo, Object value) {
    DateTimeCodec.writeDateTimeProto(cdo, Converter.convertToDateTime(value));
  }

  @Override
  public ExprType getProtoExprType() {
    return ExprType.MysqlTime;
  }

  /**
   * {@inheritDoc}
   * @param value a timestamp value in string in format "yyyy-MM-dd HH:mm:ss"
   * @return a {@link LocalDateTime} Object
   * TODO: need decode time with time zone info.
   */
  @Override
  public Object getOriginDefaultValueNonNull(String value) {
    return Converter.convertToDateTime(value);
  }

}
