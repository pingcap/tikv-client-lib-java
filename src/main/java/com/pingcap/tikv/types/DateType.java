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
import com.pingcap.tikv.exception.TiClientInternalException;
import com.pingcap.tikv.meta.TiColumnInfo;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class DateType extends DataType {
  public static final DateType DATE = new DateType(MySQLType.TypeDate);
  public static final MySQLType[] subTypes = new MySQLType[] { MySQLType.TypeDate };

  private static final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");

  private DateType(MySQLType tp) {
    super(tp);
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
    if (localDateTime == null) {
      return null;
    }
    //TODO revisit this later.
    return new Date(localDateTime.getYear() - 1900,
        localDateTime.getMonthValue() - 1,
        localDateTime.getDayOfMonth());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected void encodeNotNull(CodecDataOutput cdo, EncodeType encodeType, Object value) {
    Date date;
    try {
      if (value instanceof Date) {
        date = (Date) value;
      } else {
        // format ensure only date part without time
        date = new Date(format.parse(value.toString()).getTime());
      }
    } catch (Exception e) {
      throw new TiClientInternalException("Can not cast Object to LocalDateTime: " + value, e);
    }
    boolean writeFlag = (encodeType != EncodeType.PROTO);
    DateTimeCodec.writeDateFully(cdo, date, writeFlag);
  }

  @Override
  public ExprType getProtoExprType() {
    return ExprType.MysqlTime;
  }

  /**
   * {@inheritDoc}
   * @param value a date represents in string in "yyyy-MM-dd" format
   * @return a {@link Date} Object
   */
  @Override
  public Object getOriginDefaultValueNonNull(String value) {
    DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    LocalDate localDate = LocalDate.parse(value, dateTimeFormatter);
    return new Date(localDate.getYear(), localDate.getMonthValue(), localDate.getDayOfMonth());
  }

  DateType(TiColumnInfo.InternalTypeHolder holder) {
    super(holder);
  }
}
