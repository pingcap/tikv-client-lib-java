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
import com.pingcap.tikv.codec.Codec.DateTimeCodec;
import com.pingcap.tikv.codec.Codec.IntegerCodec;
import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.codec.CodecDataOutput;
import com.pingcap.tikv.codec.InvalidCodecFormatException;
import com.pingcap.tikv.meta.TiColumnInfo;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public class TimestampType extends DataType {
  public static final TimestampType TIMESTAMP = new TimestampType(MySQLType.TypeTimestamp);
  public static final TimestampType TIME = new TimestampType(MySQLType.TypeDuration);

  public static final MySQLType[] subTypes = new MySQLType[] { MySQLType.TypeTimestamp, MySQLType.TypeDuration };

  private static final ZoneId UTC_TIMEZONE = ZoneId.of("UTC");

  protected ZoneId getDefaultTimezone() {
    return UTC_TIMEZONE;
  }

  private String getClassName() {
    return getClass().getSimpleName();
  }

  TimestampType(MySQLType tp) {
    super(tp);
  }

  TimestampType(TiColumnInfo.InternalTypeHolder holder) {
    super(holder);
  }

  @Override
  protected Object decodeNotNull(int flag, CodecDataInput cdi) {
    if (flag == Codec.UVARINT_FLAG) {
      // read packedUInt
      LocalDateTime localDateTime = DateTimeCodec.fromPackedLong(IntegerCodec.readUVarLong(cdi));
      if (localDateTime == null) {
        return null;
      }
      return Timestamp.from(ZonedDateTime.of(localDateTime, getDefaultTimezone()).toInstant());
    } else if (flag == Codec.UINT_FLAG) {
      // read UInt
      LocalDateTime localDateTime = DateTimeCodec.fromPackedLong(IntegerCodec.readULong(cdi));
      if (localDateTime == null) {
        return null;
      }
      return Timestamp.from(ZonedDateTime.of(localDateTime, getDefaultTimezone()).toInstant());
    } else {
      throw new InvalidCodecFormatException("Invalid Flag type for " + getClassName() + ": " + flag);
    }
  }

  /**
   * encode a value to cdo per type.
   *  @param cdo destination of data.
   * @param encodeType Key or Value.
   * @param value need to be encoded.
   */
  @Override
  protected void encodeNotNull(CodecDataOutput cdo, EncodeType encodeType, Object value) {
    LocalDateTime localDateTime;
    if (value instanceof LocalDateTime) {
      localDateTime = (LocalDateTime) value;
    } else {
      throw new UnsupportedOperationException("Can not cast Object to LocalDateTime ");
    }
    long val = DateTimeCodec.toPackedLong(localDateTime);
    IntegerCodec.writeULongFull(cdo, val, true);
  }

  /**
   * get origin value from string
   * @param value a timestamp value in string in format "yyyy-MM-dd HH:mm:ss"
   * @return a {@link LocalDateTime} Object
   * TODO: need decode time with time zone info.
   */
  @Override
  public Object getOriginDefaultValueNonNull(String value) {
    DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    return LocalDateTime.parse(value, dateTimeFormatter);
  }

}
