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

import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.codec.CodecDataOutput;
import com.pingcap.tikv.codec.InvalidCodecFormatException;
import com.pingcap.tikv.meta.TiColumnInfo;
import com.pingcap.tikv.row.Row;

import java.time.Duration;
import java.time.LocalDateTime;

public class DurationType extends IntegerType {
  static DurationType of(int tp) {
    return new DurationType(tp);
  }

  private DurationType(int tp) {
    super(tp);
  }

  DurationType(TiColumnInfo.InternalTypeHolder holder) {
    super(holder);
  }

  @Override
  public Object decodeNotNull(int flag, CodecDataInput cdi) {
    if (flag == VARINT_FLAG) {
      long nanoSec = IntegerType.readVarLong(cdi);
      Duration duration = Duration.ofNanos(nanoSec);
      return duration.toMillis() / 1000;
    } else if (flag == INT_FLAG) {
      long nanoSec = IntegerType.readLong(cdi);
      Duration duration = Duration.ofNanos(nanoSec);
      return duration.toMillis() / 1000;
    } else {
      throw new InvalidCodecFormatException("Invalid Flag type for Time Type: " + flag);
    }
  }

  /**
   * decode a value from cdi to row per tp.
   *
   * @param cdi source of data.
   * @param row destination of data
   * @param pos position of row.
   */
  public void decode(CodecDataInput cdi, Row row, int pos) {
    int flag = cdi.readUnsignedByte();
    if (flag == VARINT_FLAG) {
      long nanoSec = IntegerType.readVarLong(cdi);
      row.setLong(pos, nanoSec);
    } else if (flag == INT_FLAG) {
      long nanoSec = IntegerType.readLong(cdi);
      row.setLong(pos, nanoSec);
    } else {
      throw new InvalidCodecFormatException("Invalid Flag type for Time Type: " + flag);
    }
  }

  /**
   * encode a value to cdo per type.
   *
   * @param cdo destination of data.
   * @param encodeType Key or Value.
   * @param value need to be encoded.
   */
  @Override
  public void encodeNotNull(CodecDataOutput cdo, EncodeType encodeType, Object value) {
    LocalDateTime localDateTime;
    if (value instanceof LocalDateTime) {
      localDateTime = (LocalDateTime) value;
    } else {
      throw new UnsupportedOperationException("Can not cast Object to LocalDateTime ");
    }
    long val = toPackedLong(localDateTime);
    IntegerType.writeVarLong(cdo, val);
  }

  /**
   * Encode a LocalDateTime to a packed long.
   *
   * @param time localDateTime that need to be encoded.
   * @return a packed long.
   */
  private static long toPackedLong(LocalDateTime time) {
    int year = time.getYear();
    int month = time.getMonthValue() - 1;
    if(year != 0 || month != 0) {
      throw new UnsupportedOperationException("Time Convert Error: Duration of time cannot exceed one month.");
    }
    int day = time.getDayOfMonth();
    int hour = time.getHour();
    int minute = time.getMinute();
    int second = time.getSecond();
    // 1 microsecond = 1000 nano second
    int micro = time.getNano() / 1000;
    long ymd = (year * 13 + month) << 5 | day;
    long hms = hour << 12 | minute << 6 | second;
    return ((ymd << 17 | hms) << 24) | micro;
  }
}
