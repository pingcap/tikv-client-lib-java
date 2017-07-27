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

import static com.pingcap.tikv.types.TimestampType.fromPackedLong;
import static com.pingcap.tikv.types.TimestampType.toPackedLong;

import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.codec.CodecDataOutput;
import com.pingcap.tikv.codec.InvalidCodecFormatException;
import com.pingcap.tikv.meta.TiColumnInfo;
import java.sql.Date;
import java.time.LocalDateTime;
import java.time.LocalTime;

public class DateType extends DataType {
  static DateType of(int tp) {
    return new DateType(tp);
  }

  private DateType(int tp) {
    super(tp);
  }

  @Override
  public Object decodeNotNull(int flag, CodecDataInput cdi) {
    if (flag == UVARINT_FLAG) {
      // read packedUint
      LocalDateTime localDateTime = fromPackedLong(IntegerType.readUVarLong(cdi));
      if (localDateTime == null) {
        return null;
      }
      //TODO revisit this later.
      return new Date(localDateTime.getYear() - 1900,
                            localDateTime.getMonthValue() - 1,
                            localDateTime.getDayOfMonth());
    } else {
      throw new InvalidCodecFormatException("Invalid Flag type for DateType: " + flag);
    }
  }

  @Override
  public void encodeNotNull(CodecDataOutput cdo, EncodeType encodeType, Object value) {
    Date in;
    // TODO, is LocalDateTime enough here?
    if (value instanceof Date) {
      in = (Date) value;
    } else {
      throw new UnsupportedOperationException("Can not cast Object to LocalDateTime ");
    }
    long val = toPackedLong(LocalDateTime.of(in.toLocalDate(), LocalTime.of(0, 0, 0)));
    IntegerType.writeULong(cdo, val);
  }

  DateType(TiColumnInfo.InternalTypeHolder holder) {
    super(holder);
  }
}
