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


import static java.util.Objects.requireNonNull;

import com.pingcap.tikv.exception.TiClientInternalException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;


public class Converter {
  public static long convertToLong(Object val) {
    requireNonNull(val, "val is null");
    if (val instanceof Number) {
      return ((Number)val).longValue();
    } else if (val instanceof String) {
      return Long.parseLong(val.toString());
    }
    throw new TiClientInternalException(String.format("Cannot cast %s to long", val.getClass().getSimpleName()));
  }

  public static double convertToDouble(Object val) {
    requireNonNull(val, "val is null");
    if (val instanceof Number) {
      return ((Number) val).doubleValue();
    } else if (val instanceof String) {
      return Double.parseDouble(val.toString());
    }
    throw new TiClientInternalException(String.format("Cannot cast %s to double", val.getClass().getSimpleName()));
  }

  public static String convertToString(Object val) {
    requireNonNull(val, "val is null");
    return val.toString();
  }

  public static byte[] convertToBytes(Object val) {
    requireNonNull(val, "val is null");
    if (val instanceof byte[]) {
      return (byte[])val;
    } else if (val instanceof String) {
      return ((String) val).getBytes();
    }
    throw new TiClientInternalException(String.format("Cannot cast %s to bytes", val.getClass().getSimpleName()));
  }

  private static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
  public static LocalDateTime convertToDateTime(Object val) {
    requireNonNull(val, "val is null");
    if (val instanceof LocalDateTime) {
      return (LocalDateTime) val;
    } else if (val instanceof String) {
      return LocalDateTime.parse((String)val, dateTimeFormatter);
    } else {
      throw new UnsupportedOperationException("Can not cast Object to LocalDateTime ");
    }
  }

  private static final DateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd");
  public static Date convertToDate(Object val) {
    requireNonNull(val, "val is null");
    if (val instanceof Date) {
      return (Date) val;
    } else if (val instanceof String) {
      try {
        return new Date(dateFormatter.parse((String)val).getTime());
      } catch (ParseException e) {
        throw new TiClientInternalException(String.format("Error parsing string to date", (String)val), e);
      }
    } else {
      throw new TiClientInternalException(String.format("Cannot cast %s to Date", val.getClass().getSimpleName()));
    }
  }

  public static BigDecimal convertToBigDecimal(Object val) {
    requireNonNull(val, "val is null");
    if (val instanceof BigDecimal) {
      return (BigDecimal) val;
    } else if (val instanceof Double || val instanceof Float) {
      return new BigDecimal((Double)val);
    } else if (val instanceof BigInteger) {
      return new BigDecimal((BigInteger)val);
    } else if (val instanceof Number) {
      return new BigDecimal(((Number)val).longValue());
    } else if (val instanceof String) {
      return new BigDecimal((String)val);
    } else {
      throw new UnsupportedOperationException("can not cast non Number type to Double");
    }
  }
}
