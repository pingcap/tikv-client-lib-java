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

import com.pingcap.tikv.meta.TiColumnInfo;
import org.joda.time.DateTimeZone;

public class DateTimeType extends TimestampType {
  public static final DateTimeType DATETIME = new DateTimeType(MySQLType.TypeDatetime);
  public static final MySQLType[] subTypes = new MySQLType[] { MySQLType.TypeDatetime };

  private static final DateTimeZone DEFAULT_TIMEZONE = DateTimeZone.getDefault();

  /**
   * {@inheritDoc}
   */
  @Override
  protected DateTimeZone getTimezone() {
    return DEFAULT_TIMEZONE;
  }

  private DateTimeType(MySQLType tp) {
    super(tp);
  }

  DateTimeType(TiColumnInfo.InternalTypeHolder holder) {
    super(holder);
  }

}
