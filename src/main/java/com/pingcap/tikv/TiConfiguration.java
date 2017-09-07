/*
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
 */

package com.pingcap.tikv;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.net.HostAndPort;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class TiConfiguration {
  private static final int DEF_TIMEOUT = 3;
  private static final TimeUnit DEF_TIMEOUT_UNIT = TimeUnit.MINUTES;
  private static final int DEF_SCAN_BATCH_SIZE = 100;
  private static final boolean DEF_IGNORE_TRUNCATE = true;
  private static final boolean DEF_TRUNCATE_AS_WARNING = false;
  private static final int DEF_META_RELOAD_PERIOD = 1;
  private static final TimeUnit DEF_META_RELOAD_UNIT = TimeUnit.SECONDS;

  private int timeout = DEF_TIMEOUT;
  private TimeUnit timeoutUnit = DEF_TIMEOUT_UNIT;
  private boolean ignoreTruncate = DEF_IGNORE_TRUNCATE;
  private boolean truncateAsWarning = DEF_TRUNCATE_AS_WARNING;
  private TimeUnit metaReloadUnit = DEF_META_RELOAD_UNIT;
  private int metaReloadPeriod = DEF_META_RELOAD_PERIOD;
  private List<HostAndPort> pdAddrs = new ArrayList<>();

  public static TiConfiguration createDefault(List<String> pdAddrs) {
    TiConfiguration conf = new TiConfiguration();
    conf.pdAddrs =
        ImmutableList.copyOf(
            ImmutableSet.copyOf(pdAddrs)
                .asList()
                .stream()
                .map(HostAndPort::fromString)
                .collect(Collectors.toList()));
    return conf;
  }

  public int getTimeout() {
    return timeout;
  }

  public void setTimeout(int timeout) {
    this.timeout = timeout;
  }

  public TimeUnit getTimeoutUnit() {
    return timeoutUnit;
  }

  public TimeUnit getMetaReloadPeriodUnit() {
    return metaReloadUnit;
  }

  public void setMetaReloadPeriodUnit(TimeUnit timeUnit) {
    this.metaReloadUnit = timeUnit;
  }

  public void setMetaReloadPeriod(int metaReloadPeriod) {
    this.metaReloadPeriod = metaReloadPeriod;
  }

  public int getMetaReloadPeriod() {
    return metaReloadPeriod;
  }

  public void setTimeoutUnit(TimeUnit timeoutUnit) {
    this.timeoutUnit = timeoutUnit;
  }

  List<HostAndPort> getPdAddrs() {
    return pdAddrs;
  }

  public int getScanBatchSize() {
    return DEF_SCAN_BATCH_SIZE;
  }

  boolean isIgnoreTruncate() {
    return ignoreTruncate;
  }

  public void setIgnoreTruncate(boolean ignoreTruncate) {
    this.ignoreTruncate = ignoreTruncate;
  }

  boolean isTruncateAsWarning() {
    return truncateAsWarning;
  }

  public void setTruncateAsWarning(boolean truncateAsWarning) {
    this.truncateAsWarning = truncateAsWarning;
  }
}
