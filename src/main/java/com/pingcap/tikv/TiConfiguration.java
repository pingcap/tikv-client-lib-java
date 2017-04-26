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
import com.google.common.collect.Iterables;
import com.google.common.net.HostAndPort;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class TiConfiguration {
    public static final int                     DEF_TIMEOUT = 3;
    public static final TimeUnit                DEF_TIMEOUT_UNIT = TimeUnit.SECONDS;
    public static final int                     DEF_SCAN_BATCH_SIZE = 100;
    public static final boolean                 DEF_IGNORE_TRUNCATE = true;
    public static final boolean                 DEF_TRUNCATE_AS_WARNING = false;

    private int                                 timeout = DEF_TIMEOUT;
    private TimeUnit                            timeoutUnit = DEF_TIMEOUT_UNIT;
    private int                                 scanBatchSize = DEF_SCAN_BATCH_SIZE;
    private boolean                             ignoreTruncate = DEF_IGNORE_TRUNCATE;
    private boolean                             truncateAsWarning = DEF_TRUNCATE_AS_WARNING;
    private List<HostAndPort>                   pdAddrs = new ArrayList<>();

    public static TiConfiguration createDefault(List<String> pdAddrs) {
        TiConfiguration conf = new TiConfiguration();
        conf.pdAddrs = ImmutableList.copyOf(Iterables.transform(ImmutableSet.copyOf(pdAddrs).asList(),
                addStr -> HostAndPort.fromString(addStr)));
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

    public void setTimeoutUnit(TimeUnit timeoutUnit) {
        this.timeoutUnit = timeoutUnit;
    }

    public List<HostAndPort> getPdAddrs() {
        return pdAddrs;
    }

    public int getScanBatchSize() {
        return scanBatchSize;
    }

    public boolean isIgnoreTruncate() {
        return ignoreTruncate;
    }

    public void setIgnoreTruncate(boolean ignoreTruncate) {
        this.ignoreTruncate = ignoreTruncate;
    }

    public boolean isTruncateAsWarning() {
        return truncateAsWarning;
    }

    public void setTruncateAsWarning(boolean truncateAsWarning) {
        this.truncateAsWarning = truncateAsWarning;
    }
}
