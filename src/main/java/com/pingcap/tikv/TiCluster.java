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
import com.pingcap.tikv.catalog.Catalog;
import com.pingcap.tikv.meta.Row;
import com.pingcap.tikv.meta.TiDBInfo;
import com.pingcap.tikv.meta.TiRange;
import com.pingcap.tikv.meta.TiTableInfo;

import java.util.Iterator;
import java.util.List;

// Should be different per session thread
public class TiCluster implements AutoCloseable {
    private final TiSession     session;
    private final RegionManager regionManager;
    private final PDClient      client;

    private TiCluster(TiConfiguration conf) {
        this.session = TiSession.create(conf);
        this.client = PDClient.createRaw(session);
        this.regionManager = new RegionManager(client);
    }

    public static TiCluster getCluster(TiConfiguration conf) {
        return new TiCluster(conf);
    }

    public Snapshot createSnapshot() {
        return new Snapshot(regionManager, session);
    }

    public Catalog getCatalog() {
        return new Catalog(createSnapshot());
    }

    public TiSession getSession() {
        return session;
    }

    public RegionManager getRegionManager() {
        return regionManager;
    }

    @Override
    public void close() throws InterruptedException {
        if (client != null) {
            client.close();
        }
    }
}
