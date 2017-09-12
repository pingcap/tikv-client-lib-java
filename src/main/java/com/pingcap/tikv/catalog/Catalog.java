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

package com.pingcap.tikv.catalog;

import com.google.common.collect.ImmutableList;
import com.pingcap.tikv.Snapshot;
import com.pingcap.tikv.meta.TiDBInfo;
import com.pingcap.tikv.meta.TiTableInfo;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class Catalog {
  private Supplier<Snapshot> snapshotProvider;
  private ScheduledExecutorService service;
  private CatalogCache metaCache;

  private static class CatalogCache {
    private CatalogCache(HashMap<String, TiDBInfo> dbCache,
                         HashMap<TiDBInfo, HashMap<String, TiTableInfo>> tableCache,
                         CatalogTransaction transaction) {
      this.dbCache = dbCache;
      this.tableCache = tableCache;
      this.transaction = transaction;
      this.currentVersion = transaction.getLatestSchemaVersion();
    }

    private HashMap<String, TiDBInfo> dbCache = new HashMap<>();
    private HashMap<TiDBInfo, HashMap<String, TiTableInfo>> tableCache = new HashMap<>();
    private CatalogTransaction transaction;
    private long currentVersion;

    private HashMap<String, TiDBInfo> getDBCache() {
      return dbCache;
    }

    private HashMap<TiDBInfo, HashMap<String, TiTableInfo>> getTableCache() {
      return tableCache;
    }

    public CatalogTransaction getTransaction() {
      return transaction;
    }

    public long getVersion() {
      return currentVersion;
    }
  }

  public Catalog(Supplier<Snapshot> snapshotProvider, int refreshPeriod, TimeUnit periodUnit) {
    this.snapshotProvider = Objects.requireNonNull(snapshotProvider,
                                                   "Snapshot Provider is null");
    metaCache = createNewMetaCache(new CatalogTransaction(snapshotProvider.get()));
    service = Executors.newSingleThreadScheduledExecutor();
    service.scheduleAtFixedRate(() -> reloadCache(), refreshPeriod, refreshPeriod, periodUnit);
  }

  public Catalog(Supplier<Snapshot> snapshotProvider) {
    this.snapshotProvider = Objects.requireNonNull(snapshotProvider,
        "Snapshot Provider is null");
    metaCache = createNewMetaCache(new CatalogTransaction(snapshotProvider.get()));
  }

  private void reloadCache() {
    Snapshot snapshot = snapshotProvider.get();
    CatalogTransaction newTrx = new CatalogTransaction(snapshot);
    long latestVersion = newTrx.getLatestSchemaVersion();
    if (latestVersion > metaCache.getVersion()) {
      metaCache = createNewMetaCache(newTrx);
    }
  }

  private static CatalogCache createNewMetaCache(CatalogTransaction trx) {
    HashMap<String, TiDBInfo> newDBCache = new HashMap<>();
    HashMap<TiDBInfo, HashMap<String, TiTableInfo>> newTableCache = new HashMap<>();

    List<TiDBInfo> databases = trx.getDatabases();
    databases.forEach(db -> newDBCache.put(db.getName(), db));

    for (TiDBInfo db : databases) {
      List<TiTableInfo> tables = trx.getTables(db.getId());
      for (TiTableInfo table : tables) {
        newTableCache.compute(db, (key, tableMap) -> {
          if (tableMap == null) {
            HashMap<String, TiTableInfo> newTableMap = new HashMap<>();
            newTableMap.put(table.getName(), table);
            return newTableMap;
          } else {
            tableMap.put(table.getName(), table);
            return tableMap;
          }
        });
      }
    }
    return new CatalogCache(newDBCache, newTableCache, trx);
  }

  public List<TiDBInfo> listDatabases() {
    return ImmutableList.copyOf(metaCache.getDBCache().values());
  }

  public List<TiTableInfo> listTables(TiDBInfo database) {
    Objects.requireNonNull(database, "database is null");
    Map<String, TiTableInfo> tables = metaCache.getTableCache().get(database);
    if (tables == null) {
      return null;
    }
    return ImmutableList.copyOf(tables.values());
  }



  public TiDBInfo getDatabase(String dbName) {
    Objects.requireNonNull(dbName, "dbName is null");
    return metaCache.getDBCache().get(dbName);
  }

  public TiTableInfo getTable(String dbName, String tableName) {
    TiDBInfo database = getDatabase(dbName);
    if (database == null) {
      return null;
    }
    return getTable(database, tableName);
  }

  public TiTableInfo getTable(TiDBInfo database, String tableName) {
    Objects.requireNonNull(database, "database is null");
    Objects.requireNonNull(tableName, "tableName is null");
    Map<String, TiTableInfo> tableMap = metaCache.getTableCache().get(database);
    return tableMap.get(tableName);
  }

  public TiTableInfo getTable(TiDBInfo database, long tableId) {
    Objects.requireNonNull(database, "database is null");
    Map<String, TiTableInfo> tableMap = metaCache.getTableCache().get(database);
    Collection<TiTableInfo> tables = tableMap.values();
    for (TiTableInfo table : tables) {
      if (table.getId() == tableId) {
        return table;
      }
    }
    return null;
  }
}
