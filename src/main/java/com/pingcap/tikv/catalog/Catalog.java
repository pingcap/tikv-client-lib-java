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

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.pingcap.tikv.Snapshot;
import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.codec.KeyUtils;
import com.pingcap.tikv.exception.TiClientInternalException;
import com.pingcap.tikv.meta.TiDBInfo;
import com.pingcap.tikv.meta.TiTableInfo;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Catalog {
  protected static final Logger logger = LogManager.getFormatterLogger(Catalog.class);
  private static ByteString KEY_DB = ByteString.copyFromUtf8("DBs");
  private static ByteString KEY_TABLE = ByteString.copyFromUtf8("Table");
  private static ByteString KEY_SCHEMA_VERSION =  ByteString.copyFromUtf8("SchemaVersionKey");

  private static final String DB_PREFIX = "DB";
  private static final String TBL_PREFIX = "Table";

  private Supplier<Snapshot> snapshotProvider;
  private ScheduledExecutorService service;
  private CatalogCache metaCache;



  private static class CatalogCache {
    private CatalogCache(HashMap<String, TiDBInfo> dbCache,
                         HashMap<TiDBInfo, HashMap<String, TiTableInfo>> tableCache,
                         CatalogTransaction trx,
                         long version) {
      this.dbCache = dbCache;
      this.tableCache = tableCache;
      this.trx = trx;
      this.currentVersion = version;
    }

    private HashMap<String, TiDBInfo> dbCache = new HashMap<>();
    private HashMap<TiDBInfo, HashMap<String, TiTableInfo>> tableCache = new HashMap<>();
    private CatalogTransaction trx;
    private long currentVersion;

    public HashMap<String, TiDBInfo> getDbCache() {
      return dbCache;
    }

    public HashMap<TiDBInfo, HashMap<String, TiTableInfo>> getTableCache() {
      return tableCache;
    }

    public CatalogTransaction getTrx() {
      return trx;
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

  private void reloadCache() {
    Snapshot snapshot = snapshotProvider.get();
    CatalogTransaction newTrx = new CatalogTransaction(snapshot);
    long latestVersion = getLatestSchemaVersion(newTrx);
    if (latestVersion > metaCache.getVersion()) {
      metaCache = createNewMetaCache(newTrx);
    }
  }

  private static CatalogCache createNewMetaCache(CatalogTransaction trx) {
    HashMap<String, TiDBInfo> newDBCache = new HashMap<>();
    HashMap<TiDBInfo, HashMap<String, TiTableInfo>> newTableCache = new HashMap<>();

    trx.hashGetFields(KEY_DB)
        .stream()
        .map(kv -> parseFromJson(kv.second, TiDBInfo.class))
        .forEach(db -> newDBCache.put(db.getName(), db));

    Collection<TiDBInfo> databases = newDBCache.values();
    for (TiDBInfo db : databases) {
      ByteString dbKey = encodeDatabaseID(db.getId());

      trx.hashGetFields(dbKey)
          .stream()
          .filter(kv -> KeyUtils.hasPrefix(kv.first, KEY_TABLE))
          .map(kv -> parseFromJson(kv.second, TiTableInfo.class))
          .forEach(table ->
              newTableCache.compute(db, (key, tableMap) -> {
                if (tableMap == null) {
                  HashMap<String, TiTableInfo> newTableMap = new HashMap<>();
                  newTableMap.put(table.getName(), table);
                  return newTableMap;
                } else {
                  tableMap.put(table.getName(), table);
                  return tableMap;
                }
              })
          );
    }

    return new CatalogCache(newDBCache, newTableCache, trx, getLatestSchemaVersion(trx));
  }


  public List<TiDBInfo> listDatabases() {
    return ImmutableList.copyOf(metaCache.getDbCache().values());
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
    return metaCache.getDbCache().get(dbName);
  }

  public TiTableInfo getTable(TiDBInfo database, String tableName) {
    Objects.requireNonNull(database, "database is null");
    Objects.requireNonNull(tableName, "tableName is null");
    Map<String, TiTableInfo> tableMap = metaCache.getTableCache().get(database);
    return tableMap.get(tableName);
  }

  public static long getLatestSchemaVersion(CatalogTransaction trx) {
    Objects.requireNonNull(trx, "Transaction is null");
    ByteString versionBytes = trx.bytesGet(KEY_SCHEMA_VERSION);
    CodecDataInput cdi = new CodecDataInput(versionBytes.toByteArray());
    return Long.parseLong(new String(cdi.toByteArray(), StandardCharsets.UTF_8));
  }

  public TiDBInfo getDatabase(long id) {
    ByteString dbKey = encodeDatabaseID(id);
    try {
      ByteString json = metaCache.getTrx().hashGet(KEY_DB, dbKey);
      if (json == null) {
        return null;
      }
      return parseFromJson(json, TiDBInfo.class);
    } catch (Exception e) {
      // TODO: Handle key not exists and let loose others
      return null;
    }
  }

  private TiDBInfo getDatabase(ByteString dbKey) {
    Objects.requireNonNull(dbKey, "dbKey is null");
    try {
      ByteString json = metaCache.getTrx().hashGet(KEY_DB, dbKey);
      if (json == null) {
        return null;
      }
      return parseFromJson(json, TiDBInfo.class);
    } catch (Exception e) {
      // TODO: Handle key not exists and let loose others
      return null;
    }
  }

  public TiTableInfo getTable(TiDBInfo database, long tableId) {
    Objects.requireNonNull(database, "database is null");
    ByteString dbKey = encodeDatabaseID(database.getId());
    if (!databaseExists(dbKey)) {
      return null;
    }
    ByteString tableKey = encodeTableId(tableId);
    ByteString json = metaCache.getTrx().hashGet(dbKey, tableKey);
    return parseFromJson(json, TiTableInfo.class);
  }

  private static ByteString encodeDatabaseID(long id) {
    return ByteString.copyFrom(String.format("%s:%d", DB_PREFIX, id).getBytes());
  }

  private static ByteString encodeTableId(long id) {
    return ByteString.copyFrom(String.format("%s:%d", TBL_PREFIX, id).getBytes());
  }

  private boolean databaseExists(ByteString dbKey) {
    return getDatabase(dbKey) == null;
  }

  public static <T> T parseFromJson(ByteString json, Class<T> cls) {
    Objects.requireNonNull(json, "json is null");
    Objects.requireNonNull(cls, "cls is null");

    logger.debug("Parse Json %s : %s", cls.getSimpleName(), json.toStringUtf8());
    ObjectMapper mapper = new ObjectMapper();
    try {
      return mapper.readValue(json.toStringUtf8(), cls);
    } catch (JsonParseException | JsonMappingException e) {
      String errMsg =
          String.format(
              "Invalid JSON value for Type %s: %s\n", cls.getSimpleName(), json.toStringUtf8());
      throw new TiClientInternalException(errMsg, e);
    } catch (Exception e1) {
      throw new TiClientInternalException("Error parsing Json", e1);
    }
  }
}
