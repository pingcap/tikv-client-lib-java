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
import com.google.protobuf.ByteString;
import com.pingcap.tikv.Snapshot;
import com.pingcap.tikv.codec.KeyUtils;
import com.pingcap.tikv.exception.TiClientInternalException;
import com.pingcap.tikv.meta.TiDBInfo;
import com.pingcap.tikv.meta.TiTableInfo;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Catalog {
  protected static final Logger logger = LogManager.getFormatterLogger(Catalog.class);
  private static ByteString KEY_DB = ByteString.copyFromUtf8("DBs");
  private static ByteString KEY_TABLE = ByteString.copyFromUtf8("Table");

  private static final String DB_PREFIX = "DB";
  private static final String TBL_PREFIX = "Table";

  private CatalogTransaction trx;

  public Catalog(Snapshot snapshot) {
    trx = new CatalogTransaction(snapshot);
  }

  public List<TiDBInfo> listDatabases() {
    return trx.hashGetFields(KEY_DB)
        .stream()
        .map(kv -> parseFromJson(kv.second, TiDBInfo.class))
        .collect(Collectors.toList());
  }

  public TiDBInfo getDatabase(long id) {
    return getDatabase(encodeDatabaseID(id));
  }

  public List<TiTableInfo> listTables(TiDBInfo db) {
    ByteString dbKey = encodeDatabaseID(db.getId());
    if (databaseExists(dbKey)) {
      throw new TiClientInternalException("Database not exists: " + db.getName());
    }

    return trx.hashGetFields(dbKey)
        .stream()
        .filter(kv -> KeyUtils.hasPrefix(kv.first, KEY_TABLE))
        .map(kv -> parseFromJson(kv.second, TiTableInfo.class))
        .collect(Collectors.toList());
  }

  private TiDBInfo getDatabase(ByteString dbKey) {
    try {
      ByteString json = trx.hashGet(KEY_DB, dbKey);
      if (json == null) {
        return null;
      }
      return parseFromJson(json, TiDBInfo.class);
    } catch (Exception e) {
      // TODO: Handle key not exists and let loose others
      return null;
    }
  }

  // TODO: a naive implementation before meta cache implemented
  public TiDBInfo getDatabase(String dbName) {
    for (TiDBInfo db : listDatabases()) {
      if (db.getName().equalsIgnoreCase(dbName)) {
        return db;
      }
    }
    return null;
  }

  public TiTableInfo getTable(TiDBInfo database, long tableId) {
    ByteString dbKey = encodeDatabaseID(database.getId());
    if (!databaseExists(dbKey)) {
      return null;
    }
    ByteString tableKey = encodeTableId(tableId);
    ByteString json = trx.hashGet(dbKey, tableKey);
    return parseFromJson(json, TiTableInfo.class);
  }

  // TODO: a naive implementation before meta cache implemented
  public TiTableInfo getTable(TiDBInfo database, String tableName) {
    for (TiTableInfo tableInfo : listTables(database)) {
      if (tableInfo.getName().equalsIgnoreCase(tableName)) {
        return tableInfo;
      }
    }
    return null;
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
