package com.pingcap.tikv.util;

import com.pingcap.tikv.Snapshot;
import com.pingcap.tikv.TiConfiguration;
import com.pingcap.tikv.catalog.Catalog;
import com.pingcap.tikv.expression.TiExpr;
import com.pingcap.tikv.meta.TiColumnInfo;
import com.pingcap.tikv.meta.TiIndexInfo;
import com.pingcap.tikv.region.RegionManager;
import com.pingcap.tikv.row.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by birdstorm on 2017/9/12.
 *
 */
public class MockDBReader extends DBReader {
  private String db;
  private HashMap<String, tables> map;

  private class tables {
    private String tableName;
    private List<TiIndexInfo> indexInfoList;
    private List<TiColumnInfo> columnInfoList;

    private tables(String tableName) { this.tableName = tableName; }
  }

  public MockDBReader(Catalog cat, String DBName, Snapshot snapshot, RegionManager manager, TiConfiguration conf) {
    this.db = DBName;
  }

  public MockDBReader(String DBName) {
    this.db = DBName;
  }

  public void addTable(String dbName, String tableName) {
    tables t = new tables(tableName);
    map.put(dbName, t);
  }

  @Override
  public List<Row> getSelectedRows(String tableName, List<TiExpr> exprs, List<String> returnFields) {
    List<Row> ret = new ArrayList<>();
    tables t = this.map.get(tableName);
    if(t == null) {
      return ret;
    }
    return getSelectedRows(tableName, exprs, returnFields);
  }
}
