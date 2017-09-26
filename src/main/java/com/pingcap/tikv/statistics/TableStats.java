package com.pingcap.tikv.statistics;

import com.google.common.collect.ImmutableList;
import com.pingcap.tikv.expression.TiColumnRef;
import com.pingcap.tikv.expression.TiConstant;
import com.pingcap.tikv.expression.TiExpr;
import com.pingcap.tikv.expression.scalar.Equal;
import com.pingcap.tikv.expression.scalar.GreaterThan;
import com.pingcap.tikv.meta.TiColumnInfo;
import com.pingcap.tikv.meta.TiIndexInfo;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.row.Row;
import com.pingcap.tikv.util.DBReader;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by birdstorm on 2017/8/23.
 *
 */
public class TableStats {
  private ConcurrentHashMap<Long, Table> statsCache;
  private long LastVersion;
  private long PrevLastVersion;

  public TableStats() {
    statsCache = new ConcurrentHashMap<>(256);
    LastVersion = 0;
    PrevLastVersion = 0;
  }

  public void Clear() {
    statsCache.clear();
    LastVersion = 0;
    PrevLastVersion = 0;
  }

  public Table get(long TableID) {
    return statsCache.get(TableID);
  }

  public Table tableStatsFromStorage(DBReader dbReader, TiTableInfo tableInfo) {
    long id = tableInfo.getId();
    Table table = statsCache.get(id);

//    tableInfo.printIndices();

    if(table == null) {
      int sz = tableInfo.getIndices() == null ? 0 : tableInfo.getIndices().size();
      table = new Table(id, tableInfo.getColumns().size(), sz);
    } else {
      table = table.copy();
    }

    TiTableInfo histogramInfo = dbReader.getTableInfo("stats_histograms");

    List<TiExpr> firstAnd = ImmutableList.of(
        new Equal(TiColumnRef.create("table_id", histogramInfo), TiConstant.create(id)));
    List<String> returnFields = ImmutableList.of(
        "table_id", "is_index", "hist_id", "distinct_count", "version", "null_count");

    List<Row> result = dbReader.getSelectedRows("stats_histograms", firstAnd, returnFields);

    for(Row row: result) {
      long histID = row.getLong(2);
      long distinct = row.getLong(3);
      long histVer = row.getLong(4);
      long nullCount = row.getLong(5);
      long is_index = row.getLong(1);

      if (is_index > 0) {
        IndexWithHistogram idx = table.getIndices().get(histID);
        for (TiIndexInfo idxInfo : tableInfo.getIndices()) {
          if (histID == idxInfo.getId()) {
            if (idx == null || idx.getLastUpdateVersion() < histVer) {
              Histogram hg = new Histogram();
              hg = hg.histogramFromStorage(id, histID, is_index, distinct, histVer,
                  nullCount, dbReader, null);
              idx = new IndexWithHistogram(hg, idxInfo);
            }
            break;
          }
        }
        if (idx != null) {
          table.putIndices(histID, idx);
        } else {
          System.out.println("We cannot find index id " + histID + " in table info " + tableInfo.getName() + " now. It may be deleted.");
        }
      } else {
        ColumnWithHistogram col = table.getColumns().get(histID);
        for (TiColumnInfo colInfo : tableInfo.getColumns()) {
          if (histID == colInfo.getId()) {
            if (col == null || col.getLastUpdateVersion() < histVer) {
              Histogram hg = new Histogram();
              hg = hg.histogramFromStorage(id, histID, is_index, distinct, histVer,
                  nullCount, dbReader, colInfo.getType());
              col = new ColumnWithHistogram(hg, colInfo);
            }
            break;
          }
        }
        if (col != null) {
          table.putColumns(histID, col);
        } else {
          System.out.println("We cannot find column id " + histID + " in table info " + tableInfo.getName() + " now. It may be deleted.");
        }
      }
    }

    return table;
  }

  public void build(DBReader dbReader) {

    TiTableInfo metaInfo = dbReader.getTableInfo("stats_meta");

    List<TiExpr> firstAnd = ImmutableList.of(
        new GreaterThan(TiColumnRef.create("version", metaInfo), TiConstant.create(PrevLastVersion)));
    List<String> returnFields = ImmutableList.of(
        "version", "table_id", "modify_count", "count");

    List<Row> rows = dbReader.getSelectedRows("stats_meta", firstAnd, returnFields);

    PrevLastVersion = LastVersion;

    List<Table> tables = new ArrayList<>();
    List<Long> deletedTableIDs = new ArrayList<>();
    for (Row row: rows) {
      long version = row.getLong(0);
      long tableID = row.getLong(1);
      long modifyCount = row.getLong(2);
      long count = row.getLong(3);

      LastVersion = version;
      TiTableInfo tableInfo1 = dbReader.getTableInfo(tableID);
      if(tableInfo1 == null) {
        System.out.println("Unknown table ID#" + tableID + " in stats meta table, maybe it has been dropped");
        deletedTableIDs.add(tableID);
        continue;
      }
      Table tbl = tableStatsFromStorage(dbReader, tableInfo1);
      if(tbl == null) {
        System.out.println("Table stats for table#" + tableID + " is not found.");
        deletedTableIDs.add(tableID);
        continue;
      }
      tbl.setVersion(version);
      tbl.setModifyCount(modifyCount);
      tbl.setCount(count);
      tables.add(tbl);
    }
    updateTableStats(tables, deletedTableIDs);
  }

  public Table getTableStats(long tblID) {
    Table table = statsCache.get(tblID);
    if(table == null) {
      return Table.PseudoTable(tblID);
    }
    return table;
  }

  private ConcurrentHashMap<Long, Table> copyFromOldCache() {
    ConcurrentHashMap<Long, Table> newCache = new ConcurrentHashMap<>();
    for(Entry<Long, Table> e: statsCache.entrySet()) {
      newCache.put(e.getKey(), e.getValue());
    }
    return newCache;
  }

  private void updateTableStats(List<Table> tables, List<Long> deletedTableIDs) {
    ConcurrentHashMap<Long, Table> newCache = copyFromOldCache();
    for(Table t: tables) {
      if(newCache.containsKey(t.getTableID())) {
        newCache.replace(t.getTableID(), t);
      } else {
        newCache.put(t.getTableID(), t);
      }
    }
    for(long id: deletedTableIDs) {
      newCache.remove(id);
    }
    statsCache = newCache;
  }

}
