package com.pingcap.tikv.statistics;

import com.google.common.collect.ImmutableList;
import com.pingcap.tikv.Snapshot;
import com.pingcap.tikv.catalog.Catalog;
import com.pingcap.tikv.expression.TiColumnRef;
import com.pingcap.tikv.expression.TiConstant;
import com.pingcap.tikv.expression.TiExpr;
import com.pingcap.tikv.expression.scalar.Equal;
import com.pingcap.tikv.expression.scalar.GreaterEqual;
import com.pingcap.tikv.meta.*;
import com.pingcap.tikv.predicates.PredicateUtils;
import com.pingcap.tikv.predicates.ScanBuilder;
import com.pingcap.tikv.region.RegionManager;
import com.pingcap.tikv.row.Row;
import com.pingcap.tikv.types.DataType;
import com.pingcap.tikv.types.DataTypeFactory;
import com.pingcap.tikv.util.DBReader;
import com.pingcap.tikv.util.RangeSplitter;

import java.util.ArrayList;
import java.util.Iterator;
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

  public Table tableStatsFromStorage(Catalog cat, Snapshot snapshot, TiTableInfo tableInfo, RegionManager manager) {
    long id = tableInfo.getId();
    Table table = statsCache.get(id);
    if(table == null) {
      int sz = tableInfo.getIndices() == null ? 0 : tableInfo.getIndices().size();
      table = new Table(id, tableInfo.getColumns().size(), sz);
    } else {
      table = table.copy();
    }

    TiDBInfo db = cat.getDatabase("mysql");
    TiTableInfo tableInfo1 = cat.getTable(db, "stats_histograms");
    TiTableInfo tableInfo2 = cat.getTable(db, "stats_buckets");
    TiIndexInfo index = TiIndexInfo.generateFakePrimaryKeyIndex(tableInfo1);

    List<TiExpr> firstAnd = ImmutableList.of(
        new Equal(TiColumnRef.create("table_id", tableInfo1), TiConstant.create(id)));


    ScanBuilder scanBuilder = new ScanBuilder();
    ScanBuilder.ScanPlan scanPlan = scanBuilder.buildScan(firstAnd, index, tableInfo1);
    TiSelectRequest selReq = new TiSelectRequest();
    selReq
        .addRanges(scanPlan.getKeyRanges())
        .setTableInfo(tableInfo1)
        .addField(TiColumnRef.create("table_id", tableInfo1))
        .addField(TiColumnRef.create("is_index", tableInfo1))
        .addField(TiColumnRef.create("hist_id", tableInfo1))
        .addField(TiColumnRef.create("distinct_count", tableInfo1))
        .addField(TiColumnRef.create("version", tableInfo1))
        .addField(TiColumnRef.create("null_count", tableInfo1))
        .setStartTs(snapshot.getVersion());

    selReq.addWhere(PredicateUtils.mergeCNFExpressions(scanPlan.getFilters()));

    List<RangeSplitter.RegionTask> keyWithRegionTasks =
        RangeSplitter.newSplitter(manager)
            .splitRangeByRegion(selReq.getRanges());

    for (RangeSplitter.RegionTask task : keyWithRegionTasks) {
      Iterator<Row> it = snapshot.select(selReq, task);
      while (it.hasNext()) {
        Row row = it.next();
        long histID = row.getLong(2);
        long distinct = row.getLong(3);
        long histVer = row.getLong(4);
        long nullCount = row.getLong(5);
        long is_index = row.getLong(1);
        if(is_index > 0) {
          IndexWithHistogram idx = table.getIndices().get(histID);
          for(TiIndexInfo idxInfo: tableInfo.getIndices()) {
            if(histID == idxInfo.getId()) {
              if(idx == null || idx.getLastUpdateVersion() < histVer) {
                Histogram hg = new Histogram();
                DataType tp = DataTypeFactory.of(0);
                hg = hg.histogramFromStorage(id, histID, is_index, distinct, histVer,
                    nullCount, snapshot, tp, tableInfo2, manager);
                idx = new IndexWithHistogram(hg, idxInfo);
              }
              break;
            }
          }
          if(idx != null) {
            table.putIndices(histID, idx);
          } else {
            System.out.println("We cannot find index id " + histID +" in table info " + tableInfo.getName() + " now. It may be deleted.");
          }
        } else {
          ColumnWithHistogram col = table.getColumns().get(histID);
          for(TiColumnInfo colInfo: tableInfo.getColumns()) {
            if(histID == colInfo.getId()) {
              if(col == null || col.getLastUpdateVersion() < histVer) {
                Histogram hg = new Histogram();
                hg = hg.histogramFromStorage(id, histID, is_index, distinct, histVer,
                    nullCount, snapshot, colInfo.getType(), tableInfo2, manager);
                col = new ColumnWithHistogram(hg, colInfo);
              }
              break;
            }
          }
          if(col != null) {
            table.putColumns(histID, col);
          } else {
            System.out.println("We cannot find column id " + histID +" in table info " + tableInfo.getName() + " now. It may be deleted.");
          }
        }
      }
    }

    return table;
  }

  public void build(DBReader dbReader) {

    Catalog cat = dbReader.getCatalog();
    Snapshot snapshot = dbReader.getSnapshot();
    RegionManager manager = dbReader.getRegionManager();

    TiDBInfo db = cat.getDatabase("mysql");
    TiTableInfo tableInfo = cat.getTable(db, "stats_meta");
    TiIndexInfo index = TiIndexInfo.generateFakePrimaryKeyIndex(tableInfo);

    List<TiExpr> firstAnd = ImmutableList.of(new GreaterEqual(TiColumnRef.create("version", tableInfo), TiConstant.create(PrevLastVersion)));

    ScanBuilder scanBuilder = new ScanBuilder();
    ScanBuilder.ScanPlan scanPlan = scanBuilder.buildScan(firstAnd, index, tableInfo);
    TiSelectRequest selReq = new TiSelectRequest();
    selReq
        .addRanges(scanPlan.getKeyRanges())
        .setTableInfo(tableInfo)
        .addField(TiColumnRef.create("version", tableInfo))
        .addField(TiColumnRef.create("table_id", tableInfo))
        .addField(TiColumnRef.create("modify_count", tableInfo))
        .addField(TiColumnRef.create("count", tableInfo))
        .setStartTs(snapshot.getVersion());

    selReq.addWhere(PredicateUtils.mergeCNFExpressions(scanPlan.getFilters()));

    List<RangeSplitter.RegionTask> keyWithRegionTasks =
        RangeSplitter.newSplitter(manager)
            .splitRangeByRegion(selReq.getRanges());

    PrevLastVersion = LastVersion;

    ArrayList<Table> tables = new ArrayList<>();
    ArrayList<Long> deletedTableIDs = new ArrayList<>();

    for (RangeSplitter.RegionTask task : keyWithRegionTasks) {
      Iterator<Row> it = snapshot.select(selReq, task);
      while (it.hasNext()) {
        Row row = it.next();
        long version = row.getLong(0);
        long tableID = row.getLong(1);
        long modifyCount = row.getLong(2);
        long count = row.getLong(3);

        LastVersion = version;
        TiTableInfo tableInfo1 = cat.getTable(db, tableID);
        if(tableInfo1 == null) {
          System.out.println("Unknown table ID [" + tableID + "] in stats meta table, maybe it has been dropped");
          deletedTableIDs.add(tableID);
          continue;
        }
        System.out.println("We have table" + tableInfo1.getName());
        Table tbl = tableStatsFromStorage(cat, snapshot, tableInfo1, manager);
        if(tbl == null) {
          deletedTableIDs.add(tableID);
          continue;
        }
        tbl.setVersion(version);
        tbl.setModifyCount(modifyCount);
        tbl.setCount(count);
        tables.add(tbl);

      }
    }
    updateTableStats(tables, deletedTableIDs);
  }

  private Table getTableStats(long tblID) {
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

  public void updateTableStats(ArrayList<Table> tables, ArrayList<Long> deletedTableIDs) {
    ConcurrentHashMap<Long, Table> newCache = copyFromOldCache();
    for(Table t: tables) {
      newCache.replace(t.getTableID(), t);
    }
    for(long id: deletedTableIDs) {
      newCache.remove(id);
    }
    statsCache = newCache;
  }

}
