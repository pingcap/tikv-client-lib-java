package com.pingcap.tikv.util;

import com.google.common.annotations.VisibleForTesting;
import com.pingcap.tikv.Snapshot;
import com.pingcap.tikv.TiConfiguration;
import com.pingcap.tikv.catalog.Catalog;
import com.pingcap.tikv.expression.TiColumnRef;
import com.pingcap.tikv.expression.TiExpr;
import com.pingcap.tikv.meta.TiDBInfo;
import com.pingcap.tikv.meta.TiIndexInfo;
import com.pingcap.tikv.meta.TiSelectRequest;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.operation.SchemaInfer;
import com.pingcap.tikv.predicates.PredicateUtils;
import com.pingcap.tikv.predicates.ScanBuilder;
import com.pingcap.tikv.region.RegionManager;
import com.pingcap.tikv.row.Row;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by birdstorm on 2017/9/6.
 *
 */
public class DBReader {
  private Catalog cat;
  private Snapshot snapshot;
  private RegionManager manager;
  private final boolean isMockedData;
  private TiConfiguration conf;
  private TiDBInfo db;
  private TiSelectRequest selectRequest;

//  public DBReader(TiCluster cluster, Snapshot snapshot) {
//    this.cat = cluster.getCatalog();
//    this.snapshot = snapshot;
//    this.manager = cluster.getRegionManager();
//  }

  public DBReader(Catalog cat, String DBName, Snapshot snapshot, RegionManager manager, TiConfiguration conf) {
    this.cat = cat;
    setCurrentDB(DBName);
    this.snapshot = snapshot;
    this.manager = manager;
    this.conf = conf;
    this.isMockedData = false;
  }

  @VisibleForTesting
  private DBReader() {
    this.isMockedData = true;
  }

  private void setCurrentDB(String DBName) {
    this.db = cat.getDatabase(DBName);
  }

  public Catalog getCatalog() {
    return cat;
  }

  public Snapshot getSnapshot() {
    return snapshot;
  }

  public RegionManager getRegionManager() {
    return manager;
  }

  public TiTableInfo getTableInfo(String tableName) {
    return cat.getTable(db, tableName);
  }

  public TiTableInfo getTableInfo(long tableID) {
    return cat.getTable(db, tableID);
  }

  public TiSelectRequest getSelectRequest(String tableName, List<TiExpr> exprs, List<String> returnFields) {
    TiTableInfo tableInfo = getTableInfo(tableName);
    TiIndexInfo index = TiIndexInfo.generateFakePrimaryKeyIndex(tableInfo);

    ScanBuilder scanBuilder = new ScanBuilder();
    ScanBuilder.ScanPlan scanPlan = scanBuilder.buildScan(exprs, index, tableInfo);
    TiSelectRequest selReq = new TiSelectRequest();

    //build select request
    selReq.addRanges(scanPlan.getKeyRanges()).setTableInfo(tableInfo);
    //add fields
    for(String s: returnFields) {
      selReq.addField(TiColumnRef.create(s, tableInfo));
    }
    selReq.setStartTs(snapshot.getVersion());

    if (conf.isIgnoreTruncate()) {
      selReq.setTruncateMode(TiSelectRequest.TruncateMode.IgnoreTruncation);
    } else if (conf.isTruncateAsWarning()) {
      selReq.setTruncateMode(TiSelectRequest.TruncateMode.TruncationAsWarning);
    }

    selReq.addWhere(PredicateUtils.mergeCNFExpressions(scanPlan.getFilters()));

    return selReq;
  }

  public List<Row> getSelectedRows(TiSelectRequest selReq) {

    List<RangeSplitter.RegionTask> keyWithRegionTasks =
        RangeSplitter.newSplitter(manager).
            splitRangeByRegion(selReq.getRanges());

    List<Row> rowList = new ArrayList<>();

    for (RangeSplitter.RegionTask task : keyWithRegionTasks) {
      Iterator<Row> it = snapshot.select(selReq, task);
      while (it.hasNext()) {
        Row row = it.next();
        rowList.add(row);
      }
    }

    return rowList;
  }

  public void printRows(List<Row> rows, TiSelectRequest selReq) {
    for(Row r: rows) {
      SchemaInfer schemaInfer = SchemaInfer.create(selReq);
      for (int i = 0; i < r.fieldCount(); i++) {
        Object val = r.get(i, schemaInfer.getType(i));
        System.out.print(val);
        System.out.print(" ");
      }
      System.out.print("\n");
    }
  }

}
