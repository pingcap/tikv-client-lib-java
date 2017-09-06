package com.pingcap.tikv.util;

import com.pingcap.tikv.Snapshot;
import com.pingcap.tikv.TiCluster;
import com.pingcap.tikv.catalog.Catalog;
import com.pingcap.tikv.expression.TiColumnRef;
import com.pingcap.tikv.expression.TiExpr;
import com.pingcap.tikv.meta.TiDBInfo;
import com.pingcap.tikv.meta.TiIndexInfo;
import com.pingcap.tikv.meta.TiSelectRequest;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.predicates.PredicateUtils;
import com.pingcap.tikv.predicates.ScanBuilder;
import com.pingcap.tikv.region.RegionManager;

import java.util.List;

/**
 * Created by birdstorm on 2017/9/6.
 *
 */
public class DBReader {
  private Catalog cat;
  private Snapshot snapshot;
  private RegionManager manager;
  private boolean isMockedData;

  public DBReader(TiCluster cluster, Snapshot snapshot) {
    this.cat = cluster.getCatalog();
    this.snapshot = snapshot;
    this.manager = cluster.getRegionManager();
    this.isMockedData = false;
  }

  public DBReader(Catalog cat, Snapshot snapshot, RegionManager manager) {
    this.cat = cat;
    this.snapshot = snapshot;
    this.manager = manager;
    this.isMockedData = true;
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

  public List<RangeSplitter.RegionTask> getKeyWithRegionTasks(String dbName, String tableName,
                                                              List<TiExpr> firstAnd, List<String> fields) {
    TiDBInfo db = cat.getDatabase(dbName);
    TiTableInfo tableInfo = cat.getTable(db, tableName);
    TiIndexInfo index = TiIndexInfo.generateFakePrimaryKeyIndex(tableInfo);

    ScanBuilder scanBuilder = new ScanBuilder();
    ScanBuilder.ScanPlan scanPlan = scanBuilder.buildScan(firstAnd, index, tableInfo);
    TiSelectRequest selReq = new TiSelectRequest();

    selReq.addRanges(scanPlan.getKeyRanges()).setTableInfo(tableInfo);

    for(String s: fields) {
      selReq.addField(TiColumnRef.create(s, tableInfo));
    }
    selReq.setStartTs(snapshot.getVersion());

    selReq.addWhere(PredicateUtils.mergeCNFExpressions(scanPlan.getFilters()));

    return RangeSplitter.newSplitter(manager).splitRangeByRegion(selReq.getRanges());
  }


}
