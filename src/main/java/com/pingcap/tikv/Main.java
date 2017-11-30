package com.pingcap.tikv;


import com.google.common.collect.ImmutableList;
import com.pingcap.tikv.catalog.Catalog;
import com.pingcap.tikv.expression.TiColumnRef;
import com.pingcap.tikv.expression.aggregate.Count;
import com.pingcap.tikv.meta.TiDBInfo;
import com.pingcap.tikv.meta.TiSelectRequest;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.predicates.ScanBuilder;
import com.pingcap.tikv.row.Row;
import com.pingcap.tikv.util.RangeSplitter;
import com.pingcap.tikv.util.RangeSplitter.RegionTask;
import java.util.Iterator;
import java.util.List;
import org.apache.log4j.Logger;

public class Main {
  private static final Logger logger = Logger.getLogger(Main.class);
  public static void main(String[] args) throws Exception {
    TiConfiguration conf = TiConfiguration.createDefault(args[0]);
    //TiConfiguration conf = TiConfiguration.createDefault("127.0.0.1:2379");
    TiSession session = TiSession.create(conf);
    Catalog cat = session.getCatalog();
    int loop = 1000;
    try {
      loop = Integer.parseInt(args[3]);
    } catch (Exception e) {}
    for (int i = 0; i < loop; i++) {
      tableScan(cat, session, args[1], args[2]);
      if (i != loop - 1) {
        Thread.currentThread().sleep(60000);
      }
    }
    //tableScan(cat, session, "test", "test");
    //System.out.println("2nd round");
    //tableScan(cat, session, args[1], args[2]);
    session.close();
  }

  private static void tableScan(Catalog cat, TiSession session, String dbName, String tableName) throws Exception {
    TiDBInfo db = cat.getDatabase(dbName);
    TiTableInfo table = cat.getTable(db, tableName);
    Snapshot snapshot = session.createSnapshot();

    logger.info(String.format("Table Scan Start: db[%s] table[%s]", dbName, tableName));

    ScanBuilder scanBuilder = new ScanBuilder();
    ScanBuilder.ScanPlan scanPlan = scanBuilder.buildTableScan(ImmutableList.of(), table);

    TiSelectRequest selReq = new TiSelectRequest();
    TiColumnRef firstColumn = TiColumnRef.create(table.getColumns().get(0).getName(), table);
    selReq.setTableInfo(table)
          .addAggregate(new Count(firstColumn))
          .addRequiredColumn(firstColumn)
          .setStartTs(snapshot.getVersion());

    List<RegionTask> regionTasks = RangeSplitter
        .newSplitter(session.getRegionManager())
        .splitRangeByRegion(scanPlan.getKeyRanges());
    for (RegionTask task : regionTasks) {
      Iterator<Row> it = snapshot.tableRead(selReq, ImmutableList.of(task));
      Row row = it.next();
      logger.info(String.format("Region:[%s] -> count[%s]", task.getRegion(), row.getLong(1)));
    }
  }
}
