package com.pingcap.tikv;


import com.google.common.collect.ImmutableList;
import com.pingcap.tikv.catalog.Catalog;
import com.pingcap.tikv.expression.TiColumnRef;
import com.pingcap.tikv.expression.TiConstant;
import com.pingcap.tikv.expression.TiExpr;
import com.pingcap.tikv.expression.scalar.GreaterThan;
import com.pingcap.tikv.meta.TiDBInfo;
import com.pingcap.tikv.meta.TiSelectRequest;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.operation.SchemaInfer;
import com.pingcap.tikv.predicates.PredicateUtils;
import com.pingcap.tikv.predicates.ScanBuilder;
import com.pingcap.tikv.row.Row;
import com.pingcap.tikv.util.Timer;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class Main {
  static TiConfiguration conf = TiConfiguration.createDefault("127.0.0.1:" + 2379);
  static TiSession session = TiSession.create(conf);
  static Catalog cat = session.getCatalog();

  public static void main(String[] args) throws Exception {
    // May need to save this reference
    if (args.length == 3) {
      conf.setIndexScanConcurrency(Integer.parseInt(args[0]));
      conf.setTableScanConcurrency(Integer.parseInt(args[1]));
      conf.setIndexScanBatchSize(Integer.parseInt(args[2]));
    }
    //testUniqueIndex();
    //tableScan();
    indexScan();
    session.close();
    System.exit(0);
  }

  private static void testUniqueIndex() {
    TiDBInfo db = cat.getDatabase("test");
    TiTableInfo table = cat.getTable(db, "test1");
    Snapshot snapshot = session.createSnapshot();

    System.out.println("Index Scan Start");
    List<TiExpr> exprs =
        ImmutableList.of(
            new GreaterThan(TiColumnRef.create("c3", table),
                TiConstant.create("aa"))
        );

    ScanBuilder scanBuilder = new ScanBuilder();
    ScanBuilder.ScanPlan scanPlan = scanBuilder.buildScan(exprs, table);

    TiSelectRequest selReq = new TiSelectRequest();
    selReq
        .addRanges(scanPlan.getKeyRanges())
        .setTableInfo(table)
        .addRequiredColumn(TiColumnRef.create("c1", table))
        .addRequiredColumn(TiColumnRef.create("c2", table))
        .setStartTs(snapshot.getVersion());

    if (scanPlan.isIndexScan()) {
      selReq.setIndexInfo(scanPlan.getIndex());
    }
    System.out.println(scanPlan.getIndex().toString());
    System.out.println(selReq.toString());

    Timer t1 = new Timer();
    Iterator<Row> it = snapshot.tableRead(selReq);

    SchemaInfer schemaInfer = SchemaInfer.create(selReq);
    long acc = 0;
    while (it.hasNext()) {
      Row r = it.next();
      for (int i = 0; i < r.fieldCount(); i++) {
        Object v = r.get(i, schemaInfer.getType(i));
        if (v != null)
          acc += (Long)v;
      }
    }
    System.out.println("acc:" + acc);
    System.out.println("done t1:" + t1.stop(TimeUnit.SECONDS));
    System.out.println("");
  }

  private static void tableScan() throws Exception {
    TiDBInfo db = cat.getDatabase("TPCH");
    TiTableInfo table = cat.getTable(db, "lineitem");
    Snapshot snapshot = session.createSnapshot();

    System.out.println("Table Scan Start");
    List<TiExpr> exprs =
        ImmutableList.of(
            new GreaterThan(TiColumnRef.create("L_SHIPDATE", table),
                TiConstant.create("1993-07-30"))
        );

    ScanBuilder scanBuilder = new ScanBuilder();
    ScanBuilder.ScanPlan scanPlan = scanBuilder.buildTableScan(exprs, table);

    TiSelectRequest selReq = new TiSelectRequest();
    selReq
        .addRanges(scanPlan.getKeyRanges())
        .setTableInfo(table)
        .addRequiredColumn(TiColumnRef.create("L_LINENUMBER", table))
        .addRequiredColumn(TiColumnRef.create("L_SHIPDATE", table))
        .setStartTs(snapshot.getVersion());

    selReq.addWhere(PredicateUtils.mergeCNFExpressions(scanPlan.getFilters()));
    System.out.println(selReq.toString());

    Timer t1 = new Timer();
    Iterator<Row> it = snapshot.tableRead(selReq);

    SchemaInfer schemaInfer = SchemaInfer.create(selReq);
    long acc = 0;
    while (it.hasNext()) {
      Row r = it.next();
      for (int i = 0; i < r.fieldCount(); i++) {
        Object v = r.get(i, schemaInfer.getType(i));
        if (v instanceof Long)
          acc += (Long)v;
      }
    }
    System.out.println("acc:" + acc);
    System.out.println("done time:" + t1.stop(TimeUnit.SECONDS));
    System.out.println("");
  }

  private static void indexScan() throws Exception {
    TiDBInfo db = cat.getDatabase("TPCH");
    TiTableInfo table = cat.getTable(db, "lineitem");
    Snapshot snapshot = session.createSnapshot();

    System.out.println("Index Scan Start");
    List<TiExpr> exprs =
        ImmutableList.of(
            new GreaterThan(TiColumnRef.create("L_SHIPDATE", table),
                TiConstant.create("1993-07-30"))
        );

    ScanBuilder scanBuilder = new ScanBuilder();
    ScanBuilder.ScanPlan scanPlan = scanBuilder.buildScan(exprs, table);

    TiSelectRequest selReq = new TiSelectRequest();
    selReq
        .addRanges(scanPlan.getKeyRanges())
        .setTableInfo(table)
        .addRequiredColumn(TiColumnRef.create("L_LINENUMBER", table))
        .setStartTs(snapshot.getVersion());

    if (scanPlan.isIndexScan()) {
      selReq.setIndexInfo(scanPlan.getIndex());
    }
    System.out.println(scanPlan.getIndex().toString());
    System.out.println(selReq.toString());

    Timer t1 = new Timer();
    Iterator<Row> it = snapshot.tableRead(selReq);

    SchemaInfer schemaInfer = SchemaInfer.create(selReq);
    long acc = 0;
    while (it.hasNext()) {
      Row r = it.next();
      for (int i = 0; i < r.fieldCount(); i++) {
        Object v = r.get(i, schemaInfer.getType(i));
        if (v != null)
          acc += (Long)v;
      }
    }
    System.out.println("acc:" + acc);
    System.out.println("done t1:" + t1.stop(TimeUnit.SECONDS));
    System.out.println("");
  }
}
