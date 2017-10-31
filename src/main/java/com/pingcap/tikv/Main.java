package com.pingcap.tikv;


import com.google.protobuf.ByteString;
import com.pingcap.tikv.catalog.Catalog;
import com.pingcap.tikv.codec.TableCodec;
import com.pingcap.tikv.expression.TiColumnRef;
import com.pingcap.tikv.kvproto.Coprocessor;
import com.pingcap.tikv.meta.TiDAGRequest;
import com.pingcap.tikv.meta.TiDBInfo;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.region.RegionManager;
import com.pingcap.tikv.row.Row;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Main {

  public static TiConfiguration conf =
      TiConfiguration.createDefault("127.0.0.1:" + 2379);
  public static TiSession session = TiSession.create(conf);
  public static Snapshot snapshot = session.createSnapshot();
  public static RegionManager regionManager = session.getRegionManager();
  public static PDClient pdClient = session.getPDClient();
  public static Catalog cat = session.getCatalog();

  public static void main(String[] args) throws Exception {
    // May need to save this reference
    TiDBInfo db = cat.getDatabase("tpch_test");
    TiTableInfo table = cat.getTable(db, "customer");
    Logger log = Logger.getLogger("io.grpc");
    log.setLevel(Level.ALL);

    List<TiDBInfo> databaseInfoList = cat.listDatabases();
    ByteString startKey = TableCodec.encodeRowKeyWithHandle(table.getId(), Long.MIN_VALUE);
    ByteString endKey = TableCodec.encodeRowKeyWithHandle(table.getId(), Long.MAX_VALUE);
    Coprocessor.KeyRange keyRange = Coprocessor.KeyRange.newBuilder().setStart(startKey).setEnd(endKey).build();
    List<Coprocessor.KeyRange> ranges = new ArrayList<>();
    ranges.add(keyRange);

    TiDAGRequest dagRequest = new TiDAGRequest();
    dagRequest.addRanges(ranges);
//    dagRequest.addField(TiColumnRef.create("c_address", table));
//    dagRequest.addField(TiColumnRef.create("c_name", table));
//    dagRequest.addField(TiColumnRef.create("c_custkey", table));
    dagRequest.addField(TiColumnRef.create("c_mktsegment", table));
    dagRequest.setTableInfo(table);
    dagRequest.setStartTs(session.getTimestamp().getVersion());
//    dagRequest.addWhere(new GreaterEqual(TiConstant.create(5), TiConstant.create(5)));
//    dagRequest.addAggregate(new Count(TiColumnRef.create("c_custkey")));
//    dagRequest.addGroupByItem(TiByItem.create(TiColumnRef.create("c_mktsegment"), false));
//    dagRequest.addGroupByItem(TiByItem.create(TiColumnRef.create("c_name"), false));
//    dagRequest.setLimit(10);
    dagRequest.bind();
    Iterator<com.pingcap.tikv.row.Row> iterator = snapshot.select(dagRequest);
    System.out.println("Show result:");
    int cnt = 0;
    while (iterator.hasNext()) {
      Row rowData = iterator.next();
      for (int i = 0; i < rowData.fieldCount(); i++) {
        System.out.print(rowData.get(i, null) + "\t");
      }
      System.out.println("     " + cnt++);
//      System.out.println(rowData.get(0, null) + " " + rowData.get(1, null));
    }


//    DAGRequest.Builder dagBuilder = DAGRequest.newBuilder();
//    dagBuilder.setStartTs(System.currentTimeMillis());
//    dagBuilder.addOutputOffsets(0);
//    dagBuilder.addOutputOffsets(1);
//    dagBuilder.setTimeZoneOffset(0);
//    dagBuilder.setFlags(0);
//    Executor.Builder executorBuilder = Executor.newBuilder();
//    executorBuilder.setTp(ExecType.TypeTableScan);
//    TableScan.Builder tableScanBuilder = TableScan.newBuilder();
//    tableScanBuilder.addColumns(table.getColumns().get(0).toProto(table));
//    tableScanBuilder.addColumns(table.getColumns().get(1).toProto(table));
//    executorBuilder.setTblScan(tableScanBuilder);
//    dagBuilder.addExecutors(executorBuilder);

  }
}
