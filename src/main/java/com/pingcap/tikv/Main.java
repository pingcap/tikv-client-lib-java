package com.pingcap.tikv;


import com.google.protobuf.ByteString;
import com.pingcap.tikv.catalog.Catalog;
import com.pingcap.tikv.codec.TableCodec;
import com.pingcap.tikv.expression.TiByItem;
import com.pingcap.tikv.expression.TiColumnRef;
import com.pingcap.tikv.expression.aggregate.Count;
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
    TiDBInfo db = cat.getDatabase("tpch_test");
    TiTableInfo table = cat.getTable(db, "customer");
    Logger log = Logger.getLogger("io.grpc");
    log.setLevel(Level.ALL);

    ByteString startKey = TableCodec.encodeRowKeyWithHandle(table.getId(), Long.MIN_VALUE);
    ByteString endKey = TableCodec.encodeRowKeyWithHandle(table.getId(), Long.MAX_VALUE);
    Coprocessor.KeyRange keyRange = Coprocessor.KeyRange.newBuilder().setStart(startKey).setEnd(endKey).build();
    List<Coprocessor.KeyRange> ranges = new ArrayList<>();
    ranges.add(keyRange);

    TiDAGRequest dagRequest = new TiDAGRequest();
    dagRequest.addRanges(ranges);
    dagRequest.addField(TiColumnRef.create("c_mktsegment", table));
    dagRequest.setTableInfo(table);
    dagRequest.setStartTs(session.getTimestamp().getVersion());
    dagRequest.addAggregate(new Count(TiColumnRef.create("c_custkey")));
    dagRequest.addGroupByItem(TiByItem.create(TiColumnRef.create("c_mktsegment"), false));
    dagRequest.bind();
    Iterator<com.pingcap.tikv.row.Row> iterator = snapshot.select(dagRequest);
    System.out.println("Show result:");
    while (iterator.hasNext()) {
      Row rowData = iterator.next();
      for (int i = 0; i < rowData.fieldCount(); i++) {
        System.out.print(rowData.get(i, null) + "\t");
      }
      System.out.println();
    }
  }
}
