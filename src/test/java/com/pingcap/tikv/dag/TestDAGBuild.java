package com.pingcap.tikv.dag;

import com.google.protobuf.ByteString;
import com.pingcap.tikv.PDClient;
import com.pingcap.tikv.Snapshot;
import com.pingcap.tikv.TiConfiguration;
import com.pingcap.tikv.TiSession;
import com.pingcap.tikv.catalog.Catalog;
import com.pingcap.tikv.codec.TableCodec;
import com.pingcap.tikv.expression.TiByItem;
import com.pingcap.tikv.expression.TiColumnRef;
import com.pingcap.tikv.expression.TiConstant;
import com.pingcap.tikv.expression.aggregate.Count;
import com.pingcap.tikv.expression.scalar.Equal;
import com.pingcap.tikv.expression.scalar.Plus;
import com.pingcap.tikv.kvproto.Coprocessor;
import com.pingcap.tikv.meta.TiDAGRequest;
import com.pingcap.tikv.meta.TiDBInfo;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.region.RegionManager;
import com.pingcap.tikv.row.Row;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class TestDAGBuild {
  public static TiConfiguration conf =
          TiConfiguration.createDefault("127.0.0.1:" + 2379);
  public static TiSession session = TiSession.create(conf);
  public static Snapshot snapshot = session.createSnapshot();
  public static RegionManager regionManager = session.getRegionManager();
  public static PDClient pdClient = session.getPDClient();
  public static Catalog cat = session.getCatalog();
  List<Coprocessor.KeyRange> ranges = new ArrayList<>();
  List<TiDBInfo> databaseInfoList = cat.listDatabases();
  public TiColumnRef custKey = TiColumnRef.create("c_custkey");
  public TiColumnRef mktsegment = TiColumnRef.create("c_mktsegment");
  private TiByItem complexGroupBy = TiByItem.create(new Plus(mktsegment, TiConstant.create("1")), false);

  TiTableInfo table;
  TiDBInfo db;

  @Before
  public void initTpch() {
    // May need to save this reference
    db = cat.getDatabase("tpch_test");
    table = cat.getTable(db, "customer");
    Logger log = Logger.getLogger("io.grpc");
    log.setLevel(Level.ALL);

    ByteString startKey = TableCodec.encodeRowKeyWithHandle(table.getId(), Long.MIN_VALUE);
    ByteString endKey = TableCodec.encodeRowKeyWithHandle(table.getId(), Long.MAX_VALUE);
    Coprocessor.KeyRange keyRange = Coprocessor.KeyRange.newBuilder().setStart(startKey).setEnd(endKey).build();
    ranges.add(keyRange);
  }

  private void showIterRes(Iterator<Row> iterator) {
    System.out.println("Result:");
    while (iterator.hasNext()) {
      Row rowData = iterator.next();
      for (int i = 0; i < rowData.fieldCount(); i++) {
        System.out.print(rowData.get(i, null) + "\t");
      }
      System.out.println();
    }
  }

  @Test
  public void testAggGroupBy() {
    TiDAGRequest dagRequest = new TiDAGRequest();
    dagRequest.addRanges(ranges);
    dagRequest.addField(TiColumnRef.create("c_mktsegment", table));
    dagRequest.setTableInfo(table);
    dagRequest.setStartTs(session.getTimestamp().getVersion());
    dagRequest.addAggregate(new Count(custKey));
    dagRequest.addGroupByItem(TiByItem.create(TiColumnRef.create("c_mktsegment"), false));
    dagRequest.bind();
    Iterator<Row> iterator = snapshot.select(dagRequest);
    showIterRes(iterator);
  }

  @Test
  public void testAggMultiGroupBy() {
    TiDAGRequest dagRequest = new TiDAGRequest();
    dagRequest.addRanges(ranges);
    dagRequest.setTableInfo(table);
    dagRequest.setStartTs(session.getTimestamp().getVersion());
    dagRequest.addAggregate(new Count(custKey));
    dagRequest.addGroupByItem(TiByItem.create(TiColumnRef.create("c_name"), false));
    dagRequest.addGroupByItem(TiByItem.create(TiColumnRef.create("c_mktsegment"), false));
    dagRequest.bind();
    Iterator<Row> iterator = snapshot.select(dagRequest);
    showIterRes(iterator);
  }

  @Test
  public void testSelectCount() {
    TiDAGRequest dagRequest = new TiDAGRequest();
    dagRequest.addRanges(ranges);
    dagRequest.setTableInfo(table);
    dagRequest.setStartTs(session.getTimestamp().getVersion());
    dagRequest.addAggregate(new Count(TiColumnRef.create("c_custkey")));
    dagRequest.bind();
    Iterator<Row> iterator = snapshot.select(dagRequest);
    showIterRes(iterator);
  }

  @Test
  public void testSelectWhere() {
    TiDAGRequest dagRequest = new TiDAGRequest();
    dagRequest.addRanges(ranges);
    dagRequest.setTableInfo(table);
    dagRequest.addField(TiColumnRef.create("c_custkey"));
    dagRequest.setStartTs(session.getTimestamp().getVersion());
    dagRequest.addWhere(new Equal(TiConstant.create(3), TiConstant.create(1)));
    dagRequest.bind();
    Iterator<Row> iterator = snapshot.select(dagRequest);
    Assert.assertTrue(!iterator.hasNext());
    showIterRes(iterator);
  }

  @Test
  public void testComplexGB() {
    TiDAGRequest dagRequest = new TiDAGRequest();
    dagRequest.addRanges(ranges);
    dagRequest.setTableInfo(table);
    dagRequest.addField(custKey);
    dagRequest.setStartTs(session.getTimestamp().getVersion());
    dagRequest.addWhere(new Equal(TiConstant.create(1), TiConstant.create(1)));
//    dagRequest.addGroupByItem(complexGroupBy);
    dagRequest.bind();
    Iterator<Row> iterator = snapshot.select(dagRequest);
    Assert.assertTrue(iterator.hasNext());
    showIterRes(iterator);
  }

  @Test
  public void testComplexWhere() {
    TiDAGRequest dagRequest = new TiDAGRequest();
    dagRequest.addRanges(ranges);
    dagRequest.addField(mktsegment);
    dagRequest.setTableInfo(table);
    dagRequest.setStartTs(session.getTimestamp().getVersion());
    dagRequest.addWhere(new Equal(new Plus(TiConstant.create(1), custKey), TiConstant.create(4)));
    dagRequest.bind();
    Iterator<Row> iterator = snapshot.select(dagRequest);
    Assert.assertTrue(iterator.hasNext());
    showIterRes(iterator);
  }
}
