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
import com.pingcap.tikv.expression.scalar.*;
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
  private TiColumnRef shipdate = TiColumnRef.create("l_shipdate");
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

    bindRanges();
  }

  private void bindRanges() {
    ranges.clear();

    ByteString startKey = TableCodec.encodeRowKeyWithHandle(table.getId(), Long.MIN_VALUE);
    ByteString endKey = TableCodec.encodeRowKeyWithHandle(table.getId(), Long.MAX_VALUE);
    Coprocessor.KeyRange keyRange = Coprocessor.KeyRange.newBuilder().setStart(startKey).setEnd(endKey).build();
    ranges.add(keyRange);
  }

  private void showIterRes(Iterator<Row> iterator) {
    System.out.println("Result is:");
    int count = 0;

    while (iterator.hasNext()) {
      Row rowData = iterator.next();
      count++;
      for (int i = 0; i < rowData.fieldCount(); i++) {
        System.out.print(rowData.get(i, null) + "\t");
      }
      System.out.println();
    }
    System.out.println("Total data count: " + count);
  }

  @Test
  public void testAggGroupBy() {
    TiDAGRequest dagRequest = new TiDAGRequest();
    dagRequest.addRanges(ranges);
    dagRequest.addRequiredColumn(TiColumnRef.create("c_mktsegment", table));
    dagRequest.setTableInfo(table);
    dagRequest.setStartTs(session.getTimestamp().getVersion());
    dagRequest.addAggregate(new Count(custKey));
    dagRequest.addGroupByItem(TiByItem.create(TiColumnRef.create("c_mktsegment"), false));
    dagRequest.resolve();
    Iterator<Row> iterator = snapshot.select(dagRequest);
    Assert.assertTrue(iterator.hasNext());
  }

  @Test
  public void testSelectAll() {
    TiDAGRequest dagRequest = new TiDAGRequest();
    dagRequest.setTableInfo(table);
    dagRequest.addRanges(ranges);
    dagRequest.addRequiredColumn(mktsegment);
    dagRequest.setStartTs(session.getTimestamp().getVersion());
    dagRequest.resolve();
    Iterator<Row> iterator = snapshot.select(dagRequest);
    Assert.assertTrue(iterator.hasNext());
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
    dagRequest.resolve();
    Iterator<Row> iterator = snapshot.select(dagRequest);
    Assert.assertTrue(iterator.hasNext());
  }

  @Test
  public void testSelectCount() {
    TiDAGRequest dagRequest = new TiDAGRequest();
    dagRequest.addRanges(ranges);
    dagRequest.setTableInfo(table);
    dagRequest.setStartTs(session.getTimestamp().getVersion());
    dagRequest.addAggregate(new Count(TiColumnRef.create("c_custkey")));
    dagRequest.resolve();
    Iterator<Row> iterator = snapshot.select(dagRequest);
    Assert.assertTrue(iterator.hasNext());
//    showIterRes(iterator);
  }

  @Test
  public void testSelectWhere() {
    TiDAGRequest dagRequest = new TiDAGRequest();
    dagRequest.addRanges(ranges);
    dagRequest.setTableInfo(table);
    dagRequest.addRequiredColumn(TiColumnRef.create("c_custkey"));
    dagRequest.setStartTs(session.getTimestamp().getVersion());
    dagRequest.addWhere(new Equal(TiConstant.create(3), TiConstant.create(1)));
    dagRequest.resolve();
    Iterator<Row> iterator = snapshot.select(dagRequest);
    Assert.assertTrue(!iterator.hasNext());
  }

  @Test
  public void testComplexGB() {
    TiDAGRequest dagRequest = new TiDAGRequest();
    dagRequest.addRanges(ranges);
    dagRequest.setTableInfo(table);
    dagRequest.addRequiredColumn(custKey);
    dagRequest.setStartTs(session.getTimestamp().getVersion());
    dagRequest.addWhere(new Equal(TiConstant.create(1), TiConstant.create(1)));
    dagRequest.resolve();
    Iterator<Row> iterator = snapshot.select(dagRequest);
    Assert.assertTrue(iterator.hasNext());
  }

  @Test
  public void testComplexWhere() {
    TiDAGRequest dagRequest = new TiDAGRequest();
    dagRequest.addRanges(ranges);
    dagRequest.addRequiredColumn(mktsegment);
    dagRequest.setTableInfo(table);
    dagRequest.setStartTs(session.getTimestamp().getVersion());
    dagRequest.addWhere(new Equal(new Plus(TiConstant.create(1), custKey), TiConstant.create(4)));
    dagRequest.resolve();
    Iterator<Row> iterator = snapshot.select(dagRequest);
    Assert.assertTrue(iterator.hasNext());
  }

  @Test
  public void testExpBug() {
    table = cat.getTable(db, "lineitem");
    bindRanges();

    TiDAGRequest dagRequest = new TiDAGRequest();
    dagRequest.addRanges(ranges);
    dagRequest.setTableInfo(table);
    dagRequest.addRequiredColumn(shipdate);
    dagRequest.setStartTs(session.getTimestamp().getVersion());
    dagRequest.setLimit(65);
    dagRequest.resolve();
    Iterator<Row> iterator = snapshot.select(dagRequest);
    Assert.assertTrue(iterator.hasNext());
  }

  private void showIterCount(Iterator<Row> iterator) {
    int count = 0;
    while (iterator.hasNext()) {
      count++;
    }
    System.out.println("Data count: " + count);
  }
}
