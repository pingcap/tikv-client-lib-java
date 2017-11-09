package com.pingcap.tikv.statistics;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.pingcap.tikv.codec.CodecDataOutput;
import com.pingcap.tikv.expression.TiColumnRef;
import com.pingcap.tikv.expression.TiConstant;
import com.pingcap.tikv.expression.TiExpr;
import com.pingcap.tikv.expression.scalar.*;
import com.pingcap.tikv.meta.TiKey;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.types.DataType;
import com.pingcap.tikv.types.DataTypeFactory;
import com.pingcap.tikv.util.Bucket;
import com.pingcap.tikv.util.MockDBReader;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.pingcap.tikv.types.Types.TYPE_BLOB;
import static com.pingcap.tikv.types.Types.TYPE_LONG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by birdstorm on 2017/9/13.
 *
 */
public class TableStatsTest {
  private MockDBReader mockDBReader;
  private DataType blobs = DataTypeFactory.of(TYPE_BLOB);
  private DataType ints = DataTypeFactory.of(TYPE_LONG);

  private static TiTableInfo t1TableInfo;
  private static TiTableInfo histogramInfo;
  private static TiTableInfo metaInfo;
  private static TiTableInfo bucketsInfo;

  @Before
  public void setUp() {
    mockDBReader = new MockDBReader("mysql");
    mockDBReader.addTable("mysql", "stats_histograms");
    mockDBReader.addTable("mysql", "stats_meta");
    mockDBReader.addTable("mysql", "stats_buckets");
    mockDBReader.addTable("mysql", "t1");

    mockDBReader.buildStatsBuckets();
    mockDBReader.buildStatsHistograms();
    mockDBReader.buildStatsMeta();
    mockDBReader.buildTable(
        "t1",
        ImmutableList.of(
            mockDBReader.createMockColumn("c1", TYPE_LONG, true),
            mockDBReader.createMockColumn("s1", TYPE_LONG)
        ),
        ImmutableList.of(
            mockDBReader.createMockIndex("idx_c1", ImmutableList.of("c1"), true),
            mockDBReader.createMockIndex("idx_s1", ImmutableList.of("s1"))
        ),
        47);
    String t1TableData = "\b\002\b\002\b\004\b\004\b\006\b\006";
    mockDBReader.addTableData("t1", t1TableData, 4, 3,
        ImmutableList.of(DataTypeFactory.of(TYPE_LONG), DataTypeFactory.of(TYPE_LONG)));

    t1TableInfo = mockDBReader.getTableInfo("t1");


    String statsBucketTableData = "\b^\b\000\b\002\b\000\b\004\b\002\002\0021\002\0020\b^\b\000\b\002\b\002\b\006\b\002\002\0023\002\0022";

    ArrayList<DataType> dataTypes = new ArrayList<>();
    for(int i = 0; i < 6; i ++) {
      dataTypes.add(ints);
    }
    for(int i = 6; i < 8; i ++) {
      dataTypes.add(blobs);
    }
    mockDBReader.addTableData("stats_buckets", statsBucketTableData, 18, 2, dataTypes);

    bucketsInfo = mockDBReader.getTableInfo("stats_buckets");
    List<TiExpr> exprs = ImmutableList.of(
        new NotEqual(TiColumnRef.create("table_id", bucketsInfo), TiConstant.create(1)));
    List<String> returnFields = ImmutableList.of(
        "table_id", "is_index", "hist_id", "bucket_id", "count", "repeats", "upper_bound", "lower_bound");
    mockDBReader.printRows("stats_buckets", exprs, returnFields);


    String statsMetaTableData = "\t\216\200\220\203\351\257\314\273\005\b:\b\000\t\000\t\215\200\300\211\351\257\314\273\005\b>\b\000\t" +
        "\000\t\214\200\360\225\351\257\314\273\005\bB\b\000\t\000\t\215\200\240\234\351\257\314\273\005\bF\b\000\t\000\t\213\200\320\250" +
        "\351\257\314\273\005\bJ\b\000\t\000\t\201\200\260\265\351\257\314\273\005\bN\b\000\t\000\t\215\200\340\273\351\257\314\273\005" +
        "\bR\b\000\t\000\t\201\200\220\310\351\257\314\273\005\bV\b\000\t\000\t\202\200\300\355\252\345\320\274\005\bZ\b\006\t\003\t\202" +
        "\200\240\350\223\211\321\274\005\b^\b\006\t\003\t\202\200\200\252\250\261\213\275\005\bf\b\006\t\003";
    dataTypes = new ArrayList<>();
    for(int i = 0; i < 4; i ++) {
      dataTypes.add(ints);
    }
    mockDBReader.addTableData("stats_meta", statsMetaTableData, 16, 11, dataTypes);

    metaInfo = mockDBReader.getTableInfo("stats_meta");
    exprs = ImmutableList.of(
        new NotEqual(TiColumnRef.create("table_id", metaInfo), TiConstant.create(1)));
    returnFields = ImmutableList.of("version", "table_id", "modify_count", "count");
    mockDBReader.printRows("stats_meta", exprs, returnFields);


    String statsHistogramsData = "\bZ\b\000\b\004\b\000\b\000\b\000\t\210\200\260\331\225\246\317\273\005\b^\b\000\b\004\b\000\b\000\b" +
        "\000\t\201\200\320\253\357\210\321\274\005\bf\b\000\b\002\b\000\b\000\b\000\t\203\200\240\367\204\261\213\275\005\bf\b\000\b" +
        "\004\b\000\b\000\b\000\t\203\200\240\367\204\261\213\275\005\b^\b\002\b\004\b\006\b\000\b\000\t\201\200\360\315\240\200\231\275" +
        "\005\b^\b\002\b\002\b\006\b\000\b\000\t\207\200\240\324\240\200\231\275\005\b^\b\000\b\002\b\006\b\000\b\000\t\211\200\240\324" +
        "\240\200\231\275\005";
    dataTypes = new ArrayList<>();
    for(int i = 0; i < 7; i ++) {
      dataTypes.add(ints);
    }
    mockDBReader.addTableData("stats_histograms", statsHistogramsData, 22, 7, dataTypes);

    histogramInfo = mockDBReader.getTableInfo("stats_histograms");
    exprs = ImmutableList.of(
        new NotEqual(TiColumnRef.create("table_id", histogramInfo), TiConstant.create(1)));
    returnFields = ImmutableList.of("table_id", "is_index", "hist_id",
        "distinct_count", "null_count", "modify_count", "version");
    mockDBReader.printRows("stats_histograms", exprs, returnFields);
  }

  @Test
  public void testBuild() {
    List<TiExpr> exprsTest = ImmutableList.of(
        new NotEqual(TiColumnRef.create("c1", t1TableInfo), TiConstant.create(1)));
    List<String> returnFieldsTest = ImmutableList.of("c1", "s1");
    System.out.println("==========Build=========");
    mockDBReader.printRows("t1", exprsTest, returnFieldsTest);

    TableStats tableStats = new TableStats();
    tableStats.build(mockDBReader);
//    System.out.println("id = " + t1TableInfo.getId());
    Table t = tableStats.tableStatsFromStorage(mockDBReader, t1TableInfo);
    assertTrue(t.getColumns().size() > 0);
//    System.out.println("name=" + t.getColumns().values().iterator().next().getColumnInfo().getName());
    List<TiExpr> exprs = ImmutableList.of(
        new GreaterEqual(TiColumnRef.create("c1", t1TableInfo), TiConstant.create(1)));
    System.out.println("selectivity = " + t.Selectivity(mockDBReader, exprs));
  }

  private ByteString[] generateData(int dimension, int num) {
    int len = ((int) Math.round(Math.pow(num, dimension)));
    ByteString[] ret = new ByteString[len];
    DataType t = DataTypeFactory.of(TYPE_LONG);
    if(dimension == 1) {
      CodecDataOutput cdo = new CodecDataOutput();
      for(int i = 0; i < num; i ++) {
        cdo.reset();
        t.encode(cdo, DataType.EncodeType.KEY, (long)i);
        ret[i] = cdo.toByteString();
      }
    } else {
      for(int i = 0; i < len; i ++) {
        long j = i;
        CodecDataOutput cdo = new CodecDataOutput();
        long tmp[] = new long[dimension];
        for(int k = 0; k < dimension; k ++) {
          tmp[dimension-k-1] = j % num;
          j = j / num;
        }
        for(int k = 0; k < dimension; k ++) {
          t.encode(cdo, DataType.EncodeType.KEY, tmp[k]);
        }
        ret[i] = cdo.toByteString();
      }
    }
    return ret;
  }

  // mockStatsHistogram will create a statistics.Histogram, of which the data is uniform distribution.
  private Histogram mockStatsHistogram(long id, ByteString[] values, long repeat) {
    int ndv = values.length;
    Histogram histogram = new Histogram(id, ndv, new ArrayList<>());
    byte[] byte0 = new byte[1];
    byte0[0] = (byte) 0;
    for(int i = 0; i < ndv; i ++) {
      Bucket bucket = new Bucket(TiKey.create(values[i]));
      bucket.setRepeats(repeat);
      bucket.setCount(repeat * (i + 1));
      if(i > 0) {
        bucket.setLowerBound(TiKey.create(values[i - 1].concat(ByteString.copyFrom(byte0))));
      }
      histogram.getBuckets().add(bucket);
    }
    return histogram;
  }

  private Table mockStatsTable(TiTableInfo tbl, long rowCount) {
    return new Table(tbl.getId(), rowCount, false);
  }

  private class test{
    private List<TiExpr> exprs;
    private double selectivity;
    private test(List<TiExpr> e, double d) {
      exprs = new ArrayList<>();
      exprs.addAll(e);
      selectivity = d;
    }
  }

  @Test
  public void testSelectivity() {

    System.out.println("========SelectivityTest=======");

    mockDBReader.addTable("mysql", "t");
    mockDBReader.buildTable(
        "t",
        ImmutableList.of(
            mockDBReader.createMockColumn("a", TYPE_LONG, true),
            mockDBReader.createMockColumn("b", TYPE_LONG),
            mockDBReader.createMockColumn("c", TYPE_LONG),
            mockDBReader.createMockColumn("d", TYPE_LONG),
            mockDBReader.createMockColumn("e", TYPE_LONG)
        ),
        ImmutableList.of(
          mockDBReader.createMockIndex("idx_cd", ImmutableList.of("c", "d")),
          mockDBReader.createMockIndex("idx_de", ImmutableList.of("d", "e"))
        ),
        51);

    TiTableInfo tbl = mockDBReader.getTableInfo("t");
    Table statsTbl = mockStatsTable(tbl, 540);

    ByteString[] colValues = generateData(1, 54);
    for(int i = 1; i <= 5; i ++) {
      statsTbl.putColumns(i, new ColumnWithHistogram(mockStatsHistogram(i, colValues, 10), tbl.getColumns().get(i - 1)));
    }

    ByteString[] idxValues = generateData(2, 3);
    statsTbl.putIndices(1, new IndexWithHistogram(mockStatsHistogram(1, idxValues, 60), tbl.getIndices().get(0)));
    statsTbl.putIndices(2, new IndexWithHistogram(mockStatsHistogram(2, idxValues, 60), tbl.getIndices().get(1)));

    final test[] tests = {
        new test(
            ImmutableList.of(
                new Equal(TiColumnRef.create("a", tbl), TiConstant.create((long) 1))
            ),
            0.01851851851
        ),
        new test(
            ImmutableList.of(
                new GreaterThan(TiColumnRef.create("a", tbl), TiConstant.create((long) 0)),
                new LessThan(TiColumnRef.create("a", tbl), TiConstant.create((long) 2))
            ),
            0.01851851851
        ),
        new test(
            ImmutableList.of(
                new GreaterEqual(TiColumnRef.create("a", tbl), TiConstant.create((long) 1)),
                new LessThan(TiColumnRef.create("a", tbl), TiConstant.create((long) 2))
            ),
            0.01851851851
        ),
        new test(
            ImmutableList.of(
                new GreaterEqual(TiColumnRef.create("a", tbl), TiConstant.create((long) 1)),
                new GreaterThan(TiColumnRef.create("b", tbl), TiConstant.create((long) 1)),
                new LessThan(TiColumnRef.create("a", tbl), TiConstant.create((long) 2))
            ),
            0.01783264746
        ),
        new test(
            ImmutableList.of(
                new GreaterEqual(TiColumnRef.create("a", tbl), TiConstant.create((long) 1)),
                new GreaterThan(TiColumnRef.create("c", tbl), TiConstant.create((long) 1)),
                new LessThan(TiColumnRef.create("a", tbl), TiConstant.create((long) 2))
            ),
            0.00617283950
        ),
        new test(
            ImmutableList.of(
                new GreaterEqual(TiColumnRef.create("a", tbl), TiConstant.create((long) 1)),
                new GreaterEqual(TiColumnRef.create("c", tbl), TiConstant.create((long) 1)),
                new LessThan(TiColumnRef.create("a", tbl), TiConstant.create((long) 2))
            ),
            0.01234567901
        ),
        new test(
            ImmutableList.of(
                new Equal(TiColumnRef.create("d", tbl), TiConstant.create((long) 0)),
                new Equal(TiColumnRef.create("e", tbl), TiConstant.create((long) 1))
            ),
            0.11111111111
        ),
        new test(
            ImmutableList.of(
                new GreaterThan(TiColumnRef.create("b", tbl), TiConstant.create((long) 1))
            ),
            0.96296296296
        ),
        new test(
            ImmutableList.of(
                new GreaterThan(TiColumnRef.create("c", tbl), TiConstant.create((long) 1))
            ),
            0.33333333333
        ),
        new test(
            ImmutableList.of(
                new GreaterThan(TiColumnRef.create("a", tbl), TiConstant.create((long) 1)),
                new LessThan(TiColumnRef.create("b", tbl), TiConstant.create((long) 2)),
                new GreaterThan(TiColumnRef.create("c", tbl), TiConstant.create((long) 3)),
                new LessThan(TiColumnRef.create("d", tbl), TiConstant.create((long) 4)),
                new GreaterThan(TiColumnRef.create("e", tbl), TiConstant.create((long) 5))
            ),
            0.00123287439
        ),
    };

    int testCount = 0;
    for(test g: tests) {
      System.out.println("TEST #" + ++testCount + ": " + g.exprs);
      double selectivity = statsTbl.Selectivity(mockDBReader, g.exprs);
      System.out.println("selectivity = " + selectivity);
      assertEquals(selectivity, g.selectivity, 0.000001);
    }

  }

}
