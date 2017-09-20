package com.pingcap.tikv.statistics;

import com.google.common.collect.ImmutableList;
import com.pingcap.tikv.expression.TiColumnRef;
import com.pingcap.tikv.expression.TiConstant;
import com.pingcap.tikv.expression.TiExpr;
import com.pingcap.tikv.expression.scalar.Equal;
import com.pingcap.tikv.expression.scalar.GreaterEqual;
import com.pingcap.tikv.expression.scalar.NotEqual;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.types.DataType;
import com.pingcap.tikv.types.DataTypeFactory;
import com.pingcap.tikv.util.MockDBReader;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.pingcap.tikv.types.Types.TYPE_BLOB;
import static com.pingcap.tikv.types.Types.TYPE_LONG;
import static org.junit.Assert.assertTrue;

/**
 * Created by birdstorm on 2017/9/13.
 *
 */
public class TableStatsTest {
  private MockDBReader mockDBReader;
  private DataType blobs = DataTypeFactory.of(TYPE_BLOB);
  private DataType ints = DataTypeFactory.of(TYPE_LONG);

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
    mockDBReader.buildTable("t1", ImmutableList.of("c1", "s1"),
        ImmutableList.of(DataTypeFactory.of(TYPE_LONG), DataTypeFactory.of(TYPE_LONG)), 47);

    String statsBucketTableData = "\b^\b\000\b\002\b\000\b\004\b\002\002\0021\002\0020\b^\b\000\b\002\b\002\b\006\b\002\002\0023\002\0022";

    ArrayList<DataType> dataTypes = new ArrayList<>();
    for(int i = 0; i < 6; i ++) {
      dataTypes.add(ints);
    }
    for(int i = 6; i < 8; i ++) {
      dataTypes.add(blobs);
    }
    mockDBReader.addTableData("stats_buckets", statsBucketTableData, 18, 2, dataTypes);


    TiTableInfo tableInfo = mockDBReader.getTableInfo("stats_buckets");
    List<TiExpr> exprs = ImmutableList.of(
        new Equal(TiColumnRef.create("table_id", tableInfo), TiConstant.create(27)));
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

    tableInfo = mockDBReader.getTableInfo("stats_meta");
    exprs = ImmutableList.of(
        new NotEqual(TiColumnRef.create("table_id", tableInfo), TiConstant.create(1)));
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

    tableInfo = mockDBReader.getTableInfo("stats_histograms");
    exprs = ImmutableList.of(
        new NotEqual(TiColumnRef.create("table_id", tableInfo), TiConstant.create(1)));
    returnFields = ImmutableList.of("table_id", "is_index", "hist_id",
        "distinct_count", "null_count", "modify_count", "version");
    mockDBReader.printRows("stats_meta", exprs, returnFields);
  }

  @Test
  public void testBuild() {
    TableStats tableStats = new TableStats();
    tableStats.build(mockDBReader);
    TiTableInfo tableInfo = mockDBReader.getTableInfo("t1");
    Table t = tableStats.tableStatsFromStorage(mockDBReader, tableInfo);
    assertTrue(t.getColumns().size() > 0);
    System.out.println("name=" + t.getColumns().values().iterator().next().getColumnInfo().getName());
    List<TiExpr> exprs = ImmutableList.of(
        new GreaterEqual(TiColumnRef.create("c1", tableInfo), TiConstant.create(2)));
    assertTrue(t.Selectivity(mockDBReader, exprs) == 0.8);
  }
}
