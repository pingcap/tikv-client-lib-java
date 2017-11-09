package com.pingcap.tikv.util;

import com.google.protobuf.ByteString;
import com.pingcap.tidb.tipb.Chunk;
import com.pingcap.tidb.tipb.RowMeta;
import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.expression.TiBinaryFunctionExpresson;
import com.pingcap.tikv.expression.TiColumnRef;
import com.pingcap.tikv.expression.TiConstant;
import com.pingcap.tikv.expression.TiExpr;
import com.pingcap.tikv.meta.*;
import com.pingcap.tikv.operation.ChunkIterator;
import com.pingcap.tikv.row.ObjectRowImpl;
import com.pingcap.tikv.row.Row;
import com.pingcap.tikv.statistics.Table;
import com.pingcap.tikv.types.DataType;
import com.pingcap.tikv.types.DataTypeFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static com.pingcap.tikv.types.Types.TYPE_BLOB;
import static com.pingcap.tikv.types.Types.TYPE_LONG;

/**
 * Created by birdstorm on 2017/9/12.
 *
 */
public class MockDBReader extends DBReader {
  private String dbName;
  private HashMap<String, table> tables;

  public class table {
    private TiTableInfo tableInfo;
    private List<TiColumnInfo> columnInfoList;
    private List<TiIndexInfo> indexInfoList;
    private List<Row> rows;
    private List<DataType> dataTypes;

    private table() {
      columnInfoList = new ArrayList<>();
      indexInfoList = new ArrayList<>();
      rows = new ArrayList<>();
      dataTypes = new ArrayList<>();
    }

    public MetaUtils.TableBuilder build() {
      return MetaUtils.TableBuilder.newBuilder();
    }

    private TiTableInfo getTableInfo() {
      return tableInfo;
    }

    void setTableInfo(TiTableInfo tableInfo) {
      this.tableInfo = tableInfo;
    }

    private TiColumnInfo getColumnInfo(String columnName) {
      for(TiColumnInfo c: columnInfoList) {
        if(c.getName().equalsIgnoreCase(columnName)) {
          return c;
        }
      }
      return null;
    }

    private void setColumnInfoList(List<TiColumnInfo> columnInfoList) {
      this.columnInfoList.clear();
      this.columnInfoList.addAll(columnInfoList);
    }

    private TiIndexInfo getIndexInfo(String indexName) {
      for(TiIndexInfo i: indexInfoList) {
        if(i.getName().equalsIgnoreCase(indexName)) {
          return i;
        }
      }
      return null;
    }

    private void setIndexInfoList(List<TiIndexInfo> indexInfoList) {
      this.indexInfoList.clear();
      this.indexInfoList.addAll(indexInfoList);
    }

    private TiColumnInfo getColumnInfo(int pos) {
      return columnInfoList.get(pos);
    }

    private TiIndexInfo getIndexInfo(int pos) {
      return indexInfoList.get(pos);
    }

    private DataType getType(int pos) {
      return dataTypes.get(pos);
    }

    private void clear() {
      columnInfoList.clear();
      indexInfoList.clear();
      rows.clear();
      dataTypes.clear();
    }
  }

  public MockDBReader(String DBName) {
    this.dbName = DBName;
    tables = new HashMap<>();
  }

  private void setDBName(String dbName) {
    if(!this.dbName.equalsIgnoreCase(dbName)) {
      this.dbName = dbName;
      clearAll();
    }
  }

  private table getTable(String tableName) {
    return tables.get(tableName);
  }

  private void clearTable(String tableName) {
    getTable(tableName).clear();
  }

  private void clearAll() {
    for(table t: tables.values()) {
      t.clear();
    }
    tables.clear();
  }

  private table getTableForce(String tableName) {
    table t = getTable(tableName);
    if(t == null) {
      addTable(dbName, tableName);
      t = getTable(tableName);
    }
    return t;
  }

  private MetaUtils.TableBuilder buildTableInfo(String tableName) {
    return getTableForce(tableName).build();
  }

  private void setTableInfo(String tableName, TiTableInfo tableInfo) {
    getTable(tableName).setTableInfo(tableInfo);
  }

  private class MockColumn {
    private String columnName;
    private DataType dataType;
    private boolean isPrimaryKey;

    private MockColumn(String columnName, int dataType, boolean isPrimaryKey) {
      this.columnName = columnName;
      this.dataType = DataTypeFactory.of(dataType);
      this.isPrimaryKey = isPrimaryKey;
    }
  }

  public MockColumn createMockColumn(String columnName, int dataType, boolean isPrimaryKey) {
    return new MockColumn(columnName, dataType, isPrimaryKey);
  }

  public MockColumn createMockColumn(String columnName, int dataType) {
    return new MockColumn(columnName, dataType, false);
  }

  private class MockIndex {
    private String indexName;
    private List<String> columnNames;
    private boolean isPrimaryKey;

    private MockIndex(String indexName, List<String> columnNames, boolean isPrimaryKey) {
      this.indexName = indexName;
      this.columnNames = columnNames;
      this.isPrimaryKey = isPrimaryKey;
    }
  }

  public MockIndex createMockIndex(String indexName, List<String> columnNames, boolean isPrimaryKey) {
    return new MockIndex(indexName, columnNames, isPrimaryKey);
  }

  public MockIndex createMockIndex(String indexName, List<String> columnNames) {
    return new MockIndex(indexName, columnNames, false);
  }

  private List<TiIndexColumn> buildIndexColumns(table t, List<String> columNames) {
    List<TiIndexColumn> ret = new ArrayList<>();
    for(String columnName: columNames) {
      TiColumnInfo columnInfo = t.getColumnInfo(columnName);
      if(columnInfo != null) {
        ret.add(columnInfo.toIndexColumn());
      }
    }
    return ret;
  }

  public TiTableInfo buildTable(String tableName, List<MockColumn> mockColumns, List<MockIndex> mockIndices, long tableID) {
    MetaUtils.TableBuilder tableBuilder = buildTableInfo(tableName).name(tableName);
    for(MockColumn mockColumn: mockColumns) {
      tableBuilder = tableBuilder.addColumn(mockColumn.columnName, mockColumn.dataType, mockColumn.isPrimaryKey);
    }
    for(MockIndex mockIndex: mockIndices) {
      tableBuilder = tableBuilder.appendIndex(mockIndex.indexName, mockIndex.columnNames, mockIndex.isPrimaryKey);
    }
    TiTableInfo tableInfo = tableBuilder.build(tableID);
    setTableInfo(tableName, tableInfo);
    int i = 0;
    for(MockColumn mockColumn: mockColumns) {
      TiColumnInfo columnInfo =
          new TiColumnInfo(i + 1, mockColumn.columnName, i, mockColumn.dataType, mockColumn.isPrimaryKey);
      addColumn(tableName, columnInfo);
      i ++;
    }
    i = 0;
    table t = getTable(tableName);
    for(MockIndex mockIndex: mockIndices) {
      TiIndexInfo indexInfo = new TiIndexInfo(i + 1, mockIndex.indexName, tableName,
          buildIndexColumns(t, mockIndex.columnNames), mockIndex.isPrimaryKey);
      addIndex(tableName, indexInfo);
      i ++;
    }
    return tableInfo;
  }

  public TiTableInfo buildStatsBuckets() {
    DataType blobs = DataTypeFactory.of(TYPE_BLOB);
    DataType ints = DataTypeFactory.of(TYPE_LONG);
    TiTableInfo tableInfo = buildTableInfo("stats_buckets")
        .name("stats_buckets")
        .addColumn("table_id", ints)
        .addColumn("is_index", ints)
        .addColumn("hist_id", ints)
        .addColumn("bucket_id", ints)
        .addColumn("count", ints)
        .addColumn("repeats", ints)
        .addColumn("upper_bound", blobs)
        .addColumn("lower_bound", blobs)
        .build();
    setTableInfo("stats_buckets", tableInfo);
    TiColumnInfo columnInfo;
    columnInfo = new TiColumnInfo(1, "table_id", 0, DataTypeFactory.of(TYPE_LONG), false);
    addColumn("stats_buckets", columnInfo);
    columnInfo = new TiColumnInfo(2, "is_index", 1, DataTypeFactory.of(TYPE_LONG), false);
    addColumn("stats_buckets", columnInfo);
    columnInfo = new TiColumnInfo(3, "hist_id", 2, DataTypeFactory.of(TYPE_LONG), false);
    addColumn("stats_buckets", columnInfo);
    columnInfo = new TiColumnInfo(4, "bucket_id", 3, DataTypeFactory.of(TYPE_LONG), false);
    addColumn("stats_buckets", columnInfo);
    columnInfo = new TiColumnInfo(5, "count", 4, DataTypeFactory.of(TYPE_LONG), false);
    addColumn("stats_buckets", columnInfo);
    columnInfo = new TiColumnInfo(6, "repeats", 5, DataTypeFactory.of(TYPE_LONG), false);
    addColumn("stats_buckets", columnInfo);
    columnInfo = new TiColumnInfo(7, "upper_bound", 6, DataTypeFactory.of(TYPE_BLOB), false);
    addColumn("stats_buckets", columnInfo);
    columnInfo = new TiColumnInfo(8, "lower_bound", 7, DataTypeFactory.of(TYPE_BLOB), false);
    addColumn("stats_buckets", columnInfo);
    return tableInfo;
  }

  public TiTableInfo buildStatsHistograms() {
    DataType ints = DataTypeFactory.of(TYPE_LONG);
    TiTableInfo tableInfo = buildTableInfo("stats_histograms")
        .name("stats_histograms")
        .addColumn("table_id", ints)
        .addColumn("is_index", ints)
        .addColumn("hist_id", ints)
        .addColumn("distinct_count", ints)
        .addColumn("null_count", ints)
        .addColumn("modify_count", ints)
        .addColumn("version", ints)
        .build();
    setTableInfo("stats_histograms", tableInfo);
    TiColumnInfo columnInfo;
    columnInfo = new TiColumnInfo(1, "table_id", 0, DataTypeFactory.of(TYPE_LONG), false);
    addColumn("stats_histograms", columnInfo);
    columnInfo = new TiColumnInfo(2, "is_index", 1, DataTypeFactory.of(TYPE_LONG), false);
    addColumn("stats_histograms", columnInfo);
    columnInfo = new TiColumnInfo(3, "hist_id", 2, DataTypeFactory.of(TYPE_LONG), false);
    addColumn("stats_histograms", columnInfo);
    columnInfo = new TiColumnInfo(4, "distinct_count", 3, DataTypeFactory.of(TYPE_LONG), false);
    addColumn("stats_histograms", columnInfo);
    columnInfo = new TiColumnInfo(5, "null_count", 4, DataTypeFactory.of(TYPE_LONG), false);
    addColumn("stats_histograms", columnInfo);
    columnInfo = new TiColumnInfo(6, "modify_count", 5, DataTypeFactory.of(TYPE_LONG), false);
    addColumn("stats_histograms", columnInfo);
    columnInfo = new TiColumnInfo(7, "version", 6, DataTypeFactory.of(TYPE_LONG), false);
    addColumn("stats_histograms", columnInfo);
    return tableInfo;
  }

  public TiTableInfo buildStatsMeta() {
    DataType ints = DataTypeFactory.of(TYPE_LONG);
    TiTableInfo tableInfo = buildTableInfo("stats_meta")
        .name("stats_meta")
        .addColumn("version", ints)
        .addColumn("table_id", ints)
        .addColumn("modify_count", ints)
        .addColumn("count", ints)
        .build();
    setTableInfo("stats_meta", tableInfo);
    TiColumnInfo columnInfo;
    columnInfo = new TiColumnInfo(1, "version", 0, DataTypeFactory.of(TYPE_LONG), false);
    addColumn("stats_meta", columnInfo);
    columnInfo = new TiColumnInfo(2, "table_id", 1, DataTypeFactory.of(TYPE_LONG), false);
    addColumn("stats_meta", columnInfo);
    columnInfo = new TiColumnInfo(3, "modify_count", 2, DataTypeFactory.of(TYPE_LONG), false);
    addColumn("stats_meta", columnInfo);
    columnInfo = new TiColumnInfo(4, "count", 3, DataTypeFactory.of(TYPE_LONG), false);
    addColumn("stats_meta", columnInfo);
    return tableInfo;
  }

  public void addTable(String dbName, String tableName) {
    setDBName(dbName);
    if(getTable(tableName) != null) {
      clearTable(tableName);
    }
    tables.put(tableName, new table());
  }

  private void addColumn(String tableName, TiColumnInfo columnInfo) {
    table t = getTable(tableName);
    if(t == null) {
      throw new NullPointerException("table" + tableName + "not found.");
    } else {
      t.columnInfoList.add(columnInfo);
    }
  }

  private void addIndex(String tableName, TiIndexInfo indexInfo) {
    table t = getTable(tableName);
    if(t == null) {
      throw new NullPointerException("table" + tableName + "not found.");
    } else {
      t.indexInfoList.add(indexInfo);
    }
  }

  public void addTableData(String tableName, String tableDataStr, int length, int rowCount, List<DataType> types) {
    List<Chunk> chunks = new ArrayList<>();

    byte[] buf = new byte[tableDataStr.length()];
    for (int i = 0; i < tableDataStr.length(); i++) {
      buf[i] = (byte)tableDataStr.charAt(i);
    }

    Chunk.Builder builder = Chunk.newBuilder().setRowsData(ByteString.copyFrom(buf));
    for(int i = 0; i < rowCount; i ++) {
      builder = builder.addRowsMeta(i, RowMeta.newBuilder().setHandle(i + 8).setLength(length));
    }
    Chunk chunk = builder.build();
    chunks.add(chunk);

    ChunkIterator chunkIterator = new ChunkIterator(chunks);
    DataType blobs = DataTypeFactory.of(TYPE_BLOB);
    DataType ints = DataTypeFactory.of(TYPE_LONG);

    CodecDataInput cdi;
    table t = getTable(tableName);
    t.dataTypes.addAll(types);
    t.rows.clear();
    int n = types.size();

    for(int i = 0; i < rowCount; i ++) {
      cdi = new CodecDataInput(chunkIterator.next());
      Row row = ObjectRowImpl.create(n);
      for(int j = 0; j < n; j ++) {
        DataType tp = types.get(j);
        if(tp.getClass().equals(ints.getClass())) {
          ints.decodeValueToRow(cdi, row, j);
        } else if(tp.getClass().equals(blobs.getClass())) {
          blobs.decodeValueToRow(cdi, row, j);
        } else {
          System.out.println("No matching types.");
        }
      }
      t.rows.add(row);
    }
  }

  public TiColumnInfo getColumnInfo(String tableName, String columnName) {
    return getTable(tableName).getColumnInfo(columnName);
  }

  public TiIndexInfo getIndexInfo(String tableName, String indexName) {
    return getTable(tableName).getIndexInfo(indexName);
  }

  @Override
  public List<Row> getSelectedRows(String tableName, List<TiExpr> exprs, List<String> returnFields) {
    table t = this.tables.get(tableName);
    if(t == null) {
      return new ArrayList<>();
    } else {
      return getSelectedRows(t, exprs, returnFields);
    }
  }

  private boolean checkOps(List<TiExpr> e, Row row, List<TiColumnInfo> columnInfoList, String Op) {
    TiExpr e0 = e.get(0);
    TiExpr e1 = e.get(1);
    for(int i = 0; i < row.fieldCount(); i ++) {
      String columnName = columnInfoList.get(i).getName();
      boolean ok = false;

      if(e0 instanceof TiConstant && e1 instanceof TiColumnRef && ((TiColumnRef) e1).getName().equalsIgnoreCase(columnName)) {
        TiKey<ByteString> r = TiKey.encode(row.get(i, e0.getType()));
        TiKey<ByteString> c = TiKey.encode(((TiConstant) e0).getValue());

        switch (Op) {
          case "=":
            ok = r.compareTo(c) == 0;
            break;
          case "NullEqual":
            // not done
            break;
          case ">=":
            ok = r.compareTo(c) <= 0;
            break;
          case ">":
            ok = r.compareTo(c) < 0;
            break;
          case "<=":
            ok = r.compareTo(c) >= 0;
            break;
          case "<":
            ok = r.compareTo(c) > 0;
            break;
          case "<>":
            ok = r.compareTo(c) != 0;
            break;
          default:
            // shouldn't be here
            System.out.println("unknown or unsupported operator " + Op);
        }
      } else if(e1 instanceof TiConstant && e0 instanceof TiColumnRef && ((TiColumnRef) e0).getName().equalsIgnoreCase(columnName)) {
        TiKey<ByteString> r = TiKey.encode(row.get(i, e1.getType()));
        TiKey<ByteString> c = TiKey.encode(((TiConstant) e1).getValue());

        switch (Op) {
          case "=":
            ok = r.compareTo(c) == 0;
            break;
          case "NullEqual":
            // not done
            break;
          case ">=":
            ok = r.compareTo(c) >= 0;
            break;
          case ">":
            ok = r.compareTo(c) > 0;
            break;
          case "<=":
            ok = r.compareTo(c) <= 0;
            break;
          case "<":
            ok = r.compareTo(c) < 0;
            break;
          case "<>":
            ok = r.compareTo(c) != 0;
            break;
          default:
            // shouldn't be here
            System.out.println("unknown or unsupported operator " + Op);
        }
      }
      if(ok) {
        return true;
      }
    }
    return false;
  }

  private List<Row> getSelectedRows(table t, List<TiExpr> exprs, List<String> returnFields) {
    List<Row> ret = new ArrayList<>();
    int n = returnFields.size();
    next: for(Row row: t.rows) {
      for(TiExpr expr: exprs) {
        if (expr instanceof TiBinaryFunctionExpresson) {
          List<TiExpr> e = ((TiBinaryFunctionExpresson) expr).getArgs();
          if (Table.checkColumnConstant(e)) {
            if(!checkOps(e, row, t.columnInfoList, ((TiBinaryFunctionExpresson) expr).getName())) {
              continue next;
            }
          }
        }
      }
      Row cur = ObjectRowImpl.create(n);
      int cnt = 0;
      for(String s: returnFields) {
        for(int i = 0; i < row.fieldCount(); i ++) {
          DataType dataType = t.getType(i);
          String columnName = t.columnInfoList.get(i).getName();
          if(s.equalsIgnoreCase(columnName)) {
            cur.set(cnt ++, dataType, row.get(i, dataType));
          }
        }
      }
      ret.add(cur);
    }
    System.out.println("altogether " + ret.size() + " rows from "
        + t.getTableInfo().getName() + "#" + t.getTableInfo().getId());
    return ret;
  }

  // TODO: will be extremely slow on large data set
  public void printRows(String tableName, List<TiExpr> exprs, List<String> returnFields) {
    table t = getTable(tableName);
    System.out.println(">>>>>>>>>>>>>" + tableName);
    if(t != null) {
      System.out.println(returnFields);
      next:
      for (Row r : t.rows) {
        for(TiExpr expr: exprs) {
          if (expr instanceof TiBinaryFunctionExpresson) {
            List<TiExpr> e = ((TiBinaryFunctionExpresson) expr).getArgs();
            if (Table.checkColumnConstant(e)) {
              if(!checkOps(e, r, t.columnInfoList, ((TiBinaryFunctionExpresson) expr).getName())) {
                continue next;
              }
            }
          }
        }
        for(String s: returnFields) {
          for (int i = 0; i < r.fieldCount(); i++) {
            Object val = r.get(i, t.getType(i));
            if(s.equalsIgnoreCase(t.getTableInfo().getColumns().get(i).getName())) {
              System.out.print(TiKey.create(val) + " ");
            }
          }
        }
        System.out.print("\n");
      }
    }
    System.out.println("<<<<<<<<<<<<<<");
  }

  @Override
  public TiTableInfo getTableInfo(String tableName) {
    return getTable(tableName).getTableInfo();
  }

  @Override
  public TiTableInfo getTableInfo(long tableID) {
    for(table s: tables.values()) {
      if(s.getTableInfo().getId() == tableID) {
        return s.getTableInfo();
      }
    }
    return null;
  }

}
