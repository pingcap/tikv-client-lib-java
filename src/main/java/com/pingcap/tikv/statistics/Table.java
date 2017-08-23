package com.pingcap.tikv.statistics;

import com.google.common.collect.Range;
import com.pingcap.tidb.tipb.ColumnInfo;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.predicates.RangeBuilder.IndexRange;
import com.pingcap.tikv.types.DataType;
import com.pingcap.tikv.util.Comparables;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by birdstorm on 2017/8/16.
 */
public class Table {
  private static final long pseudoRowCount = 10000;
  private static final long pseudoEqualRate = 1000;
  private static final long pseudoLessRate = 3;
  private static final long pseudoBetweenRate = 40;

  private long TableID;
  public HashMap<Long, Column> Columns;
  private HashMap<Long, Index> Indices;
  public long Count; // Total row count in a table.
  public long ModifyCount; // Total modify count in a table.
  public long Version;
  private boolean Pseudo;

  public Table() {
  }

  public Table(long tableId, long count, boolean pseudo) {
    this.TableID = tableId;
    this.Columns = new HashMap<>();
    this.Indices = new HashMap<>();
    this.Count = count;
    this.Pseudo = pseudo;
  }

  public Table copy() {
    Table ret = new Table(this.TableID, this.Count, this.Pseudo);
    ret.Columns.putAll(this.Columns);
    ret.Indices.putAll(this.Indices);
    return ret;
  }

  // ColumnIsInvalid checks if this column is invalid.
  private boolean ColumnIsInvalid(ColumnInfo info) {
    if (Pseudo) {
      return true;
    }
    Column column = Columns.get(info.getColumnId());
    return column.getHistogram() == null || column.getHistogram().getBuckets().length == 0;
  }

  // ColumnGreaterRowCount estimates the row count where the column greater than value.
  public double ColumnGreaterRowCount(Comparable value, ColumnInfo columnInfo) {
    if (ColumnIsInvalid(columnInfo)) {
      return (double) (Count) / pseudoLessRate;
    }
    Histogram hist = Columns.get(columnInfo.getColumnId()).getHistogram();
    double result = hist.greaterRowCount(value);
    result *= hist.getIncreaseFactor(Count);
    return result;
  }

  // ColumnLessRowCount estimates the row count where the column less than value.
  public double ColumnLessRowCount(Comparable value, ColumnInfo columnInfo) {
    if (ColumnIsInvalid(columnInfo)) {
      return (double) (Count) / pseudoLessRate;
    }
    Histogram hist = Columns.get(columnInfo.getColumnId()).getHistogram();
    double result = hist.lessRowCount(value);
    result *= hist.getIncreaseFactor(Count);
    return result;
  }

  // ColumnBetweenRowCount estimates the row count where column greater or equal to a and less than b.
  public double ColumnBetweenRowCount(Comparable a, Comparable b, ColumnInfo columnInfo) {
    if (ColumnIsInvalid(columnInfo)) {
      return (double) (Count) / pseudoBetweenRate;
    }
    Histogram hist = Columns.get(columnInfo.getColumnId()).getHistogram();
    double result = hist.betweenRowCount(a, b);
    result *= hist.getIncreaseFactor(Count);
    return result;
  }

  // ColumnEqualRowCount estimates the row count where the column equals to value.
  public double ColumnEqualRowCount(Comparable value, ColumnInfo columnInfo) {
    if (ColumnIsInvalid(columnInfo)) {
      return (double) (Count) / pseudoEqualRate;
    }
    Histogram hist = Columns.get(columnInfo.getColumnId()).getHistogram();
    double result = hist.equalRowCount(value);
    result *= hist.getIncreaseFactor(Count);
    return result;
  }

  // GetRowCountByColumnRanges estimates the row count by a slice of ColumnRange.
  public double GetRowCountByColumnRanges(long columID, List<IndexRange> columnRanges) {
    Column c = Columns.get(columID);
    Histogram hist = c.getHistogram();
    if (Pseudo || hist == null || hist.getBuckets().length == 0) {
      return getPseudoRowCountByColumnRanges(columnRanges, Count);
    }
    return c.getColumnRowCount(columnRanges);
  }

  // GetRowCountByColumnRanges estimates the row count by a slice of ColumnRange.
  public double GetRowCountByIndexRanges(long indexID, List<IndexRange> indexRanges, TiTableInfo tableInfo) {
    Index i = Indices.get(indexID);
    Histogram hist = i.getHistogram();
    if (Pseudo || hist == null || hist.getBuckets().length == 0) {
      return getPseudoRowCountByIndexRanges(indexRanges, Count);
    }
    return i.getRowCount(indexRanges, tableInfo);
  }

  public Table PseudoTable(long tableID) {
    return new Table(tableID, pseudoRowCount, true);
  }

  private double getPseudoRowCountByIndexRanges(List<IndexRange> indexRanges, double tableRowCount) {
    if (Math.abs(tableRowCount) < 1e-6) {
      return 0;
    }
    double totalCount = 0;
    for (IndexRange indexRange : indexRanges) {
      double count = tableRowCount;
      List<Object> L = indexRange.getAccessPoints();
      List<IndexRange> indexList = new ArrayList<>();
      int len = L.size();
      Range rg = indexRange.getRange();
      double rowCount;
      if (rg != null) {
        IndexRange r = new IndexRange(null, null, rg, indexRange.getRangeType());
        indexList.add(r);
        rowCount = getPseudoRowCountByColumnRanges(indexList, totalCount);
      } else {
        List<Object> lst = new ArrayList<>();
        lst.add(L.get(len - 1));
        List<DataType> types = new ArrayList<>();
        types.add(indexRange.getTypes().get(len - 1));
        IndexRange r = new IndexRange(lst, types, null, null);
        indexList.add(r);
        rowCount = getPseudoRowCountByColumnRanges(indexList, totalCount);
        --len;
      }
      count = count / tableRowCount * rowCount;
      for (int i = 0; i < len; i++) {
        count = count / 100;
      }
      totalCount += count;
    }
    if (totalCount > tableRowCount) {
      totalCount = tableRowCount / 3.0;
    }
    return totalCount;
  }

  private double getPseudoRowCountByColumnRanges(List<IndexRange> columnRanges, double tableRowCount) {
    double rowCount = 0;
    for (IndexRange columnRange : columnRanges) {
      List<Object> points = columnRange.getAccessPoints();
      Comparable lowerBound, upperBound;
      Range rg = columnRange.getRange();
      if (points.size() > 0) {
        lowerBound = Comparables.wrap(points.get(0));
        upperBound = Comparables.wrap(lowerBound);
      } else {
        lowerBound = Comparables.wrap(rg.lowerEndpoint());
        upperBound = Comparables.wrap(rg.upperEndpoint());
      }
      if (lowerBound == null && upperBound.compareTo(DataType.indexMaxValue()) == 0) {
        rowCount += tableRowCount;
      } else if (lowerBound != null && lowerBound.compareTo(DataType.indexMinValue()) == 0) {
        double nullCount = tableRowCount / pseudoEqualRate;
        if (upperBound.compareTo(DataType.indexMaxValue()) == 0) {
          rowCount += tableRowCount - nullCount;
        } else {
          double lessCount = tableRowCount / pseudoLessRate;
          rowCount += lessCount - nullCount;
        }
      } else if (upperBound.compareTo(DataType.indexMaxValue()) == 0) {
        rowCount += tableRowCount / pseudoLessRate;
      } else {
        if (lowerBound.compareTo(upperBound) == 0) {
          rowCount += tableRowCount / pseudoEqualRate;
        } else {
          rowCount += tableRowCount / pseudoBetweenRate;
        }
      }
    }
    if (rowCount > tableRowCount) {
      rowCount = tableRowCount;
    }
    return rowCount;
  }
}
