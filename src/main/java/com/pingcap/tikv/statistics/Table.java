package com.pingcap.tikv.statistics;

import com.google.common.collect.Range;
import com.pingcap.tidb.tipb.ColumnInfo;
import com.pingcap.tikv.expression.TiBinaryFunctionExpresson;
import com.pingcap.tikv.expression.TiColumnRef;
import com.pingcap.tikv.expression.TiConstant;
import com.pingcap.tikv.expression.TiExpr;
import com.pingcap.tikv.meta.TiIndexInfo;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.predicates.PredicateUtils;
import com.pingcap.tikv.predicates.RangeBuilder;
import com.pingcap.tikv.predicates.RangeBuilder.IndexRange;
import com.pingcap.tikv.predicates.ScanBuilder;
import com.pingcap.tikv.predicates.ScanBuilder.IndexMatchingResult;
import com.pingcap.tikv.types.DataType;
import com.pingcap.tikv.util.Comparables;
import com.pingcap.tikv.util.DBReader;

import java.util.*;

/**
 * Created by birdstorm on 2017/8/16.
 *
 */
public class Table {
  private static final long pseudoRowCount = 10000;
  private static final long pseudoEqualRate = 1000;
  private static final long pseudoLessRate = 3;
  private static final long pseudoBetweenRate = 40;
  // If one condition can't be calculated, we will assume that the selectivity of this condition is 0.8.
  private static final double selectionFactor = 0.8;

  private static final int indexType = 0;
  private static final int pkType = 1;
  private static final int colType = 2;

  private long TableID;
  private HashMap<Long, ColumnWithHistogram> Columns;
  private HashMap<Long, IndexWithHistogram> Indices;
  private long Count; // Total row count in a table.
  private long ModifyCount; // Total modify count in a table.
  private long Version;
  private boolean Pseudo;

  public Table() {
  }

  public Table(long tableID, int defaultSizeColumns, int defaultSizeIndices) {
    this.TableID = tableID;
    this.Columns = new HashMap<>(defaultSizeColumns);
    this.Indices = new HashMap<>(defaultSizeIndices);
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

  public long getTableID() {
    return TableID;
  }

  private void setTableID(long id) { this.TableID = id; }

  public long getCount() {
    return Count;
  }

  public void setCount(long cnt) { this.Count = cnt; }

  public long getModifyCount() {
    return ModifyCount;
  }

  void setModifyCount(long modifyCount) { this.ModifyCount = modifyCount; }

  public long getVersion() {
    return Version;
  }

  public void setVersion(long ver) { this.Version = ver; }

  public boolean getPseudo() {
    return Pseudo;
  }

  public HashMap<Long, ColumnWithHistogram> getColumns() {
    return Columns;
  }

  public HashMap<Long, IndexWithHistogram> getIndices() {
    return Indices;
  }

  void putIndices(long key, IndexWithHistogram value) {
    this.Indices.put(key, value);
  }

  void putColumns(long key, ColumnWithHistogram value) {
    this.Columns.put(key, value);
  }

  /** ColumnIsInvalid checks if this column is invalid. */
  private boolean ColumnIsInvalid(ColumnInfo info) {
    if (Pseudo) {
      return true;
    }
    ColumnWithHistogram column = Columns.get(info.getColumnId());
    return column.getHistogram() == null || column.getHistogram().getBuckets().isEmpty();
  }

  /** ColumnGreaterRowCount estimates the row count where the column greater than value. */
  public double ColumnGreaterRowCount(Comparable value, ColumnInfo columnInfo) {
    if (ColumnIsInvalid(columnInfo)) {
      return (double) (Count) / pseudoLessRate;
    }
    Histogram hist = Columns.get(columnInfo.getColumnId()).getHistogram();
    double result = hist.greaterRowCount(value);
    result *= hist.getIncreaseFactor(Count);
    return result;
  }

  /** ColumnLessRowCount estimates the row count where the column less than value. */
  public double ColumnLessRowCount(Comparable value, ColumnInfo columnInfo) {
    if (ColumnIsInvalid(columnInfo)) {
      return (double) (Count) / pseudoLessRate;
    }
    Histogram hist = Columns.get(columnInfo.getColumnId()).getHistogram();
    double result = hist.lessRowCount(value);
    result *= hist.getIncreaseFactor(Count);
    return result;
  }

  /** ColumnBetweenRowCount estimates the row count where column greater or equal to a and less than b. */
  public double ColumnBetweenRowCount(Comparable a, Comparable b, ColumnInfo columnInfo) {
    if (ColumnIsInvalid(columnInfo)) {
      return (double) (Count) / pseudoBetweenRate;
    }
    Histogram hist = Columns.get(columnInfo.getColumnId()).getHistogram();
    double result = hist.betweenRowCount(a, b);
    result *= hist.getIncreaseFactor(Count);
    return result;
  }

  /** ColumnEqualRowCount estimates the row count where the column equals to value. */
  public double ColumnEqualRowCount(Comparable value, ColumnInfo columnInfo) {
    if (ColumnIsInvalid(columnInfo)) {
      return (double) (Count) / pseudoEqualRate;
    }
    Histogram hist = Columns.get(columnInfo.getColumnId()).getHistogram();
    double result = hist.equalRowCount(value);
    result *= hist.getIncreaseFactor(Count);
    return result;
  }

  /** GetRowCountByColumnRanges estimates the row count by a slice of ColumnRange. */
  private double GetRowCountByColumnRanges(long columnID, List<IndexRange> columnRanges) {
    ColumnWithHistogram c = Columns.get(columnID);
    Histogram hist = c.getHistogram();
    if (Pseudo || hist == null || hist.getBuckets().isEmpty()) {
      return getPseudoRowCountByColumnRanges(columnRanges, Count);
    }
    return c.getColumnRowCount(columnRanges);
  }

  /** GetRowCountByColumnRanges estimates the row count by a slice of ColumnRange. */
  private double GetRowCountByIndexRanges(long indexID, List<IndexRange> indexRanges) {
    IndexWithHistogram i = Indices.get(indexID);
    Histogram hist = i.getHistogram();
    if (Pseudo || hist == null || hist.getBuckets().isEmpty()) {
      return getPseudoRowCountByIndexRanges(indexRanges, Count);
    }
    return i.getRowCount(indexRanges, getTableID());
  }

  static Table PseudoTable(long tableID) {
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

  private class exprSet {
    int tp;
    long ID;
    BitSet mask;
    List<IndexRange> ranges;

    private exprSet(int _tp, long _ID, BitSet _mask, List<IndexRange> _ranges) {
      this.tp = _tp;
      this.ID = _ID;
      this.mask = (BitSet) _mask.clone();
      this.ranges = _ranges;
    }
  }

  public static boolean checkColumnConstant(List<TiExpr> exprs) {
    if(exprs.size() != 2) {
      return false;
    }
    boolean ok1 = exprs.get(0) instanceof TiColumnRef;
    boolean ok2 = exprs.get(1) instanceof TiConstant;
    if(ok1 && ok2) return true;
    ok1 = exprs.get(1) instanceof TiColumnRef;
    ok2 = exprs.get(0) instanceof TiConstant;
    return ok1 && ok2;
  }

  private double pseudoSelectivity(List<TiExpr> exprs) {
    double minFactor = selectionFactor;
    for(TiExpr expr: exprs) {
      if(expr instanceof TiBinaryFunctionExpresson && checkColumnConstant(((TiBinaryFunctionExpresson) expr).getArgs())) {
        switch (((TiBinaryFunctionExpresson) expr).getName()) {
          case "=":
          case "NullEqual":
            minFactor = Math.min(minFactor, 1.0/pseudoEqualRate);
            break;
          case ">=":
          case ">":
          case "<=":
          case "<":
            minFactor = Math.min(minFactor, 1.0/pseudoLessRate);
            break;
          default:
          // FIXME: To resolve the between case.
        }
      }
    }
    return minFactor;
  }

  public double Selectivity(DBReader dbReader, List<TiExpr> exprs) {
    if(Count == 0) {
      return 1.0;
    }
    if(Pseudo || Columns.size() == 0 && Indices.size() == 0) {
      return pseudoSelectivity(exprs);
    }
    if(exprs.size() == 0) {
      return 1.0;
    }
    int len = exprs.size();
    TiTableInfo table = dbReader.getTableInfo(getTableID());
    ArrayList<exprSet> sets = new ArrayList<>();
    Set<TiColumnRef> extractedCols = PredicateUtils.extractColumnRefFromExpr(PredicateUtils.mergeCNFExpressions(exprs));
    for(ColumnWithHistogram colInfo: Columns.values()) {
      TiColumnRef col = TiColumnRef.colInfo2Col(extractedCols, colInfo.getColumnInfo());
      // This column should have histogram.
      if(col != null && !colInfo.getHistogram().getBuckets().isEmpty()) {
        BitSet maskCovered = new BitSet(len);
        List<IndexRange> ranges = new ArrayList<>();
        maskCovered.clear();
        ranges = getMaskAndRanges(exprs, col, maskCovered, ranges, table);
        exprSet tmp = new exprSet(colType, col.getColumnInfo().getId(), maskCovered, ranges);
        if(colInfo.getColumnInfo().isPrimaryKey()) {
          tmp.tp = pkType;
        }
        sets.add(tmp);
      }
    }
    for(IndexWithHistogram idxInfo: Indices.values()) {
      List<TiColumnRef> idxCols = TiColumnRef.indexInfo2Cols(extractedCols, idxInfo.getIndexInfo());
      // This index should have histogram.
      if(idxCols.size() > 0 && !idxInfo.getHistogram().getBuckets().isEmpty()) {
        BitSet maskCovered = new BitSet(len);
        List<IndexRange> ranges = new ArrayList<>();
        ranges = getMaskAndRanges(exprs, idxCols, maskCovered, ranges, table, idxInfo.getIndexInfo());
        exprSet tmp = new exprSet(indexType, idxInfo.getIndexInfo().getId(), maskCovered, ranges);
        sets.add(tmp);
      }
    }

    sets = getUsableSetsByGreedy(sets, len);
    double ret = 1.0;
    BitSet mask = new BitSet(len);
    mask.clear();
    mask.flip(0, len);
    for(exprSet set: sets) {
      mask.xor(set.mask);
      double rowCount = 1.0;
      switch(set.tp) {
        case pkType:
        case colType:
          rowCount = GetRowCountByColumnRanges(set.ID, set.ranges);
          break;
        case indexType:
          rowCount = GetRowCountByIndexRanges(set.ID, set.ranges);
          break;
        default:
      }
      System.out.println("Rowcount=" + rowCount + " getCount()=" + getCount());
      ret *= rowCount / getCount();
    }
    if(mask.cardinality() > 0) {
      ret *= selectionFactor;
    }
    return ret;
  }

  private List<IndexRange> getMaskAndRanges(List<TiExpr> exprs, TiColumnRef columnRef, BitSet mask, List<IndexRange> ranges,
                                            TiTableInfo table) {
    List<TiExpr> exprsClone = new ArrayList<>();
    exprsClone.addAll(exprs);
    IndexMatchingResult result = ScanBuilder.extractConditions(exprsClone, table, table.getIndices().get(0));
    List<TiExpr> accessConditions = result.getAccessConditions();
    int i = 0;
    for(TiExpr x: exprsClone) {
      for(TiExpr y: accessConditions) {
        if(x.equals(y)) {
          mask.set(i);
        }
      }
      i ++;
    }
    return RangeBuilder.appendRanges(ranges, RangeBuilder.exprToRanges(accessConditions, columnRef.getType()), columnRef.getType());
  }

  private List<IndexRange> getMaskAndRanges(List<TiExpr> exprs, List<TiColumnRef> indexColumnRef, BitSet mask, List<IndexRange> ranges,
                                TiTableInfo table, TiIndexInfo index) {
    List<TiExpr> exprsClone = new ArrayList<>();
    exprsClone.addAll(exprs);
    IndexMatchingResult result = ScanBuilder.extractConditions(exprsClone, table, index);
    List<TiExpr> accessConditions = result.getAccessConditions();
    int i = 0;
    for(TiExpr x: exprsClone) {
      for(TiExpr y: accessConditions) {
        if(x.equals(y)) {
          mask.set(i);
        }
      }
      i ++;
    }
    return RangeBuilder.appendRanges(ranges, RangeBuilder.exprToRanges(accessConditions,
        indexColumnRef.get(0).getType()), indexColumnRef.get(0).getType());
  }

  private ArrayList<exprSet> getUsableSetsByGreedy(ArrayList<exprSet> sets, int len) {
    ArrayList<exprSet> newBlocks = new ArrayList<>();
    BitSet mask = new BitSet(len);
    mask.clear();
    mask.flip(0, len);
    BitSet st;
    while (true) {
      int bestID = -1;
      int bestCount = 0;
      int bestTp = colType;
      int count = 0;
      for(exprSet set: sets) {
        st = (BitSet) set.mask.clone();
        st.and(mask);
        int bits = st.cardinality();
        if(bestTp == colType && set.tp < colType || bestCount < bits) {
          bestID = count;
          bestCount = bits;
          bestTp = set.tp;
        }
        count ++;
      }
      if(bestCount == 0) {
        break;
      } else {
        st = (BitSet) mask.clone();
        st.xor(sets.get(bestID).mask);
        mask.and(st);
        newBlocks.add(sets.get(bestID));
        sets.remove(bestID);
      }
    }
    return newBlocks;
  }

}
