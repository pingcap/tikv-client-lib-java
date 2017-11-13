package com.pingcap.tikv.statistics;

import com.google.common.collect.Range;
import com.pingcap.tidb.tipb.ColumnInfo;
import com.pingcap.tikv.expression.TiBinaryFunctionExpression;
import com.pingcap.tikv.expression.TiColumnRef;
import com.pingcap.tikv.expression.TiConstant;
import com.pingcap.tikv.expression.TiExpr;
import com.pingcap.tikv.expression.scalar.*;
import com.pingcap.tikv.meta.TiIndexInfo;
import com.pingcap.tikv.meta.TiKey;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.predicates.PredicateUtils;
import com.pingcap.tikv.predicates.RangeBuilder;
import com.pingcap.tikv.predicates.RangeBuilder.IndexRange;
import com.pingcap.tikv.predicates.ScanBuilder;
import com.pingcap.tikv.predicates.ScanBuilder.IndexMatchingResult;
import com.pingcap.tikv.types.DataType;
import com.pingcap.tikv.util.DBReader;

import java.util.*;

/**
 * Created by birdstorm on 2017/8/16.
 *
 */
public class Table {
  private static final long PSEUDO_ROW_COUNT = 10000;
  private static final double EQUAL_RATE = 1.0 / 1000.0;
  private static final double LESS_RATE = 1.0 / 3.0;
  private static final double PSEUDO_BETWEEN_RATE = 1.0 / 40;
  // If one condition can't be calculated, we will assume that the selectivity of this condition is 0.8.
  private static final double SELECTION_FACTOR = 0.8;

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
  public double ColumnGreaterRowCount(TiKey value, ColumnInfo columnInfo) {
    if (ColumnIsInvalid(columnInfo)) {
      return (double) (Count) * LESS_RATE;
    }
    Histogram hist = Columns.get(columnInfo.getColumnId()).getHistogram();
    double result = hist.greaterRowCount(value);
    result *= hist.getIncreaseFactor(Count);
    return result;
  }

  /** ColumnLessRowCount estimates the row count where the column less than value. */
  public double ColumnLessRowCount(TiKey value, ColumnInfo columnInfo) {
    if (ColumnIsInvalid(columnInfo)) {
      return (double) (Count) * LESS_RATE;
    }
    Histogram hist = Columns.get(columnInfo.getColumnId()).getHistogram();
    double result = hist.lessRowCount(value);
    result *= hist.getIncreaseFactor(Count);
    return result;
  }

  /** ColumnBetweenRowCount estimates the row count where column greater or equal to a and less than b. */
  public double ColumnBetweenRowCount(TiKey a, TiKey b, ColumnInfo columnInfo) {
    if (ColumnIsInvalid(columnInfo)) {
      return (double) (Count) * PSEUDO_BETWEEN_RATE;
    }
    Histogram hist = Columns.get(columnInfo.getColumnId()).getHistogram();
    double result = hist.betweenRowCount(a, b);
    result *= hist.getIncreaseFactor(Count);
    return result;
  }

  /** ColumnEqualRowCount estimates the row count where the column equals to value. */
  public double ColumnEqualRowCount(TiKey value, ColumnInfo columnInfo) {
    if (ColumnIsInvalid(columnInfo)) {
      return (double) (Count) * EQUAL_RATE;
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
    double ret = i.getRowCount(indexRanges, getTableID());
    ret *= hist.getIncreaseFactor(Count);
    return ret;
  }

  static Table PseudoTable(long tableID) {
    return new Table(tableID, PSEUDO_ROW_COUNT, true);
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
      TiKey<byte[]> lowerBound, upperBound;
      Range rg = columnRange.getRange();
      if (points.size() > 0) {
        lowerBound = TiKey.encode(points.get(0));
        upperBound = TiKey.encode(points.get(0));
      } else {
        lowerBound = TiKey.encode(rg.hasLowerBound()? rg.lowerEndpoint(): DataType.encodeIndexMinValue());
        upperBound = TiKey.encode(rg.hasUpperBound()? rg.upperEndpoint(): DataType.encodeIndexMaxValue());
      }
      if (!rg.hasLowerBound() && upperBound.compareTo(TiKey.encode(DataType.encodeIndexMaxValue())) == 0) {
        rowCount += tableRowCount;
      } else if (rg.hasLowerBound() && lowerBound.compareTo(TiKey.encode(DataType.encodeIndexMinValue())) == 0) {
        double nullCount = tableRowCount * EQUAL_RATE;
        if (upperBound.compareTo(TiKey.encode(DataType.encodeIndexMaxValue())) == 0) {
          rowCount += tableRowCount - nullCount;
        } else {
          double lessCount = tableRowCount * LESS_RATE;
          rowCount += lessCount - nullCount;
        }
      } else if (upperBound.compareTo(TiKey.encode(DataType.encodeIndexMaxValue())) == 0) {
        rowCount += tableRowCount * LESS_RATE;
      } else {
        if (lowerBound.compareTo(upperBound) == 0) {
          rowCount += tableRowCount * EQUAL_RATE;
        } else {
          rowCount += tableRowCount * PSEUDO_BETWEEN_RATE;
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

    private String retrieve(int x) {
      switch(x) {
        case 0: return "index";
        case 1: return "pk";
        case 2: return "column";
        default: return "";
      }
    }

    @Override
    public String toString() {
      String ans = retrieve(tp) + "#" + String.valueOf(ID) + "_" + mask + "_";
      for(IndexRange ir: ranges) {
        ans = ans.concat("," + ir);
      }
      return ans;
    }
  }

  private class exprSetCompare implements Comparator<exprSet> {
    public int compare(exprSet a, exprSet b) {
      return (int) (b.ID - a.ID);
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
    double minFactor = SELECTION_FACTOR;
    for(TiExpr expr: exprs) {
      if(expr instanceof TiBinaryFunctionExpression && checkColumnConstant(((TiBinaryFunctionExpression) expr).getArgs())) {
        if(expr instanceof Equal || expr instanceof NullEqual) {
          minFactor *= EQUAL_RATE;;
        } else if(
            expr instanceof GreaterEqual ||
            expr instanceof GreaterThan ||
            expr instanceof LessEqual ||
            expr instanceof LessThan) {
          minFactor *= LESS_RATE;
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
    List<exprSet> sets = new ArrayList<>();
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
    sets.sort(new exprSetCompare());
//    System.out.println(">>>" + sets + "<<<");
    sets = getUsableSetsByGreedy(sets, len);
//    System.out.println("<<<" + sets + ">>>");
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
//      System.out.println("Rowcount=" + rowCount + " getCount()=" + getCount());
      ret *= rowCount / getCount();
    }
    if(mask.cardinality() > 0) {
      ret *= SELECTION_FACTOR;
    }
    return ret;
  }

  private List<IndexRange> getMaskAndRanges(List<TiExpr> exprs, TiColumnRef columnRef, BitSet mask, List<IndexRange> ranges,
                                            TiTableInfo table) {
    List<TiExpr> exprsClone = new ArrayList<>();
    exprsClone.addAll(exprs);
    IndexMatchingResult result = ScanBuilder.extractConditions(exprsClone, table, columnRef.getColumnInfo());
    List<TiExpr> accessConditions = result.getAccessConditions();
    List<TiExpr> accessPoints = result.getAccessPoints();
    int i = 0;
    for(TiExpr x: exprsClone) {
      if(accessConditions.contains(x) || accessPoints.contains(x)) {
        mask.set(i);
      }
      i ++;
    }
    ranges = RangeBuilder.exprsToIndexRanges(accessPoints,
        result.getAccessPointTypes(), result.getAccessConditions(), result.getRangeType());
    return ranges;
  }

  private List<IndexRange> getMaskAndRanges(List<TiExpr> exprs, List<TiColumnRef> indexColumnRef, BitSet mask, List<IndexRange> ranges,
                                TiTableInfo table, TiIndexInfo index) {
    List<TiExpr> exprsClone = new ArrayList<>();
    exprsClone.addAll(exprs);
    IndexMatchingResult result = ScanBuilder.extractConditions(exprsClone, table, index);
    List<TiExpr> accessConditions = result.getAccessConditions();
    List<TiExpr> accessPoints = result.getAccessPoints();
    int i = 0;
    for(TiExpr x: exprsClone) {
      if(accessConditions.contains(x) || accessPoints.contains(x)) {
        mask.set(i);
      }
      i ++;
    }
    ranges = RangeBuilder.exprsToIndexRanges(accessPoints,
        result.getAccessPointTypes(), result.getAccessConditions(), result.getRangeType());
    return ranges;
  }

  private List<exprSet> getUsableSetsByGreedy(List<exprSet> sets, int len) {
    List<exprSet> newBlocks = new ArrayList<>();
    BitSet mask = new BitSet(len);
    mask.clear();
    mask.flip(0, len);
    BitSet st;
    while (true) {
      int bestID = -1;
      int bestCount = 0;
      int bestTp = colType;
      int id = 0;
      for(exprSet set: sets) {
        st = (BitSet) set.mask.clone();
        st.and(mask);
        int bits = st.cardinality();
        if((bestTp == colType && set.tp < colType && bestCount <= bits) || bestCount < bits) {
          bestID = id;
          bestCount = bits;
          bestTp = set.tp;
        }
        id ++;
      }
      if(bestCount == 0) {
        break;
      } else {
        st = (BitSet) mask.clone();
        st.xor(sets.get(bestID).mask);
        mask.and(st);
        newBlocks.add(sets.get(bestID));
        //sets should have at least one object
        for(int i = bestID; i < sets.size() - 1; i ++) {
          sets.set(i, sets.get(i + 1));
        }
        sets.remove(sets.size() - 1);
      }
    }
    return newBlocks;
  }

}
