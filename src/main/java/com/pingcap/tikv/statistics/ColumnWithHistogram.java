package com.pingcap.tikv.statistics;

import com.google.common.collect.Range;
import com.google.protobuf.ByteString;
import com.pingcap.tikv.expression.TiColumnRef;
import com.pingcap.tikv.meta.TiColumnInfo;
import com.pingcap.tikv.predicates.RangeBuilder.IndexRange;
import com.pingcap.tikv.types.DataType;
import com.pingcap.tikv.util.Comparables;

import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Created by birdstorm on 2017/8/14.
 * may be deleted according to TiDB's implementation
 */
public class ColumnWithHistogram {
  private Histogram hg;
  private TiColumnInfo info;

  public ColumnWithHistogram(Histogram hist, TiColumnInfo colInfo) {
    this.hg = hist;
    this.info = colInfo;
  }

  long getLastUpdateVersion() {
    return hg.getLastUpdateVersion();
  }

  /** getColumnRowCount estimates the row count by a slice of ColumnRange. */
  double getColumnRowCount(List<IndexRange> columnRanges) {
    double rowCount = 0.0;
    for (IndexRange range : columnRanges) {
      double cnt = 0.0;
      List<Object> points = range.getAccessPoints();
      if (!points.isEmpty()) {
        if (points.size() > 1) {
          System.out.println("Warning: ColumnRowCount should only contain one attribute.");
        }
        cnt = hg.equalRowCount(Comparables.wrap(points.get(0)));
        assert range.getRange() == null;
      } else if (range.getRange() != null){
        Range rg = range.getRange();
        Comparable lowerBound = rg.hasLowerBound() ?
            Comparables.wrap(ByteString.copyFrom(rg.lowerEndpoint().toString().getBytes())):
            Comparables.wrap(DataType.indexMinValue());
        Comparable upperBound = rg.hasUpperBound() ?
            Comparables.wrap(ByteString.copyFrom(rg.upperEndpoint().toString().getBytes())):
            Comparables.wrap(DataType.indexMaxValue());
        Objects.requireNonNull(lowerBound, "LowerBound must not be null");
        Objects.requireNonNull(upperBound, "UpperBound must not be null");

        cnt += hg.betweenRowCount(lowerBound, upperBound);

      }
      rowCount += cnt;
    }
    if (rowCount > hg.totalRowCount()) {
      rowCount = hg.totalRowCount();
    } else if (rowCount < 0) {
      rowCount = 0;
    }
    return rowCount;
  }

  public Histogram getHistogram() {
    return hg;
  }

  public TiColumnInfo getColumnInfo() {
    return info;
  }

  public static TiColumnRef indexInfo2Col(Set<TiColumnRef> cols, TiColumnInfo col) {
    return null;
  }

}
