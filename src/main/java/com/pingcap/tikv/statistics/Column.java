package com.pingcap.tikv.statistics;

import com.google.common.collect.Range;
import com.pingcap.tikv.meta.TiColumnInfo;
import com.pingcap.tikv.predicates.RangeBuilder.IndexRange;
import com.pingcap.tikv.util.Comparables;

import java.util.List;
import java.util.Objects;

/**
 * Created by birdstorm on 2017/8/14.
 * may be deleted according to TiDB's implementation
 */
public class Column {
  private Histogram hg;
  private TiColumnInfo info;

  // getColumnRowCount estimates the row count by a slice of ColumnRange.
  public double getColumnRowCount(List<IndexRange> columnRanges) {
    double rowCount = 0.0;
    for (IndexRange range : columnRanges) {
      double cnt;
      List<Object> points = range.getAccessPoints();
      if (points.size() > 0) {
        if (points.size() != 1) {
          System.out.println("Warning: ColumnRowCount should only contain one attribute.");
        }
        cnt = hg.equalRowCount(Comparables.wrap(points.get(0)));
      } else {
        Range rg = range.getRange();
        Objects.requireNonNull(rg.lowerEndpoint(), "LowerBound must not be null");
        Objects.requireNonNull(rg.upperEndpoint(), "UpperBound must not be null");

        cnt = hg.betweenRowCount(rg.lowerEndpoint(), rg.upperEndpoint());
        if (!rg.hasLowerBound()) {
          double lowCnt = hg.equalRowCount(rg.lowerEndpoint());
          cnt -= lowCnt;
        }
        if (rg.hasUpperBound()) {
          double highCnt = hg.equalRowCount(rg.upperEndpoint());
          cnt += highCnt;
        }
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

}
