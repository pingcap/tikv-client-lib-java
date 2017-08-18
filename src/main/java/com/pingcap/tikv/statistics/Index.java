package com.pingcap.tikv.statistics;

import com.google.protobuf.ByteString;
import com.pingcap.tikv.kvproto.Coprocessor.KeyRange;
import com.pingcap.tikv.meta.TiIndexInfo;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.predicates.RangeBuilder.IndexRange;
import com.pingcap.tikv.predicates.ScanBuilder;
import com.pingcap.tikv.util.Comparables;

import java.util.List;

/**
 * Created by birdstorm on 2017/8/14.
 */
public class Index {
  private Histogram hg;
  private TiIndexInfo info;

  public double getRowCount(List<IndexRange> IndexRanges, TiTableInfo tableInfo) {
    double totalCount = 0;
    List<KeyRange> KeyRanges = ScanBuilder.buildIndexScanKeyRange(tableInfo, info, IndexRanges);
    for (KeyRange range : KeyRanges) {
      ByteString lowerBound = range.getStart();
      ByteString upperBound = range.getEnd();
      double cnt = hg.betweenRowCount(Comparables.wrap(lowerBound), Comparables.wrap(upperBound));
      totalCount += cnt;
    }
    if (totalCount > hg.totalRowCount()) {
      totalCount = hg.totalRowCount();
    }
    return totalCount;
  }

  public Histogram getHistogram() {
    return hg;
  }

  public TiIndexInfo getIndexInfo() {
    return info;
  }
}
