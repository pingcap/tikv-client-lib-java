package com.pingcap.tikv.statistics;

import com.google.protobuf.ByteString;
import com.pingcap.tikv.kvproto.Coprocessor.KeyRange;
import com.pingcap.tikv.meta.TiIndexInfo;
import com.pingcap.tikv.predicates.RangeBuilder.IndexRange;
import com.pingcap.tikv.predicates.ScanBuilder;
import com.pingcap.tikv.util.Comparables;

import java.util.List;

/**
 * Created by birdstorm on 2017/8/14.
 *
 */
public class IndexWithHistogram {
  private Histogram hg;
  private TiIndexInfo info;

  public IndexWithHistogram(Histogram hist, TiIndexInfo indexInfo) {
    this.hg = hist;
    this.info = indexInfo;
  }

  protected long getLastUpdateVersion() {
    return hg.getLastUpdateVersion();
  }

  protected double getRowCount(List<IndexRange> IndexRanges, long tableID) {
    double totalCount = 0;
    List<KeyRange> KeyRanges = ScanBuilder.buildIndexScanKeyRange(tableID, info, IndexRanges);
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
