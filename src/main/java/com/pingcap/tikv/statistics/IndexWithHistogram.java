package com.pingcap.tikv.statistics;

import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import com.google.protobuf.ByteString;
import com.pingcap.tikv.codec.CodecDataOutput;
import com.pingcap.tikv.codec.KeyUtils;
import com.pingcap.tikv.meta.TiIndexInfo;
import com.pingcap.tikv.meta.TiKey;
import com.pingcap.tikv.predicates.RangeBuilder.IndexRange;
import com.pingcap.tikv.types.DataType;
import com.pingcap.tikv.types.DataTypeFactory;

import java.util.List;

import static com.pingcap.tikv.types.Types.TYPE_LONG;

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

  long getLastUpdateVersion() {
    return hg.getLastUpdateVersion();
  }

  double getRowCount(List<IndexRange> IndexRanges, long tableID) {
    double rowCount = 0.0;
    for (IndexRange range : IndexRanges) {
      double cnt = 0.0;
      List<Object> points = range.getAccessPoints();
      if (!points.isEmpty()) {
        cnt += hg.equalRowCount(TiKey.create(points.get(points.size() - 1)));
      }
      if (range.getRange() != null){
        Range rg = range.getRange();
        TiKey lowerBound, upperBound;
        DataType t;
        boolean lNull = !rg.hasLowerBound();
        boolean rNull = !rg.hasUpperBound();
        boolean lOpen = lNull || rg.lowerBoundType().equals(BoundType.OPEN);
        boolean rOpen = rNull || rg.upperBoundType().equals(BoundType.OPEN);
        String l = lOpen ? "(" : "[";
        String r = rOpen ? ")" : "]";
        CodecDataOutput cdo = new CodecDataOutput();
        Object lower = TiKey.unwrap(!lNull ? rg.lowerEndpoint() : DataType.indexMinValue());
        Object upper = TiKey.unwrap(!rNull ? rg.upperEndpoint() : DataType.indexMaxValue());
//        System.out.println("=>Index " + l + (!lNull ? TiKey.create(lower) : "-∞")
//            + "," + (!rNull ? TiKey.create(upper) : "∞") + r);
        t = DataTypeFactory.of(TYPE_LONG);
        if(lNull) {
          t.encodeMinValue(cdo);
        } else {
          if(lower instanceof Number) {
            t.encode(cdo, DataType.EncodeType.KEY, lower);
          } else {
            cdo.write(((ByteString) lower).toByteArray());
          }
        }
        if(!lNull && lOpen) {
          lowerBound = TiKey.create(ByteString.copyFrom(KeyUtils.prefixNext(cdo.toBytes())));
        } else {
          lowerBound = TiKey.create(cdo.toByteString());
        }

        cdo.reset();
        if(rNull) {
          t.encodeMaxValue(cdo);
        } else {
          if(upper instanceof Number) {
            t.encode(cdo, DataType.EncodeType.KEY, upper);
          } else {
            cdo.write(((ByteString) upper).toByteArray());
          }
        }
        if(!rNull && !rOpen) {
          upperBound = TiKey.create(ByteString.copyFrom(KeyUtils.prefixNext(cdo.toBytes())));
        } else {
          upperBound = TiKey.create(cdo.toByteString());
        }

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

  public TiIndexInfo getIndexInfo() {
    return info;
  }
}
