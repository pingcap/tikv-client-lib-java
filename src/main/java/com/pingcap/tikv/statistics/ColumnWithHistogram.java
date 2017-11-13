package com.pingcap.tikv.statistics;

import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import com.google.protobuf.ByteString;
import com.pingcap.tikv.codec.CodecDataOutput;
import com.pingcap.tikv.codec.KeyUtils;
import com.pingcap.tikv.expression.TiColumnRef;
import com.pingcap.tikv.meta.TiColumnInfo;
import com.pingcap.tikv.meta.TiKey;
import com.pingcap.tikv.predicates.RangeBuilder.IndexRange;
import com.pingcap.tikv.types.DataType;
import com.pingcap.tikv.types.DataTypeFactory;

import java.util.List;
import java.util.Set;

import static com.pingcap.tikv.types.Types.TYPE_LONG;

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
        cnt = hg.equalRowCount(TiKey.create(points.get(0)));
        assert range.getRange() == null;
      } else if (range.getRange() != null){
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
        Object lower = TiKey.unwrap(!lNull ? rg.lowerEndpoint() : DataType.encodeIndexMinValue());
        Object upper = TiKey.unwrap(!rNull ? rg.upperEndpoint() : DataType.encodeIndexMaxValue());
//        System.out.println("=>Column " + l + (!lNull ? TiKey.create(lower) : "-∞")
//            + "," + (!rNull ? TiKey.create(upper) : "∞") + r);
        t = DataTypeFactory.of(TYPE_LONG);
        if(lNull) {
          t.encodeMinValue(cdo);
        } else {
          if(lower instanceof Number) {
            t.encode(cdo, DataType.EncodeType.KEY, lower);
          } else if(lower instanceof byte[]) {
            cdo.write(((byte[]) lower));
          } else {
            cdo.write(((ByteString) lower).toByteArray());
          }
          if(lOpen) {
            cdo.writeByte(0);
          }
        }
        if(!lNull && lOpen) {
          lowerBound = TiKey.create(KeyUtils.prefixNext(cdo.toBytes()));
        } else {
          lowerBound = TiKey.create(cdo.toBytes());
        }

        cdo.reset();
        if(rNull) {
          t.encodeMaxValue(cdo);
        } else {
          if(upper instanceof Number) {
            t.encode(cdo, DataType.EncodeType.KEY, upper);
          } else if(upper instanceof byte[]) {
            cdo.write(((byte[]) upper));
          } else {
            cdo.write(((ByteString) upper).toByteArray());
          }
        }
        if(!rNull && !rOpen) {
          upperBound = TiKey.create(KeyUtils.prefixNext(cdo.toBytes()));
        } else {
          upperBound = TiKey.create(cdo.toBytes());
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

  public TiColumnInfo getColumnInfo() {
    return info;
  }

  public static TiColumnRef indexInfo2Col(Set<TiColumnRef> cols, TiColumnInfo col) {
    return null;
  }

}
