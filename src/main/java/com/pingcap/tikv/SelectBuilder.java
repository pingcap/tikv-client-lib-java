package com.pingcap.tikv;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkArgument;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import com.google.protobuf.ByteString;
import com.pingcap.tidb.tipb.SelectRequest;
import com.pingcap.tikv.codec.TableCodec;
import com.pingcap.tikv.exception.TiClientInternalException;
import com.pingcap.tikv.grpc.Kvrpcpb.KvPair;
import com.pingcap.tikv.grpc.Metapb.Region;
import com.pingcap.tikv.grpc.Metapb.Store;
import com.pingcap.tikv.meta.Row;
import com.pingcap.tikv.exception.SelectException;
import com.pingcap.tikv.meta.TiRange;
// TODO: UNDO this import later
import com.pingcap.tikv.meta.*;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.operation.ScanIterator;
import com.pingcap.tikv.operation.SelectIterator;
import com.pingcap.tikv.util.Pair;
import com.pingcap.tikv.SelectBuilder;
import com.pingcap.tidb.tipb.Expr;
import com.pingcap.tidb.tipb.ByItem;
import com.pingcap.tidb.tipb.ExprType;

import java.nio.ByteBuffer;
import java.util.*;

public class SelectBuilder {
  private static long MASK_IGNORE_TRUNCATE = 0x1;
  private static long MASK_TRUNC_AS_WARNING = 0x2;

  private final Snapshot snapshot;
  private final SelectRequest.Builder builder;
  private final ImmutableList.Builder<TiRange<Long>> rangeListBuilder;
  private TiTableInfo table;
  private long timestamp;
  private long timeZoneOffset;
  private boolean distinct;

  private TiSession getSession() {
    return snapshot.getSession();
  }

  private TiConfiguration getConf() {
    return getSession().getConf();
  }

  public static SelectBuilder newBuilder(Snapshot snapshot, TiTableInfo table) {
    return new SelectBuilder(snapshot, table);
  }

  private SelectBuilder(Snapshot snapshot, TiTableInfo table) {
    this.snapshot = snapshot;
    this.builder = SelectRequest.newBuilder();
    this.rangeListBuilder = ImmutableList.builder();
    this.table = table;

    long flags = 0;
    if (getConf().isIgnoreTruncate()) {
      flags |= MASK_IGNORE_TRUNCATE;
    } else if (getConf().isTruncateAsWarning()) {
      flags |= MASK_TRUNC_AS_WARNING;
    }
    builder.setFlags(flags);
    builder.setStartTs(snapshot.getVersion());
    // Set default timezone offset
    TimeZone tz = TimeZone.getDefault();
    builder.setTimeZoneOffset(tz.getOffset(new Date().getTime()) / 1000);
    builder.setTableInfo(table.toProto());
  }

  public SelectBuilder setTimeZoneOffset(long offset) {
    builder.setTimeZoneOffset(offset);
    return this;
  }

  public SelectBuilder setTimestamp(long timestamp) {
    builder.setStartTs(timestamp);
    return this;
  }

  private boolean isExprTypeSupported(ExprType exprType) {
    switch (exprType) {
      case Null:
      case Int64:
      case Uint64:
      case String:
      case Bytes:
      case MysqlDuration:
      case MysqlTime:
      case MysqlDecimal:
      case ColumnRef:
      case And:
      case Or:
      case LT:
      case LE:
      case EQ:
      case NE:
      case GE:
      case GT:
      case NullEQ:
      case In:
      case ValueList:
      case Like:
      case Not:
        return true;
      case Plus:
      case Div:
        return true;
      case Case:
      case If:
        return true;
      case Count:
      case First:
      case Max:
      case Min:
      case Sum:
      case Avg:
        return true;
        // TODO: finish this
        // case kv.ReqSubTypeDesc:
        // return true;
      default:
        return false;
    }
  }

  // projection
  public SelectBuilder fields(TiExpr[] exprs) {
    int i = 0;
    for (TiExpr expr : exprs) {
      if (!isExprTypeSupported(expr.getExprType())) {
        throw new SelectException("Error!! Expr Type is not supported.");
      }
      builder.setFields(i++, expr.toProto());
    }
    return this;
  }

  public SelectBuilder addRange(TiRange<Long> keyRange) {
    rangeListBuilder.add(keyRange);
    return this;
  }

  public SelectBuilder distinct(boolean distinct) {
    builder.setDistinct(distinct);
    return this;
  }

  public SelectBuilder where(TiExpr expr) {
    if (!isExprTypeSupported(expr.getExprType())) {
      throw new SelectException("Error!! Expr Type is not supported.");
    }

    builder.setWhere(expr.toProto());
    return this;
  }

  public SelectBuilder groupBy(TiByItem[] values) {
    int i = 0;
    for (TiByItem val : values) {
      if (!isExprTypeSupported(val.getExprType())) {
        throw new SelectException("Error!! Expr Type is not supported.");
      }
      builder.setGroupBy(i++, val.toProto());
    }
    return this;
  }

  public SelectBuilder addHaving(TiExpr expr) {
    if (!isExprTypeSupported(expr.getExprType())) {
      throw new SelectException("Error!! Expr Type is not supported.");
    }
    builder.setHaving(expr.toProto());
    return this;
  }

  public SelectBuilder orderBy(TiByItem[] values) {
    int i = 0;
    for (TiByItem val : values) {
      if (!isExprTypeSupported(val.getExprType())) {
        throw new SelectException("Error!! Expr Type is not supported.");
      }
      builder.setOrderBy(i++, val.toProto());
    }
    return this;
  }

  public SelectBuilder limit(long limit) {
    builder.setLimit(limit);
    return this;
  }

  public SelectBuilder addAggreates(TiExpr expr) {
    if (!isExprTypeSupported(expr.getExprType())) {
        throw new SelectException("Error!! Expr Type is not supported.");
    }
    builder.addAggregates(expr.toProto());
    return this;
  }

  public Iterator<Row> doSelect() {
    checkNotNull(table);
    List<TiRange<Long>> ranges = rangeListBuilder.build();
    checkArgument(ranges.size() > 0);
    return snapshot.select(table, builder.build(), ranges);
  }
}
