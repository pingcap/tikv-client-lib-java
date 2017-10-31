package com.pingcap.tikv.meta;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.pingcap.tidb.tipb.*;
import com.pingcap.tikv.exception.TiClientInternalException;
import com.pingcap.tikv.expression.*;
import com.pingcap.tikv.kvproto.Coprocessor;
import com.pingcap.tikv.types.DataType;
import com.pingcap.tikv.util.KeyRangeUtils;
import com.pingcap.tikv.util.Pair;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.pingcap.tikv.predicates.PredicateUtils.mergeCNFExpressions;
import static java.util.Objects.requireNonNull;

/**
 * The type Ti dag request.
 */
public class TiDAGRequest implements Serializable {
  public enum TruncateMode {
    IgnoreTruncation(0x1),
    TruncationAsWarning(0x2);

    private final long mask;

    TruncateMode(long mask) {
      this.mask = mask;
    }

    public long mask(long flags) {
      return flags | mask;
    }
  }

  private TiTableInfo tableInfo;
  private TiIndexInfo indexInfo;
  private final List<TiColumnRef> fields = new ArrayList<>();
  private final List<TiExpr> where = new ArrayList<>();
  private final List<TiByItem> groupByItems = new ArrayList<>();
  private final List<TiByItem> orderByItems = new ArrayList<>();
  // System like Spark has different type promotion rules
  // we need a cast to target when given
  private final List<Pair<TiExpr, DataType>> aggregates = new ArrayList<>();
  private final List<Coprocessor.KeyRange> keyRanges = new ArrayList<>();

  private int limit;
  private int timeZoneOffset;
  private long flags;
  private long startTs;
  private TiExpr having;
  private boolean distinct;
  private boolean handleNeeded;

  public void bind() {
    getFields().forEach(expr -> expr.bind(tableInfo));
    getWhere().forEach(expr -> expr.bind(tableInfo));
    getGroupByItems().forEach(item -> item.getExpr().bind(tableInfo));
    getOrderByItems().forEach(item -> item.getExpr().bind(tableInfo));
    getAggregates().forEach(expr -> expr.bind(tableInfo));
    if (having != null) {
      having.bind(tableInfo);
    }
  }

  public DAGRequest buildScan(boolean idxScan) {
    if (idxScan) {
      return buildIndexScan();
    } else {
      return buildTableScan();
    }
  }

  // See TiDB source code: executor/builder.go:945
  private DAGRequest buildIndexScan() {
    checkArgument(startTs != 0, "timestamp is 0");
    if (indexInfo == null) {
      throw new TiClientInternalException("Index is empty for index scan");
    }
    DAGRequest.Builder dagRequestBuilder = DAGRequest.newBuilder();
    Executor.Builder executorBuilder = Executor.newBuilder();
    IndexScan.Builder indexScanBuilder = IndexScan.newBuilder();
    indexScanBuilder
        .setTableId(tableInfo.getId())
        .setIndexId(indexInfo.getId());
    dagRequestBuilder.addExecutors(executorBuilder.setIdxScan(indexScanBuilder));

    return dagRequestBuilder
        .setFlags(flags)
        .setTimeZoneOffset(timeZoneOffset)
        .setStartTs(startTs)
        .build();
  }

  //  private DAGRequest buildTableScan() {
//    DAGRequest.Builder dagBuilder = DAGRequest.newBuilder();
//    dagBuilder.setStartTs(System.currentTimeMillis());
////    dagBuilder.addOutputOffsets(0);
////    dagBuilder.addOutputOffsets(1);
//    dagBuilder.setTimeZoneOffset(0);
//    dagBuilder.setFlags(0);
//    Executor.Builder executorBuilder = Executor.newBuilder();
////    executorBuilder.setTp(ExecType.TypeTableScan);
//    TableScan.Builder tableScanBuilder = TableScan.newBuilder();
////    tableScanBuilder.addColumns(tableInfo.getColumns().get(0).toProto(tableInfo));
////    tableScanBuilder.addColumns(tableInfo.getColumns().get(1).toProto(tableInfo));
//    tableScanBuilder.setDesc(false);
//
//    executorBuilder.setTblScan(tableScanBuilder);
//    dagBuilder.addExecutors(executorBuilder.build());
//
//
//
//    return dagBuilder.build();
//  }
  // See TiDB source code: executor/builder.go:890
  private DAGRequest buildTableScan() {
    checkArgument(startTs != 0, "timestamp is 0");
    DAGRequest.Builder dagRequestBuilder = DAGRequest.newBuilder();
    Executor.Builder executorBuilder = Executor.newBuilder();
    TableScan.Builder tblScanBuilder = TableScan.newBuilder();

    // Step1. Add columns to first executor
//    getFields().stream().map(r -> r.bind(tableInfo).getColumnInfo().toProto(tableInfo)).forEach(tblScanBuilder::addColumns);
    tableInfo.getColumns().forEach(tiColumnInfo -> tblScanBuilder.addColumns(tiColumnInfo.toProto(tableInfo)));
    executorBuilder.setTp(ExecType.TypeTableScan);
    tblScanBuilder.setTableId(tableInfo.getId());
    // cache locally in case of concurrent modification
    boolean needHandle = isHandleNeeded();
    // Currently, according to TiKV's implementation, if handle
    // is needed, we should add an extra column with an ID of -1
    if (needHandle) {
      ColumnInfo handleColumn = ColumnInfo.newBuilder()
          .setColumnId(-1)
          .setPkHandle(true)
          .build();
      tblScanBuilder.addColumns(handleColumn);
    }
    dagRequestBuilder.addExecutors(executorBuilder.setTblScan(tblScanBuilder));
    executorBuilder.clear();

    // Step2. Add others
    // DO NOT EDIT EXPRESSION ADD ORDER
    TiExpr whereExpr = mergeCNFExpressions(getWhere());
    if (whereExpr != null) {
      executorBuilder.setTp(ExecType.TypeSelection);
      dagRequestBuilder.addExecutors(
          executorBuilder.setSelection(
              Selection.newBuilder().addConditions(whereExpr.toProto())
          )
      );
    }

    if (!getGroupByItems().isEmpty() || !getAggregates().isEmpty()) {
      Aggregation.Builder aggregationBuilder = Aggregation.newBuilder();
      getGroupByItems().forEach(tiByItem -> aggregationBuilder.addGroupBy(tiByItem.getExpr().toProto()));
      getAggregates().forEach(tiExpr -> aggregationBuilder.addAggFunc(tiExpr.toProto()));
      executorBuilder.setTp(ExecType.TypeAggregation);
      dagRequestBuilder.addExecutors(
          executorBuilder.setAggregation(aggregationBuilder)
      );
      executorBuilder.clear();
    }

    if (!getOrderByItems().isEmpty()) {
      TopN.Builder topNBuilder = TopN.newBuilder();
      getOrderByItems().forEach(tiByItem -> topNBuilder.addOrderBy(tiByItem.toProto()));
      executorBuilder.setTp(ExecType.TypeTopN);
      dagRequestBuilder.addExecutors(executorBuilder.setTopN(topNBuilder));
      executorBuilder.clear();
    }

    if (0 != getLimit()) {
      Limit.Builder limitBuilder = Limit.newBuilder();
      limitBuilder.setLimit(getLimit());
      executorBuilder.setTp(ExecType.TypeLimit);
      dagRequestBuilder.addExecutors(executorBuilder.setLimit(limitBuilder));
      executorBuilder.clear();
    }

    getFields().forEach(tiColumnInfo -> dagRequestBuilder.addOutputOffsets(tiColumnInfo.getColumnInfo().getOffset()));
//    tableInfo.getColumns().forEach(tiColumnInfo -> dagRequestBuilder.addOutputOffsets(tiColumnInfo.getOffset()));
//    setTruncateMode(TruncateMode.TruncationAsWarning);
    // if handle is needed, we should append one output offset
    if (needHandle) {
      dagRequestBuilder.addOutputOffsets(tableInfo.getColumns().size());
    }
//    for (int i = 0; i <= tableInfo.getColumns().size(); i++) {
//      dagRequestBuilder.addOutputOffsets(i);
//    }
    return dagRequestBuilder
        .setTimeZoneOffset(timeZoneOffset)
        .setFlags(flags)
        .setStartTs(startTs)
        .build();
  }

  public TiDAGRequest setTableInfo(TiTableInfo tableInfo) {
    this.tableInfo = requireNonNull(tableInfo, "tableInfo is null");
    return this;
  }

  public TiTableInfo getTableInfo() {
    return this.tableInfo;
  }

  public TiDAGRequest setIndexInfo(TiIndexInfo indexInfo) {
    this.indexInfo = requireNonNull(indexInfo, "indexInfo is null");
    return this;
  }

  TiIndexInfo getIndexInfo() {
    return indexInfo;
  }

  public int getLimit() {
    return limit;
  }

  /**
   * add limit clause to select query.
   *
   * @param limit is just a integer.
   * @return a SelectBuilder
   */
  public TiDAGRequest setLimit(int limit) {
    this.limit = limit;
    return this;
  }

  /**
   * set timezone offset
   *
   * @param timeZoneOffset timezone offset
   * @return a TiDAGRequest
   */
  public TiDAGRequest setTimeZoneOffset(int timeZoneOffset) {
    this.timeZoneOffset = timeZoneOffset;
    return this;
  }

  int getTimeZoneOffset() {
    return timeZoneOffset;
  }

  /**
   * set truncate mode
   *
   * @param mode truncate mode
   * @return a TiDAGRequest
   */
  public TiDAGRequest setTruncateMode(TiDAGRequest.TruncateMode mode) {
    flags = requireNonNull(mode, "mode is null").mask(flags);
    return this;
  }

  @VisibleForTesting
  public long getFlags() {
    return flags;
  }

  /**
   * set start timestamp for the transaction
   *
   * @param startTs timestamp
   * @return a TiDAGRequest
   */
  public TiDAGRequest setStartTs(long startTs) {
    this.startTs = startTs;
    return this;
  }

  long getStartTs() {
    return startTs;
  }

  /**
   * set having clause to select query
   *
   * @param having is a expression represents Having
   * @return a TiDAGRequest
   */
  public TiDAGRequest setHaving(TiExpr having) {
    this.having = requireNonNull(having, "having is null");
    return this;
  }

  public TiDAGRequest setDistinct(boolean distinct) {
    this.distinct = distinct;
    return this;
  }

  public boolean isDistinct() {
    return distinct;
  }

  /**
   * add aggregate function to select query
   *
   * @param expr is a TiUnaryFunction expression.
   * @return a SelectBuilder
   */
  public TiDAGRequest addAggregate(TiExpr expr) {
    requireNonNull(expr, "aggregation expr is null");
    aggregates.add(Pair.create(expr, expr.getType()));
    return this;
  }

  public TiDAGRequest addAggregate(TiExpr expr, DataType targetType) {
    requireNonNull(expr, "aggregation expr is null");
    aggregates.add(Pair.create(expr, targetType));
    return this;
  }

  public List<TiExpr> getAggregates() {
    return aggregates.stream().map(p -> p.first).collect(Collectors.toList());
  }

  public List<Pair<TiExpr, DataType>> getAggregatePairs() {
    return aggregates;
  }

  /**
   * add a order by clause to select query.
   *
   * @param byItem is a TiByItem.
   * @return a SelectBuilder
   */
  TiDAGRequest addOrderByItem(TiByItem byItem) {
    orderByItems.add(requireNonNull(byItem, "byItem is null"));
    return this;
  }

  List<TiByItem> getOrderByItems() {
    return orderByItems;
  }

  /**
   * add a group by clause to select query
   *
   * @param byItem is a TiByItem
   * @return a SelectBuilder
   */
  public TiDAGRequest addGroupByItem(TiByItem byItem) {
    groupByItems.add(requireNonNull(byItem, "byItem is null"));
    return this;
  }

  public List<TiByItem> getGroupByItems() {
    return groupByItems;
  }

  /**
   * Field is not support in TiDB yet, for here we simply allow TiColumnRef instead of TiExpr like
   * in SelectRequest proto
   * <p>
   * <p>This interface allows duplicate columns and it's user's responsibility to do dedup since we
   * need to ensure exact order and items preserved during decoding
   *
   * @param column is column referred during selectReq
   */
  public TiDAGRequest addField(TiColumnRef column) {
    fields.add(requireNonNull(column, "columnRef is null"));
    return this;
  }

  public List<TiColumnRef> getFields() {
    return fields;
  }

  /**
   * set key range of scan
   *
   * @param ranges key range of scan
   */
  public TiDAGRequest addRanges(List<Coprocessor.KeyRange> ranges) {
    keyRanges.addAll(requireNonNull(ranges, "KeyRange is null"));
    return this;
  }

  public void resetRanges(List<Coprocessor.KeyRange> ranges) {
    keyRanges.clear();
    keyRanges.addAll(ranges);
  }

  public List<Coprocessor.KeyRange> getRanges() {
    return keyRanges;
  }

  public TiDAGRequest addWhere(TiExpr where) {
    this.where.add(requireNonNull(where, "where expr is null"));
    return this;
  }

  public boolean hasAggregate() {
    return null == getAggregates() ||
        !getAggregates().isEmpty();
  }

  public boolean hasGroupBy() {
    return null == getGroupByItems() ||
        !getGroupByItems().isEmpty();
  }

  public List<TiExpr> getWhere() {
    return where;
  }

  public List<TiColumnInfo> getColInfoList() {
    return getFields().stream().map(TiColumnRef::getColumnInfo).collect(Collectors.toList());
  }

  /**
   * Gets group by dt list.
   *
   * @return the group by dt list
   */
  public List<DataType> getGroupByDTList() {
    return getGroupByItems()
        .stream()
        .map(TiByItem::getExpr)
        .map(TiExpr::getType)
        .collect(Collectors.toList());
  }

  /**
   * Is handle needed boolean.
   *
   * @return the boolean
   */
  public boolean isHandleNeeded() {
    return handleNeeded;
  }

  /**
   * Sets handle needed.
   *
   * @param handleNeeded the handle needed
   */
  public void setHandleNeeded(boolean handleNeeded) {
    this.handleNeeded = handleNeeded;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    if (tableInfo != null) {
      sb.append(String.format("[table: %s] ", tableInfo.getName()));
    }

    if (getRanges().size() != 0) {
      sb.append(", Ranges: ");
      List<String> rangeStrings = getRanges()
          .stream()
          .map(r -> KeyRangeUtils.toString(r))
          .collect(Collectors.toList());
      sb.append(Joiner.on(", ").skipNulls().join(rangeStrings));
    }

    if (getFields().size() != 0) {
      sb.append(", Columns: ");
      sb.append(Joiner.on(", ").skipNulls().join(getFields()));
    }

    if (getWhere().size() != 0) {
      sb.append(", Aggregates: ");
      sb.append(Joiner.on(", ").skipNulls().join(getWhere()));
    }

    if (getAggregates().size() != 0) {
      sb.append(", Aggregates: ");
      sb.append(Joiner.on(", ").skipNulls().join(getAggregates()));
    }

    if (getGroupByItems().size() != 0) {
      sb.append(", Group By: ");
      sb.append(Joiner.on(", ").skipNulls().join(getGroupByItems()));
    }

    if (getOrderByItems().size() != 0) {
      sb.append(", Order By: ");
      sb.append(Joiner.on(", ").skipNulls().join(getOrderByItems()));
    }
    return sb.toString();

  }

}
