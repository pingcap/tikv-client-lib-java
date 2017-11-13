/*
 * Copyright 2017 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pingcap.tikv.meta;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.pingcap.tidb.tipb.SelectRequest;
import com.pingcap.tikv.exception.TiClientInternalException;
import com.pingcap.tikv.expression.TiByItem;
import com.pingcap.tikv.expression.TiColumnRef;
import com.pingcap.tikv.expression.TiExpr;
import com.pingcap.tikv.kvproto.Coprocessor.KeyRange;
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

public class TiSelectRequest implements Serializable {
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
  private final List<KeyRange> keyRanges = new ArrayList<>();

  private int limit;
  private int timeZoneOffset;
  private long flags;
  private long startTs;
  private TiExpr having;
  private boolean distinct;

  public void resolve() {
    getFields().forEach(expr -> expr.bind(tableInfo));
    getWhere().forEach(expr -> expr.bind(tableInfo));
    getGroupByItems().forEach(item -> item.getExpr().bind(tableInfo));
    getOrderByItems().forEach(item -> item.getExpr().bind(tableInfo));
    getAggregates().forEach(expr -> expr.bind(tableInfo));
    if (having != null) {
      having.bind(tableInfo);
    }
  }

  public SelectRequest buildScan(boolean idxScan) {
    if(idxScan)  {
      return buildIndexScan();
    } else {
      return buildTableScan();
    }
  }

  private SelectRequest buildIndexScan() {
    checkArgument(startTs != 0, "timestamp is 0");
    SelectRequest.Builder builder = SelectRequest.newBuilder();
    if (indexInfo == null) {
      throw new TiClientInternalException("Index is empty for index scan");
    }
    return builder
        .setIndexInfo(indexInfo.toProto(tableInfo))
        .setFlags(flags)
        .setTimeZoneOffset(timeZoneOffset)
        .setStartTs(startTs)
        .build();
  }

  private SelectRequest buildTableScan() {
    checkArgument(startTs != 0, "timestamp is 0");
    SelectRequest.Builder builder = SelectRequest.newBuilder();
    getFields().forEach(expr -> builder.addFields(expr.toProto()));

    for (TiByItem item : getGroupByItems()) {
      builder.addGroupBy(item.toProto());
    }

    for (TiByItem item : getOrderByItems()) {
      builder.addOrderBy(item.toProto());
    }

    for (TiExpr agg : getAggregates()) {
      builder.addAggregates(agg.toProto());
    }

    List<TiColumnInfo> columns;

    if (!getGroupByItems().isEmpty() || !getAggregates().isEmpty()) {
      columns = tableInfo.getColumns();
    } else {
      columns =
          getFields()
              .stream()
              .map(col -> col.bind(tableInfo).getColumnInfo())
              .collect(Collectors.toList());
    }

    TiTableInfo filteredTable =
        new TiTableInfo(
            tableInfo.getId(),
            CIStr.newCIStr(tableInfo.getName()),
            tableInfo.getCharset(),
            tableInfo.getCollate(),
            tableInfo.isPkHandle(),
            columns,
            tableInfo.getIndices(),
            tableInfo.getComment(),
            tableInfo.getAutoIncId(),
            tableInfo.getMaxColumnId(),
            tableInfo.getMaxIndexId(),
            tableInfo.getOldSchemaId());

    TiExpr whereExpr = mergeCNFExpressions(getWhere());
    if (whereExpr != null) {
      builder.setWhere(whereExpr.toProto());
    }

    return builder
        .setTableInfo(filteredTable.toProto())
        .setFlags(flags)
        .setTimeZoneOffset(timeZoneOffset)
        .setStartTs(startTs)
        .build();
  }

  public boolean isIndexScan() {
    return indexInfo != null;
  }

  public TiSelectRequest setTableInfo(TiTableInfo tableInfo) {
    this.tableInfo = requireNonNull(tableInfo, "tableInfo is null");
    return this;
  }

  public TiTableInfo getTableInfo() {
    return this.tableInfo;
  }

  public TiSelectRequest setIndexInfo(TiIndexInfo indexInfo) {
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
  public TiSelectRequest setLimit(int limit) {
    this.limit = limit;
    return this;
  }

  /**
   * set timezone offset
   *
   * @param timeZoneOffset timezone offset
   * @return a TiSelectRequest
   */
  public TiSelectRequest setTimeZoneOffset(int timeZoneOffset) {
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
   * @return a TiSelectRequest
   */
  public TiSelectRequest setTruncateMode(TruncateMode mode) {
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
   * @return a TiSelectRequest
   */
  public TiSelectRequest setStartTs(long startTs) {
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
   * @return a TiSelectRequest
   */
  public TiSelectRequest setHaving(TiExpr having) {
    this.having = requireNonNull(having, "having is null");
    return this;
  }

  public TiSelectRequest setDistinct(boolean distinct) {
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
  public TiSelectRequest addAggregate(TiExpr expr) {
    requireNonNull(expr, "aggregation expr is null");
    aggregates.add(Pair.create(expr, expr.getType()));
    return this;
  }

  public TiSelectRequest addAggregate(TiExpr expr, DataType targetType) {
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
  TiSelectRequest addOrderByItem(TiByItem byItem) {
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
  public TiSelectRequest addGroupByItem(TiByItem byItem) {
    groupByItems.add(requireNonNull(byItem, "byItem is null"));
    return this;
  }

  public List<TiByItem> getGroupByItems() {
    return groupByItems;
  }

  /**
   * Field is not support in TiDB yet, for here we simply allow TiColumnRef instead of TiExpr like
   * in SelectRequest proto
   *
   * <p>This interface allows duplicate columns and it's user's responsibility to do dedup since we
   * need to ensure exact order and items preserved during decoding
   *
   * @param column is column referred during selectReq
   */
  public TiSelectRequest addRequiredColumn(TiColumnRef column) {
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
  public TiSelectRequest addRanges(List<KeyRange> ranges) {
    keyRanges.addAll(requireNonNull(ranges, "KeyRange is null"));
    return this;
  }

  public void resetRanges(List<KeyRange> ranges) {
    keyRanges.clear();
    keyRanges.addAll(ranges);
  }

  public List<KeyRange> getRanges() {
    return keyRanges;
  }

  public TiSelectRequest addWhere(TiExpr where) {
    this.where.add(requireNonNull(where, "where expr is null"));
    return this;
  }

  public List<TiExpr> getWhere() {
    return where;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    if (tableInfo != null) {
      sb.append(String.format("\n Table: %s", tableInfo.getName()));
    }
    if (indexInfo != null) {
      sb.append(String.format("\n Index: %s", indexInfo.toString()));
    }

    if (getRanges().size() != 0) {
      sb.append("\n Ranges: ");
      List<String> rangeStrings;
      if (indexInfo == null) {
        rangeStrings = getRanges()
            .stream()
            .map(r -> KeyRangeUtils.toString(r))
            .collect(Collectors.toList());
      } else {
        List<DataType> types = KeyRangeUtils.getIndexColumnTypes(tableInfo, indexInfo);
        rangeStrings = getRanges()
            .stream()
            .map(r -> KeyRangeUtils.toString(r, types))
            .collect(Collectors.toList());
      }
      sb.append(Joiner.on(", ").skipNulls().join(rangeStrings));
    }

    if (getFields().size() != 0) {
      sb.append("\n Columns: ");
      sb.append(Joiner.on(", ").skipNulls().join(getFields()));
    }

    if (getWhere().size() != 0) {
      sb.append("\n Filter: ");
      sb.append(Joiner.on(", ").skipNulls().join(getWhere()));
    }

    if (getAggregates().size() != 0) {
      sb.append("\n Aggregates: ");
      sb.append(Joiner.on(", ").skipNulls().join(getAggregates()));
    }

    if (getGroupByItems().size() != 0) {
      sb.append("\n Group By: ");
      sb.append(Joiner.on(", ").skipNulls().join(getGroupByItems()));
    }

    if (getOrderByItems().size() != 0) {
      sb.append("\n Order By: ");
      sb.append(Joiner.on(", ").skipNulls().join(getOrderByItems()));
    }
    sb.append("\n");
    return sb.toString();
  }
}
