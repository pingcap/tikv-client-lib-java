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

package com.pingcap.tikv.predicates;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import com.pingcap.tikv.exception.TiClientInternalException;
import com.pingcap.tikv.expression.TiConstant;
import com.pingcap.tikv.expression.TiExpr;
import com.pingcap.tikv.expression.TiFunctionExpression;
import com.pingcap.tikv.expression.scalar.Equal;
import com.pingcap.tikv.expression.scalar.GreaterEqual;
import com.pingcap.tikv.expression.scalar.GreaterThan;
import com.pingcap.tikv.expression.scalar.In;
import com.pingcap.tikv.expression.scalar.LessEqual;
import com.pingcap.tikv.expression.scalar.LessThan;
import com.pingcap.tikv.expression.scalar.NotEqual;
import com.pingcap.tikv.expression.scalar.Or;
import com.pingcap.tikv.predicates.AccessConditionNormalizer.NormalizedCondition;
import com.pingcap.tikv.types.DataType;
import com.pingcap.tikv.value.TypedLiteral;
import java.util.ArrayList;
import java.util.List;

// TODO: reconsider class design and organization
public class RangeBuilder {
  /**
   * Build index ranges from access points and access conditions
   *
   * @param accessPoints conditions converting to a single point access
   * @param accessPointsTypes types of the column matches the condition
   * @param accessConditions conditions converting to a range
   * @param rangeType type of the range
   * @return Index Range for scan
   */
  List<IndexRange> exprsToIndexRanges(
      List<TiExpr> accessPoints,
      List<DataType> accessPointsTypes,
      List<TiExpr> accessConditions,
      DataType rangeType) {
    List<IndexRange> irs = exprsToPoints(accessPoints, accessPointsTypes);
    if (accessConditions != null && accessConditions.size() != 0) {
      List<Range<TypedLiteral>> ranges = exprToRanges(accessConditions, rangeType);
      return appendRanges(irs, ranges, rangeType);
    } else {
      return irs;
    }
  }

  /**
   * Turn access conditions into list of points Each condition is bound to single key We pick up
   * single condition for each index key and disregard if multiple EQ conditions in DNF
   *
   * @param accessPoints expressions that convertible to access points
   * @param types index column types
   * @return access points for each index
   */
  List<IndexRange> exprsToPoints(List<TiExpr> accessPoints, List<DataType> types) {
    requireNonNull(accessPoints, "accessPoints cannot be null");
    requireNonNull(types, "Types cannot be null");
    checkArgument(
        accessPoints.size() == types.size(), "Access points size and type size mismatches");

    List<IndexRange> irs = new ArrayList<>();
    for (int i = 0; i < accessPoints.size(); i++) {
      TiExpr func = accessPoints.get(i);
      DataType type = types.get(i);
      try {
        List<Object> points = exprToPoints(func, type);
        irs = IndexRange.appendPointsForSingleCondition(irs, points, type);
      } catch (Exception e) {
        throw new TiClientInternalException("Error converting access points" + func);
      }
    }
    return irs;
  }

  private static List<Object> exprToPoints(TiExpr expr, DataType type) {
    try {
      if (expr instanceof Or) {
        Or orExpr = (Or) expr;
        return ImmutableList.builder()
            .addAll(exprToPoints(orExpr.getArg(0), type))
            .addAll(exprToPoints(orExpr.getArg(1), type))
            .build();
      }
      checkArgument(
          expr instanceof Equal || expr instanceof In, "Only In and Equal can convert to points");
      TiFunctionExpression func = (TiFunctionExpression) expr;
      NormalizedCondition cond = AccessConditionNormalizer.normalize(func);
      ImmutableList.Builder<Object> result = ImmutableList.builder();
      cond.constantVals.forEach(constVal -> result.add(checkAndExtractConst(constVal, type)));
      return result.build();
    } catch (Exception e) {
      throw new TiClientInternalException("Failed to convert expr to points: " + expr, e);
    }
  }

  /**
   * Turn CNF filters into range
   *
   * @param accessConditions filters in CNF list
   * @param type index column type
   * @return access ranges
   */
  static List<Range<TypedLiteral>> exprToRanges(List<TiExpr> accessConditions, DataType type) {
    if (accessConditions == null || accessConditions.size() == 0) {
      return ImmutableList.of();
    }
    RangeSet<TypedLiteral> ranges = TreeRangeSet.create();
    ranges.add(Range.all());
    for (TiExpr ac : accessConditions) {
      NormalizedCondition cond = AccessConditionNormalizer.normalize(ac);
      TiConstant constVal = cond.constantVals.get(0);
      TypedLiteral literal = TypedLiteral.create(constVal.getValue(), type);
      TiExpr expr = cond.condition;

      if (expr instanceof GreaterThan) {
        ranges = ranges.subRangeSet(Range.greaterThan(literal));
      } else if (expr instanceof GreaterEqual) {
        ranges = ranges.subRangeSet(Range.atLeast(literal));
      } else if (expr instanceof LessThan) {
        ranges = ranges.subRangeSet(Range.lessThan(literal));
      } else if (expr instanceof LessEqual) {
        ranges = ranges.subRangeSet(Range.atMost(literal));
      } else if (expr instanceof Equal) {
        ranges = ranges.subRangeSet(Range.singleton(literal));
      } else if (expr instanceof NotEqual) {
        RangeSet<TypedLiteral> left = ranges.subRangeSet(Range.lessThan(literal));
        RangeSet<TypedLiteral> right = ranges.subRangeSet(Range.greaterThan(literal));
        ranges = TreeRangeSet.create(left);
        ranges.addAll(right);
      } else {
        throw new TiClientInternalException(
            "Unsupported conversion to Range " + expr.getClass().getSimpleName());
      }
    }
    return ImmutableList.copyOf(ranges.asRanges());
  }

  static List<IndexRange> appendRanges(
      List<IndexRange> indexRanges, List<Range<TypedLiteral>> ranges, DataType rangeType) {
    requireNonNull(ranges);
    List<IndexRange> resultRanges = new ArrayList<>();
    if (indexRanges == null || indexRanges.size() == 0) {
      indexRanges = ImmutableList.of(new IndexRange());
    }
    for (IndexRange ir : indexRanges) {
      for (Range r : ranges) {
        resultRanges.add(new IndexRange(ir.getAccessPoints(), ir.getTypes(), r, rangeType));
      }
    }
    return resultRanges;
  }

  private static Object checkAndExtractConst(TiConstant constVal, DataType type) {
    if (type.needCast(constVal.getValue())) {
      throw new TiClientInternalException("Casting not allowed: " + constVal + " to type " + type);
    }
    return constVal.getValue();
  }

  public static class IndexRange {
    private List<Object> accessPoints;
    private List<DataType> types;
    private Range range;
    private DataType rangeType;

    private IndexRange(
        List<Object> accessPoints, List<DataType> types, Range range, DataType rangeType) {
      this.accessPoints = accessPoints;
      this.types = types;
      this.range = range;
      this.rangeType = rangeType;
    }

    private IndexRange(List<Object> accessPoints, List<DataType> types) {
      this.accessPoints = accessPoints;
      this.types = types;
      this.range = null;
    }

    private IndexRange() {
      this.accessPoints = ImmutableList.of();
      this.types = ImmutableList.of();
      this.range = null;
    }

    private static List<IndexRange> appendPointsForSingleCondition(
        List<IndexRange> indexRanges, List<Object> points, DataType type) {
      requireNonNull(indexRanges);
      requireNonNull(points);
      requireNonNull(type);

      List<IndexRange> resultRanges = new ArrayList<>();
      if (indexRanges.size() == 0) {
        indexRanges.add(new IndexRange());
      }

      for (IndexRange ir : indexRanges) {
        resultRanges.addAll(ir.appendPoints(points, type));
      }
      return resultRanges;
    }

    private List<IndexRange> appendPoints(List<Object> points, DataType type) {
      List<IndexRange> result = new ArrayList<>();
      for (Object p : points) {
        ImmutableList.Builder<Object> newAccessPoints =
            ImmutableList.builder().addAll(accessPoints).add(p);

        ImmutableList.Builder<DataType> newTypes =
            ImmutableList.<DataType>builder().addAll(types).add(type);

        result.add(new IndexRange(newAccessPoints.build(), newTypes.build()));
      }
      return result;
    }

    List<Object> getAccessPoints() {
      return accessPoints;
    }

    boolean hasAccessPoints() {
      return accessPoints != null && accessPoints.size() != 0;
    }

    boolean hasRange() {
      return range != null;
    }

    public Range getRange() {
      return range;
    }

    public List<DataType> getTypes() {
      return types;
    }

    DataType getRangeType() {
      return rangeType;
    }
  }
}
