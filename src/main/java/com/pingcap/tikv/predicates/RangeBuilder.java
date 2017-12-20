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
import com.pingcap.tikv.key.CompoundKey;
import com.pingcap.tikv.key.Key;
import com.pingcap.tikv.key.TypedKey;
import com.pingcap.tikv.predicates.AccessConditionNormalizer.NormalizedCondition;
import com.pingcap.tikv.types.DataType;
import java.util.ArrayList;
import java.util.List;

// TODO: reconsider class design and organization
public class RangeBuilder {
  /**
   * Build index ranges from access points and access conditions
   *
   * @param pointExprs conditions converting to a single point access
   * @param pointExprTypes types of the column matches the condition
   * @param rangeExpr conditions converting to a range
   * @param rangeType type of the range
   * @return Index Range for scan
   */
  List<IndexRange> expressionToIndexRanges(
      List<TiExpr> pointExprs,
      List<DataType> pointExprTypes,
      List<TiExpr> rangeExpr,
      DataType rangeType) {
    ImmutableList.Builder<IndexRange> builder = ImmutableList.builder();
    List<Key> pointKeys = expressionToPoints(pointExprs, pointExprTypes);
    for (Key key : pointKeys) {
      if (rangeExpr != null && !rangeExpr.isEmpty()) {
        List<Range<TypedKey>> ranges = expressionToRanges(rangeExpr, rangeType);
        for (Range<TypedKey> range : ranges) {
          builder.add(new IndexRange(key, range));
        }
      } else {
        builder.add(new IndexRange(key));
      }
    }
    return builder.build();
  }

  /**
   * Turn access conditions into list of points Each condition is bound to single key We pick up
   * single condition for each index key and disregard if multiple EQ conditions in DNF
   *
   * @param accessPointExprs expressions that convertible to access points
   * @param types index column types
   * @return access points for each index
   */
  List<Key> expressionToPoints(List<TiExpr> accessPointExprs, List<DataType> types) {
    requireNonNull(accessPointExprs, "accessPointExprs cannot be null");
    requireNonNull(types, "Types cannot be null");
    checkArgument(accessPointExprs.size() == types.size(), "Access points size and type size mismatches");

    List<Key> resultKeys = new ArrayList<>();
    for (int i = 0; i < accessPointExprs.size(); i++) {
      TiExpr expr = accessPointExprs.get(i);
      DataType type = types.get(i);
      try {
        // each expr will be expand to one or more points
        List<Key> points = expressionToKeys(expr, type);
        resultKeys = joinKeys(resultKeys, points);
      } catch (Exception e) {
        throw new TiClientInternalException(String.format("Error converting access points %s", expr), e);
      }
    }
    return resultKeys;
  }

  private List<Key> joinKeys(List<Key> lhsKeys, List<Key> rhsKeys) {
    requireNonNull(lhsKeys, "lhsKeys is null");
    requireNonNull(rhsKeys, "rhsKeys is null");
    if (lhsKeys.isEmpty()) {
      return rhsKeys;
    }
    if (rhsKeys.isEmpty()) {
      return lhsKeys;
    }
    ImmutableList.Builder<Key> builder = ImmutableList.builder();
    for (Key lKey : lhsKeys) {
      for (Key rKey : rhsKeys) {
        builder.add(CompoundKey.concat(lKey, rKey));
      }
    }
    return builder.build();
  }

  private static List<Key> expressionToKeys(TiExpr expr, DataType type) {
    try {
      if (expr instanceof Or) {
        Or orExpr = (Or) expr;
        return ImmutableList.<Key>builder()
            .addAll(expressionToKeys(orExpr.getArg(0), type))
            .addAll(expressionToKeys(orExpr.getArg(1), type))
            .build();
      }
      checkArgument(expr instanceof Equal || expr instanceof In, "Only In and Equal can convert to points");
      TiFunctionExpression func = (TiFunctionExpression) expr;
      NormalizedCondition cond = AccessConditionNormalizer.normalize(func);
      ImmutableList.Builder<Key> result = ImmutableList.builder();
      cond.constantVals.forEach(constVal -> result.add(TypedKey.toTypedKey(constVal.getValue(), type)));
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
  static List<Range<TypedKey>> expressionToRanges(List<TiExpr> accessConditions, DataType type) {
    if (accessConditions == null || accessConditions.size() == 0) {
      return ImmutableList.of();
    }
    RangeSet<TypedKey> ranges = TreeRangeSet.create();
    ranges.add(Range.all());
    for (TiExpr ac : accessConditions) {
      NormalizedCondition cond = AccessConditionNormalizer.normalize(ac);
      TiConstant constVal = cond.constantVals.get(0);
      TypedKey literal = TypedKey.toTypedKey(constVal.getValue(), type);
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
        RangeSet<TypedKey> left = ranges.subRangeSet(Range.lessThan(literal));
        RangeSet<TypedKey> right = ranges.subRangeSet(Range.greaterThan(literal));
        ranges = TreeRangeSet.create(left);
        ranges.addAll(right);
      } else {
        throw new TiClientInternalException(
            "Unsupported conversion to Range " + expr.getClass().getSimpleName());
      }
    }
    return ImmutableList.copyOf(ranges.asRanges());
  }

  public static class IndexRange {
    private Key accessKey;
    private Range<TypedKey> range;

    private IndexRange(Key accessKey, Range<TypedKey> range) {
      this.accessKey = accessKey;
      this.range = range;
    }

    private IndexRange(Key accessKey) {
      this.accessKey = accessKey;
      this.range = null;
    }

    Key getAccessKey() {
      return accessKey;
    }

    boolean hasAccessKeys() {
      return accessKey != null && accessKey != null;
    }

    boolean hasRange() {
      return range != null;
    }

    public Range<TypedKey> getRange() {
      return range;
    }
  }
}
