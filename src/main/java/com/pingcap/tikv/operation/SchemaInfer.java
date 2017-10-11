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

package com.pingcap.tikv.operation;

import com.pingcap.tikv.expression.TiByItem;
import com.pingcap.tikv.expression.TiExpr;
import com.pingcap.tikv.meta.TiDAGRequest;
import com.pingcap.tikv.operation.transformer.*;
import com.pingcap.tikv.types.DataType;
import com.pingcap.tikv.types.DataTypeFactory;
import com.pingcap.tikv.util.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.pingcap.tikv.types.Types.TYPE_BLOB;

/**
 * SchemaInfer extract row's type after query is executed. It is pretty rough version. Optimization
 * is on the way. The problem we have right now is that TiDB promote Sum to Decimal which is not
 * compatible with column's type. The solution we come up with right now is use record column's type
 * ad finalFieldType and build another list recording TiExpr's type as fieldType for row reading.
 * Once we finish row reading, we first check each element in fieldType and finalFieldType share the
 * same type or not. If yes, no need for casting. If no, casting is needed here.
 */
public class SchemaInfer {
  private List<DataType> types;
  private RowTransformer rt;

  public static SchemaInfer create(TiDAGRequest dagRequest) {
    return new SchemaInfer(dagRequest);
  }

  private SchemaInfer(TiDAGRequest dagRequest) {
    types = new ArrayList<>();
    extractFieldTypes(dagRequest);
    buildTransform(dagRequest);
  }

  private void buildTransform(TiDAGRequest dagRequest) {
    RowTransformer.Builder rowTrans = RowTransformer.newBuilder();
    // 1. if group by is empty, first column should be "single group"
    // which is a string
    // 2. if multiple group by items present, it is wrapped inside
    // a byte array. we make a multiple decoding
    // 3. for no aggregation case, make only projected columns
    if (dagRequest.getGroupByItems().isEmpty()) {
      if (!dagRequest.getAggregatePairs().isEmpty()) {
        rowTrans.addProjection(Skip.SKIP_OP);
      }
    } else {
      List<DataType> types =
          dagRequest
              .getGroupByItems()
              .stream()
              .map(TiByItem::getExpr)
              .map(TiExpr::getType)
              .collect(Collectors.toList());

      rowTrans.addProjection(new MultiKeyDecoder(types));
    }

    // append aggregates if present
    if (!dagRequest.getAggregatePairs().isEmpty()) {
      for (Pair<TiExpr, DataType> pair : dagRequest.getAggregatePairs()) {
        rowTrans.addProjection(new Cast(pair.second));
      }
    } else {
      for (TiExpr field : dagRequest.getFields()) {
        rowTrans.addProjection(new NoOp(field.getType()));
      }
    }
    rowTrans.addSourceFieldTypes(types);
    rt = rowTrans.build();
  }

  /**
   * TODO: order by extract field types from tiSelectRequest for reading data to row.
   *
   * @param dagRequest is SelectRequest
   */
  private void extractFieldTypes(TiDAGRequest dagRequest) {
    if (!dagRequest.getAggregates().isEmpty()) {
      // In some cases, aggregates come without group by clause, we need add a dummy
      // single group for it.
      types.add(DataTypeFactory.of(TYPE_BLOB));
      dagRequest.getAggregates().forEach(expr -> types.add(expr.getType()));
    } else {
      // Extract all column type information from TiExpr
      dagRequest.getFields().forEach(expr -> types.add(expr.getType()));
    }
  }

  public DataType getType(int index) {
    return types.get(index);
  }

  public List<DataType> getTypes() {
    return types;
  }

  public RowTransformer getRowTransformer() {
    return this.rt;
  }
}
