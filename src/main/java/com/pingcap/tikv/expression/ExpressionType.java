package com.pingcap.tikv.expression;

public enum ExpressionType {
  Constant,
  ColumnRef,
  ScalarFunction,
  Aggregation
}
