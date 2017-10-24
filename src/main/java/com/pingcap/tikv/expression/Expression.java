package com.pingcap.tikv.expression;

import com.pingcap.tidb.tipb.FieldType;

public interface Expression {
  FieldType getType();

}
