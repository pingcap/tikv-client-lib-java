package com.pingcap.tikv.expression.scalar;

import com.pingcap.tidb.tipb.Expr;
import com.pingcap.tidb.tipb.ExprType;
import com.pingcap.tidb.tipb.FieldType;
import com.pingcap.tidb.tipb.ScalarFuncSig;
import com.pingcap.tikv.expression.TiExpr;
import com.pingcap.tikv.expression.TiFunctionExpression;
import com.pingcap.tikv.types.DataType;

/**
 * Scalar function
 * Used in DAG mode
 */
public abstract class ScalarFunction extends TiFunctionExpression {
  ScalarFunction(TiExpr... args) {
    super(args);
  }

  /**
   * Gets scalar function PB code representation.
   *
   * @return the pb code
   */
  abstract ScalarFuncSig getSignature();

  /**
   * Get scalar function argument type
   *
   * Note:In DAG mode, all the arguments' type should
   * be the same
   */
  public DataType getArgType() {
    if (args.isEmpty()) {
      return null;
    }

    return args.get(0).getType();
  }

  @Override
  public Expr toProto() {
    Expr.Builder builder = Expr.newBuilder();
    // Scalar function type
    builder.setTp(ExprType.ScalarFunc);
    // Return type
    builder.setFieldType(FieldType.newBuilder().setTp(getType().getTypeCode()).build());
    // Set function signature
    builder.setSig(getSignature());
    for (TiExpr arg : args) {
      builder.addChildren(arg.toProto());
    }

    return builder.build();
  }
}
