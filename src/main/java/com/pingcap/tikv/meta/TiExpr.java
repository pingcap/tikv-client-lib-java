package com.pingcap.tikv.meta;

import com.pingcap.tidb.tipb.Expr;
import com.pingcap.tidb.tipb.ExprType;
import com.google.protobuf.ByteString;
import com.pingcap.tikv.codec.CodecDataOutput;
import com.pingcap.tikv.codec.LongUtils;

import java.util.List;

public class TiExpr {
  private TiTableInfo table;
  private String value;
  private ExprType exprType;
  private static List<Expr> childrens;

  public static TiExpr create() {
    return new TiExpr();
  }

  private TiExpr() {
  }

  public TiExpr setType(ExprType expr) {
    this.exprType = expr;
    return this;
  }

  public TiExpr setValue(String value) {
    this.value = value;
    return this;
  }

  private boolean isExprTypeSupported(ExprType exprType) {
    return true;
  }

  public TiExpr addChildren(Expr expr) {
    if (isExprTypeSupported(expr.getTp())) {
        throw new IllegalArgumentException("Unsupported Expr Type");
    }
    childrens.add(expr);
    return this;
  }

  public TiExpr addAllChildren(List<Expr> exprs) {
      childrens.addAll(exprs);
      return this;
  }

  public Expr toProto() {
    // TODO this is only for Sum
    Expr.Builder builder = Expr.newBuilder();
    Expr.Builder columnRefBuilder = Expr.newBuilder();
    builder.setTp(exprType);
    columnRefBuilder.setTp(ExprType.ColumnRef);
    CodecDataOutput cdo = new CodecDataOutput();
    //TODO: tableinfo passed into and getColumnId
    LongUtils.writeLong(cdo, 1);
    columnRefBuilder.setVal(ByteString.copyFrom(cdo.toBytes()));
    builder.addChildren(0, columnRefBuilder.build());
    return builder.build();
  }

  public ExprType getExprType() {
    return this.exprType;
  }

  public static Expr getDefaultInstance() {
    return Expr.getDefaultInstance();
  }
}
