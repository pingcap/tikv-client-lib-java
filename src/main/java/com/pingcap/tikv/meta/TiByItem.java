package com.pingcap.tikv.meta;

import com.pingcap.tidb.tipb.ByItem;
import com.pingcap.tidb.tipb.Expr;
import com.pingcap.tidb.tipb.ExprType;
import com.google.protobuf.ByteString;



public class TiByItem {
    private TiExpr expr;
    private boolean desc;
    public ByItem toProto() {
        ByItem.Builder builder = ByItem.newBuilder();
        return builder.build();
    }

    public ExprType getExprType() {
        return this.expr.getExprType();
    }

}
