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

package com.pingcap.tikv.expression;

import com.pingcap.tidb.tipb.Expr;
import com.pingcap.tidb.tipb.ExprType;
import com.pingcap.tikv.codec.CodecDataOutput;
import com.pingcap.tikv.exception.TiExpressionException;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.types.DataType;
import com.pingcap.tikv.types.DataType.EncodeType;
import com.pingcap.tikv.types.DateTimeType;
import com.pingcap.tikv.types.DateType;
import com.pingcap.tikv.types.DecimalType;
import com.pingcap.tikv.types.IntegerType;
import com.pingcap.tikv.types.RealType;
import com.pingcap.tikv.types.StringType;
import com.pingcap.tikv.types.TimestampType;
import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Objects;
import org.joda.time.LocalDate;
import org.joda.time.LocalDateTime;


// Refactor needed.
// Refer to https://github.com/pingcap/tipb/blob/master/go-tipb/expression.pb.go
// TODO: This might need a refactor to accept an DataType?
public class TiConstant implements TiExpr {
  public static class DateWrapper implements Serializable {
    private Long value;

    public DateWrapper(Long value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return value == null ? "" : value.toString();
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      } else if (obj instanceof DateWrapper) {
        return ((DateWrapper) obj).value.equals(value);
      }
      return false;
    }

    @Override
    public int hashCode() {
      return (int) (7 * (31 * value));
    }
  }

  private Object value;
  private DataType type;

  public static TiConstant create(Object value) {
    return new TiConstant(value);
  }

  private TiConstant(Object value) {
    this.value = value;
    this.type = getDefaultType();
  }

  protected boolean isIntegerType() {
    return value instanceof Long
        || value instanceof Integer
        || value instanceof Short
        || value instanceof Byte;
  }

  private DataType getDefaultType() {
    if (value == null) {
      throw new TiExpressionException("NULL constant has no type");
    } else if (isIntegerType()) {
      return IntegerType.BIGINT;
    } else if (value instanceof String) {
      return StringType.VARCHAR;
    } else if (value instanceof Float) {
      return RealType.FLOAT;
    } else if (value instanceof Double) {
      return RealType.DOUBLE;
    } else if (value instanceof BigDecimal) {
      return DecimalType.DECIMAL;
    } else if (value instanceof LocalDateTime) {
      return DateTimeType.DATETIME;
    } else if (value instanceof LocalDate) {
      return DateType.DATE;
    } else if (value instanceof Timestamp) {
      return TimestampType.TIMESTAMP;
    } else {
      throw new TiExpressionException("Constant type not supported:" + value.getClass().getSimpleName());
    }
  }

  public void coerce(DataType type) {
    this.type = type;
  }

  public Object getValue() {
    return value;
  }

  // refer to expr_to_pb.go:datumToPBExpr
  // But since it's a java client, we ignored
  // unsigned types for now
  // TODO: Add unsigned constant types support
  @Override
  public Expr toProto() {
    Expr.Builder builder = Expr.newBuilder();
    if (value == null) {
      builder.setTp(ExprType.Null);
      return builder.build();
    } else {
      builder.setTp(type.getProtoExprType());
      CodecDataOutput cdo = new CodecDataOutput();
      type.encode(cdo, EncodeType.PROTO, value);
      builder.setVal(cdo.toByteString());
    }
    return builder.build();
  }

  @Override
  public DataType getType() {
    return type;
  }

  @Override
  public TiConstant resolve(TiTableInfo table) {
    return this;
  }

  @Override
  public String toString() {
    if (value == null) {
      return "null";
    }
    if (value instanceof String) {
      return String.format("\"%s\"", value);
    }
    return value.toString();
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof TiConstant) {
      return Objects.equals(value, ((TiConstant) other).value);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return value == null ? 0 : value.hashCode();
  }
}
