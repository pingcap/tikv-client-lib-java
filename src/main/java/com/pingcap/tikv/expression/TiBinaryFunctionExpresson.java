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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public abstract class TiBinaryFunctionExpresson extends TiFunctionExpression {
  protected TiBinaryFunctionExpresson(TiExpr lhs, TiExpr rhs) {
    super(lhs, rhs);
  }

  public abstract String getName();

  @Override
  protected void validateArguments(TiExpr... args) throws RuntimeException {
    checkNotNull(args, "Arguments of " + getName() + " cannot be null");
    checkArgument(this.args.size() == 2, getName() + " takes only 2 argument");
  }

  @Override
  public String toString() {
    return String.format("(%s %s %s)", getArg(0), getName(), getArg(1));
  }
}
