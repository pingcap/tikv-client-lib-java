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

package com.pingcap.tikv.util;

import com.pingcap.tidb.tipb.ScalarFuncSig;
import com.pingcap.tikv.exception.TypeException;
import com.pingcap.tikv.types.*;

import static java.util.Objects.requireNonNull;

/**
 * The ScalarFunction Signature inferrer.
 * <p>
 * Used to infer a target signature for the given DataType
 */
public class ScalarFuncInfer {
  /**
   * Infer scalar function signature.
   * You should provide candidates for the
   * inferrer to choose.
   *
   * @param dataType     the data type
   * @param intSig       the int sig
   * @param decimalSig   the decimal sig
   * @param realSig      the real sig
   * @param durationType the duration type
   * @param timeType     the time type
   * @return the scalar func sig
   */
  public static ScalarFuncSig infer(DataType dataType,
                                    ScalarFuncSig intSig,
                                    ScalarFuncSig decimalSig,
                                    ScalarFuncSig realSig,
                                    ScalarFuncSig durationType,
                                    ScalarFuncSig timeType) {
    requireNonNull(dataType, "Data type should not be null!");

    if (dataType instanceof IntegerType) {
      return requireNonNull(intSig, "No IntegerType signature provided!");
    } else if (dataType instanceof DecimalType) {
      return requireNonNull(decimalSig, "No DecimalType signature provided!");
    } else if (dataType instanceof RealType) {
      return requireNonNull(realSig, "No RealType signature provided!");
    } else if (dataType instanceof DurationType) {
      return requireNonNull(durationType, "No DurationType signature provided!");
    } else if (dataType instanceof TimestampType || dataType instanceof DateType) {
      return requireNonNull(timeType, "No TimestampType signature provided!");
    } else {
      throw new TypeException("Unsupported data type:" + dataType);
    }
  }

  /**
   * Infer scalar function signature.
   * You should provide candidates for the
   * inferrer to choose.
   *
   * @param dataType     the data type
   * @param intSig       the int sig
   * @param decimalSig   the decimal sig
   * @param realSig      the real sig
   * @param durationType the duration type
   * @param timeType     the time type
   * @param stringType   the string type
   * @return the scalar func sig
   */
  public static ScalarFuncSig infer(DataType dataType,
                                    ScalarFuncSig intSig,
                                    ScalarFuncSig decimalSig,
                                    ScalarFuncSig realSig,
                                    ScalarFuncSig durationType,
                                    ScalarFuncSig timeType,
                                    ScalarFuncSig stringType) {
    requireNonNull(dataType, "Data type should not be null!");

    if (dataType instanceof BytesType) {
      return requireNonNull(stringType, "No StringType signature provided!");
    }

    return infer(dataType, intSig, decimalSig, realSig, durationType, timeType);
  }
}
