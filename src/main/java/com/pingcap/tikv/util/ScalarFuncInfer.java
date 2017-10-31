package com.pingcap.tikv.util;

import com.pingcap.tidb.tipb.ScalarFuncSig;
import com.pingcap.tikv.exception.TypeException;
import com.pingcap.tikv.types.*;

import java.util.Objects;

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
    Objects.requireNonNull(dataType, "Data type should not be null!");

    if (dataType instanceof IntegerType) {
      Objects.requireNonNull(intSig, "No IntegerType signature provided!");
      return intSig;
    } else if (dataType instanceof DecimalType) {
      Objects.requireNonNull(decimalSig, "No DecimalType signature provided!");
      return decimalSig;
    } else if (dataType instanceof RealType) {
      Objects.requireNonNull(realSig, "No RealType signature provided!");
      return realSig;
    } else if (dataType instanceof DurationType) {
      Objects.requireNonNull(durationType, "No DurationType signature provided!");
      return durationType;
    } else if (dataType instanceof TimestampType || dataType instanceof DateType) {
      Objects.requireNonNull(timeType, "No TimestampType signature provided!");
      return timeType;
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
    Objects.requireNonNull(dataType, "Data type should not be null!");
    if (dataType instanceof BytesType) {
      Objects.requireNonNull(stringType, "No StringType signature provided!");
      return stringType;
    }

    return infer(dataType, intSig, decimalSig, realSig, durationType, timeType);
  }
}
