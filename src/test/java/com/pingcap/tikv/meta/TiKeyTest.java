/*
 *
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
 *
 */

package com.pingcap.tikv.meta;

import com.google.protobuf.ByteString;
import org.junit.Test;

import java.util.function.Function;

import static org.junit.Assert.assertTrue;

@SuppressWarnings("unchecked")
public class TiKeyTest {
  @Test
  public void wrapTest() throws Exception {
    // compared as unsigned
    testBytes(new byte[] {1, 2, -1, 10}, new byte[] {1, 2, 0, 10}, x -> x > 0);
    testBytes(new byte[] {1, 2, 0, 10}, new byte[] {1, 2, 0, 10}, x -> x == 0);
    testBytes(new byte[] {1, 2, 0, 10}, new byte[] {1, 2, 1, 10}, x -> x < 0);
    testBytes(new byte[] {1, 2, 0, 10}, new byte[] {1, 2, 0}, x -> x > 0);

    testComparable(1, 2, x -> x < 0);
    testComparable(13, 13, x -> x == 0);
    testComparable(13, 2, x -> x > 0);
  }

  private void testBytes(byte[] lhs, byte[] rhs, Function<Integer, Boolean> tester) {
    ByteString lhsBS = ByteString.copyFrom(lhs);
    ByteString rhsBS = ByteString.copyFrom(rhs);

    Comparable lhsComp = TiKey.create(lhsBS);
    Comparable rhsComp = TiKey.create(rhsBS);

    assertTrue(tester.apply(lhsComp.compareTo(rhsComp)));

    lhsComp = TiKey.create(lhs);
    rhsComp = TiKey.create(rhs);

    assertTrue(tester.apply(lhsComp.compareTo(rhsComp)));
  }

  private void testComparable(Object lhs, Object rhs, Function<Integer, Boolean> tester) {
    Comparable lhsComp = TiKey.create(lhs);
    Comparable rhsComp = TiKey.create(rhs);

    assertTrue(tester.apply(lhsComp.compareTo(rhsComp)));
  }
}
