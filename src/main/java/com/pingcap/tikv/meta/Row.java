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

package com.pingcap.tikv.meta;


import com.pingcap.tikv.type.FieldType;

// TODO: Mapping unsigned and other types
// Even in case of mem-buffer-based row we can ignore field types
// when en/decoding if we put some padding bits for fixed length
// and use fixed length index for var-length
public interface Row {
    void        setNull(int pos);
    boolean     isNull(int pos);

    void        setDecimal(int pos, double v);
    double      getDecimal(int pos);

    void        setLong(int pos, long v);
    long        getLong(int pos);

    // deal with unsigned as signed for now
    // client code are responsible for wrap with BigInteger if needed
    void        setULong(int pos, long v);
    long        getULong(int pos);

    void        setString(int pos, String v);
    String      getString(int pos);

    void        set(int pos, FieldType type, Object v);
    Object      get(int pos, FieldType type);
    int         fieldCount();
}
