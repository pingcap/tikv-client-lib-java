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

// A dummy implementation of Row interface
// Using non-memory compact format
public class ObjectRowImpl implements Row {
    private final Object[] values;

    public static Row create(int fieldCount) {
        return new ObjectRowImpl(fieldCount);
    }

    private ObjectRowImpl(int fieldCount) {
        values = new Object[fieldCount];
    }

    @Override
    public void setNull(int pos) {
        values[pos] = null;
    }

    @Override
    public boolean isNull(int pos) {
        return values[pos] == null;
    }


    @Override
    public void setDecimal(int pos, double v) {
        values[pos] = v;
    }

    @Override
    public double getDecimal(int pos) {
        // Null should be handled by client code with isNull
        // below all get method behave the same
        return (double)values[pos];
    }
    
    @Override
    public void setLong(int pos, long v) {
        values[pos] = v;
    }

    @Override
    public long getLong(int pos) {
        // Null should be handled by client code with isNull
        // below all get method behave the same
        return (long)values[pos];
    }

    @Override
    public void setULong(int pos, long v) {
        setLong(pos, v);
    }

    @Override
    public long getULong(int pos) {
        return getLong(pos);
    }

    @Override
    public void setString(int pos, String v) {
        values[pos] = v;
    }

    @Override
    public String getString(int pos) {
        return (String)values[pos];
    }

    @Override
    public void set(int pos, FieldType type, Object v) {
        // Ignore type for this implementation since no serialization happens
        values[pos] = v;
    }

    @Override
    public Object get(int pos, FieldType type) {
        // Ignore type for this implementation since no serialization happens
        return values[pos];
    }

    @Override
    public int fieldCount() {
        return values.length;
    }
}
