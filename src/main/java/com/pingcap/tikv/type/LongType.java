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

package com.pingcap.tikv.type;

import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.codec.LongUtils;
import com.pingcap.tikv.meta.Row;
import com.pingcap.tikv.meta.TiColumnInfo;

public class LongType extends FieldType {
    private final boolean varLength;

    private static int UNSIGNED_FLAG = 32;
    public static final int TYPE_CODE = 3;

    public LongType(TiColumnInfo.InternalTypeHolder holder) {
        super(holder);
        this.varLength = true;
    }

    public LongType() {
        this.varLength = true;
    }

    public LongType(int flag, boolean varLength) {
        this.varLength = varLength;
    }

    protected boolean isUnsigned() {
        return (flag & UNSIGNED_FLAG) != 0;
    }

    @Override
    public void decodeValueNoNullToRow(CodecDataInput cdi, Row row, int pos) {
        // NULL should be checked outside
        if (isUnsigned()) {
            if (varLength) {
                row.setULong(pos, LongUtils.readUVarLong(cdi));
            } else {
                row.setULong(pos, LongUtils.readULong(cdi));
            }
        } else {
            if (varLength) {
                row.setLong(pos, LongUtils.readVarLong(cdi));
            } else {
                row.setLong(pos, LongUtils.readLong(cdi));
            }
        }
    }



    @Override
    protected boolean isValidFlag(int flag) {
        if (isUnsigned()) {
            return flag == LongUtils.UINT_FLAG || flag == LongUtils.UVARINT_FLAG;
        } else {
            return flag == LongUtils.INT_FLAG || flag == LongUtils.VARINT_FLAG;
        }
    }

    @Override
    public int getTypeCode() {
        return TYPE_CODE;
    }

    @Override
    public String toString() {
        return (isUnsigned() ? "Unsigned" : "Signed") + "_LongType";
    }
}
