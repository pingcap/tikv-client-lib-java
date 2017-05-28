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
import com.pingcap.tikv.codec.DecimalUtils;

public class DecimalType extends FieldType {
    public static final int TYPE_CODE = 6;

    public DecimalType(TiColumnInfo.InternalTypeHolder holder) {
        super(holder);
    }

    public DecimalType() {
    }

    @Override
    public void decodeValueNoNullToRow(CodecDataInput cdi, Row row, int pos) {
        row.setDecimal(pos, DecimalUtils.readDecimalFully(cdi));
    }

    @Override
    protected boolean isValidFlag(int flag) {
        return flag == this.getTypeCode();
    }

    @Override
    public int getTypeCode() {
        return TYPE_CODE;
    }

    @Override
    public String toString() {
        return "DecimalType";
    }
}
