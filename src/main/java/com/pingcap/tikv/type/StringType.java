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

import com.pingcap.tikv.codec.BytesUtils;
import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.codec.LongUtils;
import com.pingcap.tikv.meta.Row;
import com.pingcap.tikv.meta.TiColumnInfo;

public class StringType extends FieldType {
//    public static final int TYPE_CODE = 0xfe;
    // mysql/type.go:34
    public static final int TYPE_CODE = 15;
    public static boolean isCompacted = false;
    public StringType(TiColumnInfo.InternalTypeHolder holder) {
        super(holder);
    }
    public StringType() {

    }

    @Override
    protected void decodeValueNoNullToRow(CodecDataInput cdi, Row row, int pos) {
        if (isCompacted) {
           byte[] bs = BytesUtils.readCompactBytes(cdi);
           String v = new String(bs);
           row.setString(pos, v);
        } else {
           String v = new String(BytesUtils.readBytes(cdi));
        }
    }

    @Override
    protected boolean isValidFlag(int flag) {
        if (flag == BytesUtils.COMPACT_BYTES_FLAG) {
            this.isCompacted = true;
            return true;
        }
        return flag == BytesUtils.BYTES_FLAG;
    }

    @Override
    public String toString() {
        return "StringType";
    }

    public int getTypeCode() {
        return TYPE_CODE;
    }
}
