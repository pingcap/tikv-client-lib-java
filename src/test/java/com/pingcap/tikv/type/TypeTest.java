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

import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.pingcap.tidb.tipb.Chunk;
import com.pingcap.tidb.tipb.RowMeta;
import com.pingcap.tidb.tipb.Select;
import com.pingcap.tikv.*;
import com.pingcap.tikv.codec.BytesUtils;
import com.pingcap.tikv.meta.*;
import com.pingcap.tikv.operation.SelectIterator;
import jdk.nashorn.internal.parser.JSONParser;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class TypeTest {
    private final String value = "foo";

    @Test
    public void testLongAndStringType() throws Exception {
        String rowsData = "\b\002\002\006foo";
        RowMeta rowMeta = RowMeta.newBuilder()
                            .setHandle((long)1)
                            .setLength((long)7)
                            .build();
        Chunk chunk = Chunk.newBuilder().addRowsMeta(rowMeta).setRowsData(ByteString.copyFromUtf8(rowsData)).build();
        List<Chunk> chunks = new ArrayList<>();
        chunks.add(chunk);
        FieldType[] fieldTypes = new FieldType[2];
        fieldTypes[0] = new LongType();
        fieldTypes[1] = new StringType();
        SelectIterator it = new SelectIterator(chunks, fieldTypes);
        Row r  = it.next();
        long val1 = r.getLong(0);
        String val2 = r.getString(1);
        Assert.assertSame("value should be 1", (long)1, val1);
        Assert.assertTrue(value.equals(val2));
    }
}
