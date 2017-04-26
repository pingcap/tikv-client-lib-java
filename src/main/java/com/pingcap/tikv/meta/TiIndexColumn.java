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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;


public class TiIndexColumn {
    private String name;
    private int    offset;
    private int    length;

    @JsonCreator
    public TiIndexColumn(@JsonProperty("name")CIStr   name,
                         @JsonProperty("offset")int   offset,
                         @JsonProperty("length")int   length) {
        this.name = name.getL();
        this.offset = offset;
        this.length = length;
    }

    public String getName() {
        return name;
    }

    public int getOffset() {
        return offset;
    }

    public int getLength() {
        return length;
    }
}
