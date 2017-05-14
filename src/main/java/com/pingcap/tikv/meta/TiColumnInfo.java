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
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import com.pingcap.tidb.tipb.ColumnInfo;
import com.pingcap.tikv.exception.TiClientInternalException;
import com.pingcap.tikv.type.FieldType;
import com.pingcap.tikv.type.LongType;
import com.pingcap.tikv.type.StringType;

import java.util.List;
import java.util.Map;


@JsonIgnoreProperties(ignoreUnknown = true)
public class TiColumnInfo {
    private final long        id;
    private final String      name;
    private final int         offset;
    private final FieldType   type;
    private final SchemaState schemaState;
    private final String      comment;
    private final boolean     isPrimaryKey;

    private static final int PK_MASK = 0x2;

    @JsonCreator
    public TiColumnInfo(@JsonProperty("id")long                   id,
                        @JsonProperty("name")CIStr                name,
                        @JsonProperty("offset")int                offset,
                        @JsonProperty("type")InternalTypeHolder   type,
                        @JsonProperty("state")int                 schemaState,
                        @JsonProperty("comment")String            comment) {
        this.id = id;
        this.name = name.getL();
        this.offset = offset;
        this.type = type.toFieldType();
        this.schemaState = SchemaState.fromValue(schemaState);
        this.comment = comment;
        // I don't think pk flag should be set on type
        // Refactor against original tidb code
        this.isPrimaryKey = (type.getFlag() & PK_MASK) > 0;
    }

    public long getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public int getOffset() {
        return offset;
    }

    public FieldType getType() {
        return type;
    }

    public SchemaState getSchemaState() {
        return schemaState;
    }

    public String getComment() {
        return comment;
    }

    public boolean isPrimaryKey() {
        return isPrimaryKey;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class InternalTypeHolder {
        private final int          tp;
        private final int          flag;
        private final int          flen;
        private final int          decimal;
        private final String       charset;
        private final String       collate;
        private final List<String> elems;

        interface Builder<E extends FieldType> {
            E build(InternalTypeHolder holder);
        }

        private static Map<Integer, Builder<? extends FieldType>> typeBuilder;
        static {
            typeBuilder = ImmutableMap.<Integer, Builder<? extends FieldType>>builder()
                    .put(LongType.TYPE_CODE, holder -> new LongType(holder))
                    .put(StringType.TYPE_CODE, holder -> new StringType(holder))
                    .build();
        }

        @JsonCreator
        public InternalTypeHolder(@JsonProperty("Tp")int                tp,
                                  @JsonProperty("Flag")int              flag,
                                  @JsonProperty("Flen")int              flen,
                                  @JsonProperty("Decimal")int           decimal,
                                  @JsonProperty("Charset")String        charset,
                                  @JsonProperty("Collate")String        collate,
                                  @JsonProperty("Elems")List<String>    elems) {
            this.tp = tp;
            this.flag = flag;
            this.flen = flen;
            this.decimal = decimal;
            this.charset = charset;
            this.collate = collate;
            this.elems = elems;
        }

        public InternalTypeHolder(ColumnInfo c) {
            this.tp = c.getTp();
            this.flag = c.getFlag();
            this.flen = c.getColumnLen();
            this.decimal = c.getDecimal();
            this.charset = "";
            this.collate = Collation.translate(c.getCollation());
            this.elems = c.getElemsList();
        }

        public int getTp() {
            return tp;
        }

        public int getFlag() {
            return flag;
        }

        public int getFlen() {
            return flen;
        }

        public int getDecimal() {
            return decimal;
        }

        public String getCharset() {
            return charset;
        }

        public String getCollate() {
            return collate;
        }

        public List<String> getElems() {
            return elems;
        }

        public FieldType toFieldType() {
            Builder<? extends FieldType> builder = typeBuilder.get(getTp());
            if (builder == null) {
                throw new TiClientInternalException("Invalid Field Type code: " + getTp());
            }
            return builder.build(this);
        }
    }

    public ColumnInfo toProto() {
        return toProtoBuilder().build();
    }

    public ColumnInfo.Builder toProtoBuilder() {
        return ColumnInfo.newBuilder()
                .setColumnId(id)
                .setTp(type.getTypeCode())
                .setCollation(type.getCollationCode())
                .setColumnLen(type.getLength())
                .setDecimal(type.getDecimal())
                .setFlag(type.getFlag())
                .setColumnLen(type.getLength())
                .setPkHandle(isPrimaryKey())
                .addAllElems(type.getElems());
    }
}
