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

package com.pingcap.tikv.catalog;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.pingcap.tikv.Snapshot;
import com.pingcap.tikv.exception.TiClientInternalException;
import com.pingcap.tikv.codec.*;
import com.pingcap.tikv.util.Pair;
import com.pingcap.tikv.util.TiFluentIterable;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;


public class CatalogTransaction {
    private final Snapshot      snapshot;
    private final byte[]        prefix;

    private static final byte[]  META_PREFIX = new byte[] {'m'};

    private static final byte HASH_META_FLAG = 'H';
    private static final byte HASH_DATA_FLAG = 'h';
    private static final byte STR_META_FLAG = 'S';
    private static final byte STR_DATA_FLAG = 's';

    public CatalogTransaction(Snapshot snapshot) {
        this.snapshot = snapshot;
        this.prefix = META_PREFIX;
    }

    private Snapshot getSnapshot() {
        return snapshot;
    }

    public byte[] getPrefix() {
        return prefix;
    }

    private static void encodeStringDataKey(CodecDataOutput cdo, byte[] prefix, byte[] key) {
        cdo.write(prefix);
        BytesUtils.writeBytes(cdo, key);
        LongUtils.writeULong(cdo, STR_DATA_FLAG);
    }

    private static void encodeHashDataKey(CodecDataOutput cdo, byte[] prefix, byte[] key, byte[] field) {
        encodeHashDataKeyPrefix(cdo, prefix, key);
        BytesUtils.writeBytes(cdo, field);
    }

    private static void encodeHashDataKeyPrefix(CodecDataOutput cdo, byte[] prefix, byte[] key) {
        cdo.write(prefix);
        BytesUtils.writeBytes(cdo, key);
        LongUtils.writeULong(cdo, HASH_DATA_FLAG);
    }

    private Pair<ByteString, ByteString> decodeHashDataKey(ByteString rawKey) {
       checkArgument(KeyUtils.hasPrefix(rawKey, ByteString.copyFrom(prefix)),
                    "invalid encoded hash data key prefix: " + new String(prefix));
        CodecDataInput cdi = new CodecDataInput(rawKey.toByteArray());
        cdi.skipBytes(prefix.length);
        byte[] key = BytesUtils.readBytes(cdi);
        long typeFlag = LongUtils.readULong(cdi);
        if (typeFlag != HASH_DATA_FLAG) {
            throw new TiClientInternalException("Invalid hash data flag: " + typeFlag);
        }
        byte[] field = BytesUtils.readBytes(cdi);
        return Pair.create(ByteString.copyFrom(key), ByteString.copyFrom(field));
    }

    public ByteString hashGet(ByteString key, ByteString field) {
        CodecDataOutput cdo = new CodecDataOutput();
        encodeHashDataKey(cdo, prefix, key.toByteArray(), field.toByteArray());
        return snapshot.get(cdo.toByteString());
    }

    public List<Pair<ByteString, ByteString>> hashGetFields(ByteString key) {
        CodecDataOutput cdo = new CodecDataOutput();
        encodeHashDataKeyPrefix(cdo, prefix, key.toByteArray());
        ByteString encodedKey = cdo.toByteString();

        Iterable<Pair<ByteString, ByteString>> iter =
                TiFluentIterable.from(snapshot.scan(encodedKey))
                                .stopWhen(kv -> !KeyUtils.hasPrefix(kv.getKey(), encodedKey))
                                .transform(kv -> Pair.create(decodeHashDataKey(kv.getKey()).second, kv.getValue()));

        return ImmutableList.copyOf(iter);
    }
}
