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

import static com.google.common.base.Preconditions.checkArgument;

import com.google.protobuf.ByteString;
import com.pingcap.tikv.Snapshot;
import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.codec.CodecDataOutput;
import com.pingcap.tikv.codec.KeyUtils;
import com.pingcap.tikv.exception.TiClientInternalException;
import com.pingcap.tikv.kvproto.Kvrpcpb;
import com.pingcap.tikv.types.BytesType;
import com.pingcap.tikv.types.IntegerType;
import com.pingcap.tikv.util.Pair;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class CatalogTransaction {
  private final Snapshot snapshot;
  private final byte[] prefix;

  private static final byte[] META_PREFIX = new byte[] {'m'};

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
    BytesType.writeBytes(cdo, key);
    IntegerType.writeULong(cdo, STR_DATA_FLAG);
  }

  private static void encodeHashDataKey(
      CodecDataOutput cdo, byte[] prefix, byte[] key, byte[] field) {
    encodeHashDataKeyPrefix(cdo, prefix, key);
    BytesType.writeBytes(cdo, field);
  }

  private static void encodeHashDataKeyPrefix(CodecDataOutput cdo, byte[] prefix, byte[] key) {
    cdo.write(prefix);
    BytesType.writeBytes(cdo, key);
    IntegerType.writeULong(cdo, HASH_DATA_FLAG);
  }

  private Pair<ByteString, ByteString> decodeHashDataKey(ByteString rawKey) {
    checkArgument(
        KeyUtils.hasPrefix(rawKey, ByteString.copyFrom(prefix)),
        "invalid encoded hash data key prefix: " + new String(prefix));
    CodecDataInput cdi = new CodecDataInput(rawKey.toByteArray());
    cdi.skipBytes(prefix.length);
    byte[] key = BytesType.readBytes(cdi);
    long typeFlag = IntegerType.readULong(cdi);
    if (typeFlag != HASH_DATA_FLAG) {
      throw new TiClientInternalException("Invalid hash data flag: " + typeFlag);
    }
    byte[] field = BytesType.readBytes(cdi);
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

    Iterator<Kvrpcpb.KvPair> iterator = snapshot.scan(encodedKey);
    List<Pair<ByteString, ByteString>> fields = new ArrayList<>();
    while (iterator.hasNext()) {
      Kvrpcpb.KvPair kv = iterator.next();
      if (!KeyUtils.hasPrefix(kv.getKey(), encodedKey)) {
        break;
      }
      fields.add(Pair.create(decodeHashDataKey(kv.getKey()).second, kv.getValue()));
    }

    return fields;
  }
}
