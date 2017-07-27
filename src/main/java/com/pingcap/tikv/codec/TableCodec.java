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

package com.pingcap.tikv.codec;

import com.google.protobuf.ByteString;
import com.pingcap.tikv.types.IntegerType;
import com.pingcap.tikv.util.Pair;

// Basically all protobuf ByteString involves buffer copy
// and might not be a good choice inside codec so we choose to
// return byte directly for future manipulation
// But this is not quite clean in case talking with GRPC interfaces
public class TableCodec {
  public static final int ID_LEN = 8;
  public static final int PREFIX_LEN = 1 + ID_LEN;
  public static final int RECORD_ROW_KEY_LEN = PREFIX_LEN + ID_LEN;

  public static final byte[] TBL_PREFIX = new byte[] {'t'};
  public static final byte[] REC_PREFIX_SEP = new byte[] {'_', 'r'};
  public static final byte[] IDX_PREFIX_SEP = new byte[] {'_', 'i'};

  public static final long SIGN_MASK = ~Long.MAX_VALUE;

  public static void writeRowKey(CodecDataOutput cdo, long tableId, byte[] encodeHandle) {
    appendTableRecordPrefix(cdo, tableId);
    cdo.write(encodeHandle);
  }

  // EncodeIndexSeekKey encodes an index value to kv.Key.
  public static void writeIndexSeekKey(
      CodecDataOutput cdo, long tableId, long indexId, byte[]... dataGroup) {
    appendTableIndexPrefix(cdo, tableId);
    IntegerType.writeLong(cdo, indexId);
    for (byte[] data : dataGroup) {
      if (data != null) {
        cdo.write(data);
      }
    }
  }

  // appendTableRecordPrefix appends table record prefix  "t[tableID]_r".
  // tablecodec.go:appendTableRecordPrefix
  private static void appendTableRecordPrefix(CodecDataOutput cdo, long tableId) {
    cdo.write(TBL_PREFIX);
    IntegerType.writeLong(cdo, tableId);
    cdo.write(REC_PREFIX_SEP);
  }

  // appendTableIndexPrefix appends table index prefix  "t[tableID]_i".
  // tablecodec.go:appendTableIndexPrefix
  private static void appendTableIndexPrefix(CodecDataOutput cdo, long tableId) {
    cdo.write(TBL_PREFIX);
    IntegerType.writeLong(cdo, tableId);
    cdo.write(IDX_PREFIX_SEP);
  }

  // encodeRowKeyWithHandle encodes the table id, row handle into a bytes buffer/array
  public static ByteString encodeRowKeyWithHandle(long tableId, long handle) {
    CodecDataOutput cdo = new CodecDataOutput();
    writeRowKeyWithHandle(cdo, tableId, handle);
    return cdo.toByteString();
  }

  public static void writeRowKeyWithHandle(CodecDataOutput cdo, long tableId, long handle) {
    appendTableRecordPrefix(cdo, tableId);
    IntegerType.writeLong(cdo, handle);
  }

  public static void writeRecordKey(CodecDataOutput cdo, byte[] recordPrefix, long handle) {
    cdo.write(recordPrefix);
    IntegerType.writeLong(cdo, handle);
  }

  public static Pair<Long, Long> readRecordKey(CodecDataInput cdi) {
    if (!consumeAndMatching(cdi, TBL_PREFIX)) {
      throw new CodecException("Invalid Table Prefix");
    }
    long tableId = IntegerType.readLong(cdi);

    if (!consumeAndMatching(cdi, REC_PREFIX_SEP)) {
      throw new CodecException("Invalid Record Prefix");
    }

    long handle = IntegerType.readLong(cdi);
    return Pair.create(tableId, handle);
  }

  public static long flipSignBit(long v) {
    return v ^ SIGN_MASK;
  }

  private static boolean consumeAndMatching(CodecDataInput cdi, byte[] prefix) {
    for (byte b : prefix) {
      if (cdi.readByte() != b) {
        return false;
      }
    }
    return true;
  }
}
