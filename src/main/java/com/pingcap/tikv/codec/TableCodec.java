package com.pingcap.tikv.codec;

import com.google.protobuf.ByteString;
import com.pingcap.tikv.util.Pair;

import java.io.IOException;

import static com.google.common.base.Preconditions.checkArgument;

// Basically all protobuf ByteString involves buffer copy
// and might not be a good choice inside codec so we choose to
// return byte directly for future manipulation
// But this is not quite clean in case talking with GRPC interfaces
public class TableCodec {
    public static final int ID_LEN = 8;
    public static final int PREFIX_LEN = 1 + ID_LEN;
    public static final int RECORD_ROW_KEY_LEN = PREFIX_LEN + ID_LEN;

    public static final byte [] TBL_PREFIX      = new byte[] {'t'};
    public static final byte [] REC_PREFIX_SEP  = new byte[] {'_', 'r'};
    public static final byte [] IDX_PREFIX_SEP  = new byte[] {'_', 'i'};

    private static final long SIGN_MASK = ~Long.MAX_VALUE;

    public static void writeRowKey(CodecDataOutput cdo, long tableId, byte[] encodeHandle) {
        appendTableRecordPrefix(cdo, tableId);
        cdo.write(encodeHandle);
    }

    private static void appendTableRecordPrefix(CodecDataOutput cdo, long tableId) {
        cdo.write(TBL_PREFIX);
        LongUtils.writeLong(cdo, tableId);
        cdo.write(REC_PREFIX_SEP);
    }
    // encodeRowKeyWithHandle encodes the table id, row handle into a bytes buffer/array
    public static ByteString encodeRowKeyWithHandle(long tableId, long handle) {
        CodecDataOutput cdo = new CodecDataOutput();
        appendTableRecordPrefix(cdo, tableId);
        LongUtils.writeLong(cdo, handle);
        return cdo.toByteString();
    }

    public static void writeRowKeyWithHandle(CodecDataOutput cdo, long tableId, long handle) {
        appendTableRecordPrefix(cdo, tableId);
        LongUtils.writeLong(cdo, handle);
    }

    public static void writeRecordKey(CodecDataOutput cdo, byte[] recordPrefix, long handle) {
        cdo.write(recordPrefix);
        LongUtils.writeLong(cdo, handle);
    }

    public static Pair<Long, Long> readRecordKey(CodecDataInput cdi) {
        if (!consumeAndMatching(cdi, TBL_PREFIX)) {
            throw new CodecException("Invalid Table Prefix");
        }
        long tableId = LongUtils.readLong(cdi);

        if (!consumeAndMatching(cdi, REC_PREFIX_SEP)) {
            throw new CodecException("Invalid Record Prefix");
        }

        long handle = LongUtils.readLong(cdi);
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
