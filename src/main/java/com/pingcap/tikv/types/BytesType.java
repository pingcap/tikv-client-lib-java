/*
 *
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
 *
 */

package com.pingcap.tikv.types;

import com.pingcap.tikv.codec.Codec.BytesCodec;
import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.codec.CodecDataOutput;
import com.pingcap.tikv.codec.InvalidCodecFormatException;
import com.pingcap.tikv.meta.TiColumnInfo;

public class BytesType extends DataType {

  static BytesType of(int tp) {
    return new BytesType(tp);
  }

  protected BytesType(int tp) {
    super(tp);
  }

  protected BytesType(TiColumnInfo.InternalTypeHolder holder) {
    super(holder);
  }

  @Override
  protected Object decodeNotNull(int flag, CodecDataInput cdi) {
    if (flag == COMPACT_BYTES_FLAG) {
      return new String(BytesCodec.readCompactBytes(cdi));
    } else if (flag == BYTES_FLAG) {
      return new String(BytesCodec.readBytes(cdi));
    } else {
      throw new InvalidCodecFormatException("Invalid Flag type for : " + flag);
    }
  }

  /**
   * encode value to cdo per type. If key, then it is memory comparable. If value, no guarantee.
   *  @param cdo destination of data.
   * @param encodeType Key or Value.
   * @param value need to be encoded.
   */
  @Override
  protected void encodeNotNull(CodecDataOutput cdo, EncodeType encodeType, Object value) {
    byte[] bytes;
    if (value instanceof String) {
      bytes = ((String) value).getBytes();
    } else {
      throw new UnsupportedOperationException("can not cast non-String type to String");
    }
    if (encodeType == EncodeType.KEY) {
      cdo.write(BYTES_FLAG);
      BytesCodec.writeBytes(cdo, bytes);
    } else {
      cdo.write(COMPACT_BYTES_FLAG);
      BytesCodec.writeCompactBytes(cdo, bytes);
    }
  }
}
