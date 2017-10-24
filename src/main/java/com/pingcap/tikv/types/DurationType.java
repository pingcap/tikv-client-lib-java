package com.pingcap.tikv.types;

import com.pingcap.tikv.meta.TiColumnInfo;

/**
 * The type Duration type.
 * TODO:Implement the encoding and decoding
 */
public class DurationType extends TimestampType {
  /**
   * Instantiates a new Duration type.
   *
   * @param holder the holder
   */
  DurationType(TiColumnInfo.InternalTypeHolder holder) {
    super(holder);
  }
}
