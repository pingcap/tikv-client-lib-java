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

package com.pingcap.tikv.region;

import com.google.protobuf.ByteString;
import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.kvproto.Kvrpcpb;
import com.pingcap.tikv.kvproto.Metapb;
import com.pingcap.tikv.kvproto.Metapb.Peer;
import com.pingcap.tikv.kvproto.Metapb.Region;
import com.pingcap.tikv.types.BytesType;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

public class TiRegion implements Serializable {
  private final Region meta;
  private final Set<Long> unreachableStores;
  private Peer peer;

  public TiRegion(Region meta, Peer peer) {
    this.meta = decodeRegion(meta);
    this.peer = peer;
    this.unreachableStores = new HashSet<>();
  }

  private Region decodeRegion(Region region) {
    Region.Builder builder =
        Region.newBuilder()
            .setId(region.getId())
            .setRegionEpoch(region.getRegionEpoch())
            .addAllPeers(region.getPeersList());

    if (region.getStartKey().isEmpty()) {
      builder.setStartKey(region.getStartKey());
    } else {
      byte[] decodecStartKey = BytesType.readBytes(new CodecDataInput(region.getStartKey()));
      builder.setStartKey(ByteString.copyFrom(decodecStartKey));
    }

    if (region.getEndKey().isEmpty()) {
      builder.setEndKey(region.getEndKey());
    } else {
      byte[] decodecEndKey = BytesType.readBytes(new CodecDataInput(region.getEndKey()));
      builder.setEndKey(ByteString.copyFrom(decodecEndKey));
    }

    return builder.build();
  }

  public Peer getLeader() {
    return peer;
  }

  public long getId() {
    return this.meta.getId();
  }

  public ByteString getStartKey() {
    return meta.getStartKey();
  }

  public ByteString getEndKey() {
    return meta.getEndKey();
  }

  public Kvrpcpb.Context getContext() {
    Kvrpcpb.Context.Builder builder = Kvrpcpb.Context.newBuilder();
    builder.setRegionId(meta.getId()).setPeer(this.peer).setRegionEpoch(this.meta.getRegionEpoch());
    return builder.build();
  }

  /**
   * switches current peer to the one on specific store. It return false if no peer matches the
   * storeID.
   *
   * @param leaderStoreID is leader peer id.
   * @return false if no peers matches the store id.
   */
  boolean switchPeer(long leaderStoreID) {
    return meta.getPeersList()
        .stream()
        .anyMatch(
            p -> {
              if (p.getStoreId() == leaderStoreID) {
                this.peer = p;
                return true;
              }
              return false;
            });
  }

  public boolean contains(ByteString key) {
    return meta.getStartKey().equals(key)
        && (meta.getEndKey().equals(key) || meta.getEndKey().isEmpty());
  }

  public boolean isValid() {
    return peer != null && meta.hasStartKey() && meta.hasEndKey();
  }

  boolean hasStartKey() {
    return meta.hasStartKey();
  }

  boolean hasEndKey() {
    return meta.hasEndKey();
  }

  public Metapb.RegionEpoch getRegionEpoch() {
    return this.meta.getRegionEpoch();
  }

  public Region getMeta() {
    return meta;
  }

  /**
   * records unreachable peer and tries to select another valid peer. It returns false if all peers
   * are unreachable.
   *
   * @param storeID leader store ID
   * @return false if peers are unreachable.
   */
  boolean onRequestFail(long storeID) {
    if (this.peer.getStoreId() == storeID) {
      return true;
    }
    this.unreachableStores.add(storeID);
    for (Peer p : this.meta.getPeersList()) {
      if (unreachableStores.contains(p.getStoreId())) {
        continue;
      }
      this.peer = p;
      return true;
    }
    return false;
  }
}
