package com.pingcap.tikv.region;


import com.pingcap.tikv.kvproto.Metapb.Store;

public interface RegionErrorReceiver {
  void onNotLeader(TiRegion region, Store store);
  void onStoreNotMatch();
}
