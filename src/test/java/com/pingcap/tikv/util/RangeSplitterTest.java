package com.pingcap.tikv.util;

import static com.pingcap.tikv.GrpcUtils.encodeKey;
import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.pingcap.tikv.codec.CodecDataOutput;
import com.pingcap.tikv.kvproto.Coprocessor.KeyRange;
import com.pingcap.tikv.kvproto.Kvrpcpb.IsolationLevel;
import com.pingcap.tikv.kvproto.Metapb;
import com.pingcap.tikv.kvproto.Metapb.Peer;
import com.pingcap.tikv.region.RegionManager;
import com.pingcap.tikv.region.TiRegion;
import com.pingcap.tikv.types.IntegerType;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.Test;

public class RangeSplitterTest {
  static class MockRegionManager extends RegionManager {
    private List<KeyRange> regionRanges =
        ImmutableList.of(keyRange(null, 30L), keyRange(30L, 50L), keyRange(50L, null));

    private Map<KeyRange, TiRegion> mockRegionMap =
        regionRanges
            .stream()
            .collect(Collectors.toMap(kr -> kr, kr -> region(regionRanges.indexOf(kr), kr)));

    public MockRegionManager() {
      super(null);
    }

    @Override
    public Pair<TiRegion, Metapb.Store> getRegionStorePairByKey(ByteString key) {
      for (Map.Entry<KeyRange, TiRegion> entry : mockRegionMap.entrySet()) {
        if (KeyRangeUtils.toRange(entry.getKey()).contains(Comparables.wrap(key))) {
          TiRegion region = entry.getValue();
          return Pair.create(region, Metapb.Store.newBuilder().setId(region.getId()).build());
        }
      }
      return null;
    }
  }

  private static KeyRange keyRange(Long s, Long e) {
    ByteString sKey = ByteString.EMPTY;
    ByteString eKey = ByteString.EMPTY;
    if (s != null) {
      CodecDataOutput cdo = new CodecDataOutput();
      IntegerType.writeLongFull(cdo, s, true);
      sKey = cdo.toByteString();
    }

    if (e != null) {
      CodecDataOutput cdo = new CodecDataOutput();
      IntegerType.writeLongFull(cdo, e, true);
      eKey = cdo.toByteString();
    }

    return KeyRange.newBuilder().setStart(sKey).setEnd(eKey).build();
  }

  private static TiRegion region(long id, KeyRange range) {
    return new TiRegion(
        Metapb.Region.newBuilder()
            .setId(id)
            .setStartKey(encodeKey(range.getStart().toByteArray()))
            .setEndKey(encodeKey(range.getEnd().toByteArray()))
            .addPeers(Peer.getDefaultInstance())
            .build(),
        null, IsolationLevel.RC);
  }

  private MockRegionManager mgr = new MockRegionManager();

  @Test
  public void splitRangeByRegionTest() throws Exception {
    RangeSplitter s = RangeSplitter.newSplitter(mgr);
    List<RangeSplitter.RegionTask> tasks =
        s.splitRangeByRegion(
            ImmutableList.of(
                keyRange(null, 40L), keyRange(41L, 42L), keyRange(45L, 50L), keyRange(70L, 1000L)));

    assertEquals(tasks.get(0).getRegion().getId(), 0);
    assertEquals(tasks.get(0).getRanges().size(), 1);
    assertEquals(tasks.get(0).getRanges().get(0), keyRange(null, 30L));

    assertEquals(tasks.get(1).getRegion().getId(), 1);
    assertEquals(tasks.get(1).getRanges().get(0), keyRange(30L, 40L));
    assertEquals(tasks.get(1).getRanges().get(1), keyRange(41L, 42L));
    assertEquals(tasks.get(1).getRanges().get(2), keyRange(45L, 50L));
    assertEquals(tasks.get(1).getRanges().size(), 3);

    assertEquals(tasks.get(2).getRegion().getId(), 2);
    assertEquals(tasks.get(2).getRanges().size(), 1);
    assertEquals(tasks.get(2).getRanges().get(0), keyRange(70L, 1000L));
  }
}
