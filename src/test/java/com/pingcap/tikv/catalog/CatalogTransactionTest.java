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

import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.pingcap.tikv.GrpcUtils;
import com.pingcap.tikv.KVMockServer;
import com.pingcap.tikv.PDMockServer;
import com.pingcap.tikv.TiCluster;
import com.pingcap.tikv.TiConfiguration;
import com.pingcap.tikv.TiSession;
import com.pingcap.tikv.codec.CodecDataOutput;
import com.pingcap.tikv.kvproto.Kvrpcpb.IsolationLevel;
import com.pingcap.tikv.kvproto.Metapb;
import com.pingcap.tikv.meta.TiDBInfo;
import com.pingcap.tikv.region.TiRegion;
import com.pingcap.tikv.types.BytesType;
import com.pingcap.tikv.types.IntegerType;
import java.util.List;
import org.junit.Before;
import org.junit.Test;


public class CatalogTransactionTest {
  private KVMockServer kvServer;
  private PDMockServer pdServer;
  private static final String LOCAL_ADDR = "127.0.0.1";
  private static final long CLUSTER_ID = 1024;
  private int port;
  private TiSession session;
  private TiRegion region;

  @Before
  public void setUp() throws Exception {
    Metapb.Region r =
        Metapb.Region.newBuilder()
            .setRegionEpoch(Metapb.RegionEpoch.newBuilder().setConfVer(1).setVersion(1))
            .setId(1)
            .setStartKey(ByteString.EMPTY)
            .setEndKey(ByteString.EMPTY)
            .addPeers(Metapb.Peer.newBuilder().setId(1).setStoreId(1))
            .build();

    region = new TiRegion(r, r.getPeers(0), IsolationLevel.RC);
    pdServer = new PDMockServer();
    pdServer.start(CLUSTER_ID);
    kvServer = new KVMockServer();
    port = kvServer.start(region);
    // No PD needed in this test
    TiConfiguration conf = TiConfiguration.createDefault(ImmutableList.of(""));
    session = TiSession.create(conf);
  }

  private void addPDRegionResponse() {
    pdServer.addGetRegionResp(
        GrpcUtils.makeGetRegionResponse(
            pdServer.getClusterId(),
            region.getRawRegion()));
  }

  private void addPDMemberResponse() {
    pdServer.addGetMemberResp(
        GrpcUtils.makeGetMembersResponse(
            pdServer.getClusterId(),
            GrpcUtils.makeMember(1, "http://" + LOCAL_ADDR + ":" + pdServer.port)));
  }

  private void addPDStoreResponse() {
    pdServer.addGetStoreResp(
        GrpcUtils.makeGetStoreResponse(
            pdServer.getClusterId(),
            GrpcUtils.makeStore(
                1,
                LOCAL_ADDR + ":" + kvServer.getPort(),
                Metapb.StoreState.Up)));
  }

  private void setupPDResponse() {
    addPDMemberResponse();
    addPDRegionResponse();
    addPDStoreResponse();
  }

  private ByteString getSchemaVersionKey() {
    CodecDataOutput cdo = new CodecDataOutput();
    cdo.write(new byte[] {'m'});
    BytesType.writeBytes(cdo, "SchemaVersionKey".getBytes());
    IntegerType.writeULong(cdo, 's');
    return cdo.toByteString();
  }

  @Test
  public void getLatestSchemaVersion() throws Exception {
    setupPDResponse();
    CodecDataOutput cdo = new CodecDataOutput();
    cdo.write(new byte[] {'m'});
    BytesType.writeBytes(cdo, "SchemaVersionKey".getBytes());
    IntegerType.writeULong(cdo, 's');
    kvServer.put(getSchemaVersionKey(), ByteString.copyFromUtf8("666"));
    TiConfiguration conf =
        TiConfiguration.createDefault(ImmutableList.of("127.0.0.1:" + pdServer.port));
    TiCluster cluster = TiCluster.getCluster(conf);
    CatalogTransaction trx = new CatalogTransaction(cluster.createSnapshot());
    assertEquals(666, trx.getLatestSchemaVersion());
  }

  private ByteString getDBKey(String postfix) {
    CodecDataOutput cdo = new CodecDataOutput();
    cdo.write(new byte[] {'m'});
    BytesType.writeBytes(cdo, "DBs".getBytes());
    IntegerType.writeULong(cdo, 'h');
    BytesType.writeBytes(cdo, postfix.getBytes());
    return cdo.toByteString();
  }

  @Test
  public void getDatabases() throws Exception {
    setupPDResponse();
    kvServer.put(getDBKey("DB:130"),
        ByteString.copyFromUtf8("{\n"
            + " \"id\":130,\n"
            + " \"db_name\":{\"O\":\"global_temp\",\"L\":\"global_temp\"},\n"
            + " \"charset\":\"utf8\",\"collate\":\"utf8_bin\",\"state\":5\n"
            + "}")
    );

    kvServer.put(getDBKey("DB:264"),
        ByteString.copyFromUtf8( "{\n"
            + "\"id\":264,\n"
            + "\"db_name\":{\"O\":\"TPCH_001\",\"L\":\"tpch_001\"},\n"
            + "\"charset\":\"utf8\",\"collate\":\"utf8_bin\",\"state\":5\n"
            + "}")
    );

    TiConfiguration conf =
        TiConfiguration.createDefault(ImmutableList.of("127.0.0.1:" + pdServer.port));
    TiCluster cluster = TiCluster.getCluster(conf);
    CatalogTransaction trx = new CatalogTransaction(cluster.createSnapshot());
    List<TiDBInfo> dbs = trx.getDatabases();
    assertEquals(2, dbs.size());
    assertEquals(130, dbs.get(0).getId());
    assertEquals("global_temp", dbs.get(0).getName());

    assertEquals(264, dbs.get(1).getId());
    assertEquals("TPCH_001", dbs.get(1).getName());
  }
}