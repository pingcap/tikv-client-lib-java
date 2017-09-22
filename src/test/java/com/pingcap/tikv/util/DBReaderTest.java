package com.pingcap.tikv.util;

import com.google.common.collect.ImmutableList;
import com.pingcap.tikv.*;
import com.pingcap.tikv.catalog.Catalog;
import com.pingcap.tikv.kvproto.Kvrpcpb;
import com.pingcap.tikv.meta.MetaUtils;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.region.RegionManager;
import com.pingcap.tikv.region.TiRegion;
import com.pingcap.tikv.types.DataTypeFactory;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Created by birdstorm on 2017/9/7.
 *
 */
public class DBReaderTest {
  private KVMockServer kvServer;
  private PDMockServer pdServer;
  private static final long CLUSTER_ID = 1024;
  private TiConfiguration conf;

  @Before
  public void setUp() throws Exception {
    pdServer = new PDMockServer();
    pdServer.start(CLUSTER_ID);
    kvServer = new KVMockServer();
    kvServer.start(new TiRegion(MetaUtils.MetaMockHelper.region, MetaUtils.MetaMockHelper.region.getPeers(0), Kvrpcpb.IsolationLevel.RC));
    // No PD needed in this test
    conf = TiConfiguration.createDefault(ImmutableList.of("127.0.0.1:" + pdServer.port));
  }

  @Test
  public void testGetSelectedRows() throws Exception {
    MetaUtils.MetaMockHelper helper = new MetaUtils.MetaMockHelper(pdServer, kvServer);
    helper.preparePDForRegionRead();
    helper.setSchemaVersion(668);

    helper.addDatabase(130, "mysql");
    helper.addTable(130, 42, "stats_histograms");
    helper.addTable(130, 43, "stats_bucket");
    helper.addTable(130, 44, "stats_meta");
    helper.addTable(130, 45, "t1");

    TiCluster cluster = TiCluster.getCluster(conf);
    Catalog cat = cluster.getCatalog();

    ReflectionWrapper wrapper = new ReflectionWrapper(cat);
    wrapper.call("reloadCache");

    Snapshot snapshot = cluster.createSnapshot();
    RegionManager manager = cluster.getRegionManager();
    DBReader dbReader = new DBReader(cat, "mysql", snapshot, manager, conf);

    TiTableInfo histogramInfo = dbReader.getTableInfo("stats_histograms");
    assertEquals(42, histogramInfo.getId());
    assertEquals(DataTypeFactory.of(3).getClass(), histogramInfo.getColumns().get(0).getType().getClass());
    assertEquals(5, histogramInfo.getColumns().get(0).getSchemaState().getStateCode());
  }

}
