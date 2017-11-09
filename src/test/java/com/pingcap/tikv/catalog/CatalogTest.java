package com.pingcap.tikv.catalog;

import com.google.common.collect.ImmutableList;
import com.pingcap.tikv.KVMockServer;
import com.pingcap.tikv.PDMockServer;
import com.pingcap.tikv.TiCluster;
import com.pingcap.tikv.TiConfiguration;
import com.pingcap.tikv.kvproto.Kvrpcpb.IsolationLevel;
import com.pingcap.tikv.meta.MetaUtils.MetaMockHelper;
import com.pingcap.tikv.meta.TiDBInfo;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.region.TiRegion;
import com.pingcap.tikv.util.ReflectionWrapper;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;


public class CatalogTest {
  private KVMockServer kvServer;
  private PDMockServer pdServer;
  private static final long CLUSTER_ID = 1024;
  private TiConfiguration conf;

  @Before
  public void setUp() throws Exception {
    pdServer = new PDMockServer();
    pdServer.start(CLUSTER_ID);
    kvServer = new KVMockServer();
    kvServer.start(new TiRegion(MetaMockHelper.region, MetaMockHelper.region.getPeers(0), IsolationLevel.RC));
    // No PD needed in this test
    conf = TiConfiguration.createDefault(ImmutableList.of("127.0.0.1:" + pdServer.port));
  }

  @Test
  public void listDatabases() throws Exception {
    MetaMockHelper helper = new MetaMockHelper(pdServer, kvServer);
    helper.preparePDForRegionRead();
    helper.setSchemaVersion(666);

    helper.addDatabase(130, "global_temp");
    helper.addDatabase(264, "TPCH_001");

    TiCluster cluster = TiCluster.getCluster(conf);
    Catalog cat = cluster.getCatalog();
    List<TiDBInfo> dbs = cat.listDatabases();
    List<String> names = dbs.stream().map(db -> db.getName()).sorted().collect(Collectors.toList());
    assertEquals(2, dbs.size());
    assertEquals("TPCH_001", names.get(0));
    assertEquals("global_temp", names.get(1));

    helper.addDatabase(265, "other");
    helper.setSchemaVersion(667);

    ReflectionWrapper wrapper = new ReflectionWrapper(cat);
    wrapper.call("reloadCache");

    dbs = cat.listDatabases();
    assertEquals(3, dbs.size());
    names = dbs.stream().map(db -> db.getName()).sorted().collect(Collectors.toList());
    assertEquals("TPCH_001", names.get(0));
    assertEquals("global_temp", names.get(1));
    assertEquals("other", names.get(2));

    assertEquals(130, cat.getDatabase("global_temp").getId());
    assertEquals(null, cat.getDatabase("global_temp111"));
  }

  @Test
  public void listTables() throws Exception {
    MetaMockHelper helper = new MetaMockHelper(pdServer, kvServer);
    helper.preparePDForRegionRead();
    helper.setSchemaVersion(666);

    helper.addDatabase(130, "global_temp");
    helper.addDatabase(264, "TPCH_001");

    helper.addTable(130, 42, "test");
    helper.addTable(130, 43, "tEst1");

    TiCluster cluster = TiCluster.getCluster(conf);
    Catalog cat = cluster.getCatalog();
    TiDBInfo db = cat.getDatabase("global_temp");
    List<TiTableInfo> tables = cat.listTables(db);
    List<String> names = tables.stream().map(table -> table.getName()).sorted().collect(Collectors.toList());
    assertEquals(2, tables.size());
    assertEquals("tEst1", names.get(0));
    assertEquals("test", names.get(1));

    assertEquals("test", cat.getTable(db, 42).getName());
    assertEquals("tEst1", cat.getTable(db, 43).getName());
    assertEquals(null, cat.getTable(db, 44));

    helper.addTable(130, 44, "other");
    helper.setSchemaVersion(667);

    ReflectionWrapper wrapper = new ReflectionWrapper(cat);
    wrapper.call("reloadCache");

    tables = cat.listTables(db);
    names = tables.stream().map(table -> table.getName()).sorted().collect(Collectors.toList());
    assertEquals(3, tables.size());
    assertEquals("other", names.get(0));
    assertEquals("tEst1", names.get(1));
    assertEquals("test", names.get(2));

    db = cat.getDatabase("TPCH_001");
    tables = cat.listTables(db);
    assertEquals(null, tables);

    helper.dropDatabase(db.getId());
    wrapper.call("reloadCache");
    tables = cat.listTables(db);
    assertEquals(null, tables);

    assertEquals(42, cat.getTable("global_temp", "test").getId());
    assertEquals(null, cat.getTable("global_temp", "test111"));
  }
}