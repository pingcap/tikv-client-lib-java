package com.pingcap.tikv;


import com.google.common.collect.ImmutableList;
import com.pingcap.tikv.catalog.Catalog;
import com.pingcap.tikv.meta.TiDBInfo;
import com.pingcap.tikv.meta.TiTableInfo;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Main {

  private static TiConfiguration conf =
      TiConfiguration.createDefault(ImmutableList.of("127.0.0.1:" + 2379));
  private static TiCluster cluster = TiCluster.getCluster(conf);

  public static void main(String[] args) throws Exception {
    // May need to save this reference
    Logger log = Logger.getLogger("io.grpc");
    log.setLevel(Level.WARNING);

    try(Catalog cat = cluster.getCatalog()) {
      cat.listDatabases();
      TiDBInfo db = cat.getDatabase("TPCH_test");
      TiTableInfo table = cat.getTable(db, "test");
      if (table != null) {
        System.out.println("exist");
      } else {
        System.out.println("deleted");
      }
    }

    cluster.close();
  }
}
