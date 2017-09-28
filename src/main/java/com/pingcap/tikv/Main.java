package com.pingcap.tikv;


import com.pingcap.tikv.catalog.Catalog;
import com.pingcap.tikv.meta.TiDBInfo;
import com.pingcap.tikv.meta.TiTableInfo;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Main {

  private static TiConfiguration conf =
      TiConfiguration.createDefault("127.0.0.1:" + 2379);
  private static TiSession session = TiSession.create(conf);
  private static Snapshot snapshot = session.createSnapshot();

  public static void main(String[] args) throws Exception {
    // May need to save this reference
    TiSession session = TiSession.create(conf);
    Catalog cat = session.getCatalog();
    TiDBInfo db = cat.getDatabase("test");
    TiTableInfo table = cat.getTable(db, "t2");
    Logger log = Logger.getLogger("io.grpc");
    log.setLevel(Level.WARNING);

    cat.listDatabases();
    while (true) {
      if (table != null) {
        System.out.println("exist");
      } else {
        System.out.println("deleted");
      }
      Thread.currentThread().sleep(1000);
    }
  }
}
