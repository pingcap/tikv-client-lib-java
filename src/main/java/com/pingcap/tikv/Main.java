package com.pingcap.tikv;


import com.google.common.collect.ImmutableList;
import com.pingcap.tikv.catalog.Catalog;
import com.pingcap.tikv.expression.TiColumnRef;
import com.pingcap.tikv.expression.TiConstant;
import com.pingcap.tikv.expression.TiExpr;
import com.pingcap.tikv.expression.scalar.GreaterEqual;
import com.pingcap.tikv.expression.scalar.NotEqual;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.region.TiRegion;
import com.pingcap.tikv.statistics.Table;
import com.pingcap.tikv.statistics.TableStats;
import com.pingcap.tikv.util.DBReader;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Main {

  private static TiConfiguration conf =
      TiConfiguration.createDefault(ImmutableList.of("127.0.0.1:" + 2379));
  private static TiCluster cluster = TiCluster.getCluster(conf);
  private static Snapshot snapshot = cluster.createSnapshot();

  public static void main(String[] args) throws Exception {
    // May need to save this reference
    Logger log = Logger.getLogger("io.grpc");
    log.setLevel(Level.WARNING);
    PDClient client = PDClient.createRaw(cluster.getSession());
    for (int i = 0; i < 51; i++) {
      TiRegion r = client.getRegionByID(i);
      r.getId();
    }

    Catalog cat = cluster.getCatalog();
    DBReader dbReader = new DBReader(cat, "mysql", snapshot, cluster.getRegionManager(), conf);

    TiTableInfo table = dbReader.getTableInfo("t1");
    List<TiExpr> exprs =
        ImmutableList.of(
            new NotEqual(TiColumnRef.create("s1", table), TiConstant.create("xxxxx")));

    List<String> returnFields =
        ImmutableList.of("c1", "s1");

    dbReader.printRows("t1", exprs, returnFields);

//    System.out.println(table.getIndices().get(0).getId());

    System.out.println(table.getId());

    TableStats tableStats = new TableStats();
    tableStats.build(dbReader);

    System.out.println(table.getName() + "-->" + table.getColumns().get(0).getName());
    Table t = tableStats.tableStatsFromStorage(dbReader, table);
//    Table t = tableStats.getTableStats(table.getId());

    System.out.println();

    List<TiExpr> myExprs = ImmutableList.of(
        new GreaterEqual(TiColumnRef.create("s1", table), TiConstant.create(2)));
    System.out.println(myExprs.size());
    System.out.println(t.Selectivity(dbReader, myExprs));

    cluster.close();
    client.close();
  }

}
