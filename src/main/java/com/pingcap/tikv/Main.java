package com.pingcap.tikv;

import com.google.common.collect.ImmutableList;
import com.pingcap.tikv.catalog.Catalog;
import com.pingcap.tikv.meta.Row;
import com.pingcap.tikv.meta.TiDBInfo;
import com.pingcap.tikv.meta.TiRange;
import com.pingcap.tikv.meta.TiExpr;
import com.pingcap.tidb.tipb.ExprType;
import com.pingcap.tikv.meta.TiTableInfo;

import java.util.Iterator;

public class Main {
  public static void main(String[] args) throws Exception {
    TiConfiguration conf = TiConfiguration.createDefault(ImmutableList.of("127.0.0.1:" + 2379));
    TiCluster cluster = TiCluster.getCluster(conf);
    Catalog cat = cluster.getCatalog();
    TiDBInfo db = cat.getDatabase("test");
    TiTableInfo table = cat.getTable(db, "t1");
    Snapshot snapshot = cluster.createSnapshot();
    Iterator<Row> it =
        snapshot
            .newSelect(table)
            .addRange(TiRange.create(0L, Long.MAX_VALUE))
            // .addHaving(null)
            // .groupBy(null)
            // .orderBy(null)
            // .where(null)
            // .distinct(false)
            // .setTimestamp(0)
            // .fields(null)
            .addAggreates(TiExpr.create().setType(ExprType.Sum).setValue("c1"))
            .doSelect();

    while (it.hasNext()) {
      Row r = it.next();
      String val2 = r.getString(0);
      double val1 = r.getDecimal(1);
      // String val3 = r.getString(1);
      System.out.println(val2);
      System.out.println(val1);
      // System.out.println(val3);
    }

    cluster.close();
    return;
  }
}
