## Ti-Client in Java [Under Construction]

A Java client for [TiDB](https://github.com/pingcap/tidb)/[TiKV](https://github.com/pingcap/tikv). 
It suppose to:
+ Communicate via [gRPC](http://www.grpc.io/)
+ Talk to Placement Driver searching for a region
+ Talk to TiKV reading / writing data the format/encoding as a real TiDB.
+ Talk Coprocessor for calculation pushdown

## How to build

```
mvn package
```

## How to use for now
Since it's not quite complete, a usage sample for now can be given is:
```java
	TiConfiguration conf = TiConfiguration.createDefault(ImmutableList.of("127.0.0.1:" + 2379));
        TiCluster cluster = TiCluster.getCluster(conf);
        Catalog cat = cluster.getCatalog();
        List<TiDBInfo> dbs = cat.listDatabases();
        for (TiDBInfo db : dbs) {
            System.out.println(db.getName());
            List<TiTableInfo> tables = cat.listTables(db);
            for (TiTableInfo table : tables) {
                System.out.println("Table:" + table.getName());
            }
        }

....

        Snapshot snapshot = cluster.createSnapshot();
        Iterator<Row> it = snapshot.newSelect()
                .addRange(TiRange.create(0L, Long.MAX_VALUE))
                .setTable(table)
                .doSelect();

        while (it.hasNext()) {
            Row r = it.next();
            long val = r.getLong(0);
        }
```

## TODO
Contributions are welcomed. Here is a [TODO](https://github.com/pingcap/tikv-client-java/wiki/TODO-Lists) and you might contact maxiaoyu@pingcap.com if needed.

## License
Apache 2.0 license. See the [LICENSE](./LICENSE) file for details.
