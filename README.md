## Ti-Client in Java [Under Construction]

NOTES: For now it works with only TiKV branch disksing/grpc instead of official release.

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
        TiDBInfo db = cat.getDatabase("test");
        TiTableInfo table = cat.getTable(db, "t2");

....

        Snapshot snapshot = cluster.createSnapshot();
        Iterator<Row> it = snapshot.newSelect(table)
                .addRange(TiRange.create(0L, Long.MAX_VALUE))
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
