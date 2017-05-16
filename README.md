## Ti-Client in Java [Under Construction]

NOTES: For now it works with only TiKV branch disksing/grpc instead of official release.

A Java client for [TiDB](https://github.com/pingcap/tidb)/[TiKV](https://github.com/pingcap/tikv). 
It suppose to:
+ Communicate via [gRPC](http://www.grpc.io/)
+ Talk to Placement Driver searching for a region
+ Talk to TiKV reading / writing data the format/encoding as a real TiDB.
+ Talk Coprocessor for calculation pushdown

## How to build

The following command can install dependencies for you.
```
mvn package
```

Alternatively, you can use bazel for much faster build.
```
bazel build //src/...
bazel run //src/main/java/com/pingcap/tikv:tikv-runner
bazel test //src/...
```
this project is designed to hook with `pd` and `tikv` which you can find in `PingCap` github page.
For the sake of saving your and our time, we submoudle these already. The following command can download them.
```
git submodule update --init --recursive
```

When you work with this project, you have to communicate with `pd` and `tikv`. There is a script taking care of this. By executing the following commands, `pd` and `tikv` can be executed on background.
```
cd scripts
make pd
make tikv
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
