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

## License
Apache 2.0 license. See the [LICENSE](./LICENSE) file for details.