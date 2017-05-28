package(default_visibility = ["//visibility:public"])

filegroup(
    name = "srcs",
    srcs = glob(["**"]) + [
        "//src/main/java/com/pingcap/tikv:srcs",
        "//src/main/resources:srcs",
        "//src/test/java/com/pingcap/tikv:srcs",
    ],
)

java_binary(
    name = "tikv-java-client",
    main_class = "com.pingcap.tikv.Main",
    runtime_deps = [
        "//src/main/java/com/pingcap/tikv:tikv-java-client-lib",
    ],
)
