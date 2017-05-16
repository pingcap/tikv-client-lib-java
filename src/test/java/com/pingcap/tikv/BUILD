package(default_visibility = ["//visibility:public"])

filegroup(
    name = "srcs",
    srcs = glob(["**/*.java"]),
    visibility = ["//src:__pkg__"],
)

load(":rule.bzl", "junit_suite_test")

junit_suite_test(
    name = "AllTests",
    srcs = glob(
         ["**/*.java"],
    ),
    deps = [
        "//src/main/java/com/pingcap/tikv:tikv-java-lib",
        "@junit_junit//jar",
        "//src/main/proto:java_compile_imports",
        "//src/main/proto:java",
        "@com_fasterxml_jackson_core_jackson_annotations//jar",
        "@com_fasterxml_jackson_core_jackson_core//jar",
        "@com_fasterxml_jackson_core_jackson_databind//jar",
         "@io_grpc_grpc_netty//jar",
         "@io_netty_netty_transport//jar",
         "@io_netty_netty_common//jar",
         "@io_netty_netty_codec_http2//jar",
         "@io_netty_netty_codec_http//jar",
         "@io_netty_netty_codec//jar",
         "@io_netty_netty_handler//jar",
         "@io_netty_netty_buffer//jar",
         "@io_netty_netty_resolver//jar",
         "@io_netty_netty_codec_socks//jar",
    ],
)