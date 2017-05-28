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
        "//src/main/java/com/pingcap/tikv:tikv-java-client-lib",
        "//src/main/proto:java",
        "//src/main/proto:java_compile_imports",
        "@com_fasterxml_jackson_core_jackson_annotations//jar",
        "@com_fasterxml_jackson_core_jackson_core//jar",
        "@com_fasterxml_jackson_core_jackson_databind//jar",
        "@io_grpc_grpc_netty//jar",
        "@org_pubref_rules_protobuf//java:grpc_compiletime_deps",
        "@org_pubref_rules_protobuf//java:netty_runtime_deps",
        "@junit_junit//jar",
    ],
)
