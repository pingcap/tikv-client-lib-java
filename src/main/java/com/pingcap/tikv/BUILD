package(default_visibility = ["//visibility:public"])

java_binary(
    name = "tikv-runner",
    main_class = "com.pingcap.tikv.Main",
    srcs = glob(
        ["**/*.java"],
    ),
    deps = [
        "//src/main/proto:java",
        "@com_fasterxml_jackson_core_jackson_annotations//jar",
        "@com_fasterxml_jackson_core_jackson_core//jar",
        "@com_fasterxml_jackson_core_jackson_databind//jar",
        "@com_google_code_findbugs_jsr305//jar",
        "@com_google_code_gson_gson//jar",
        "@com_google_errorprone_error_prone_annotations//jar",
        "@com_google_guava_guava//jar",
        "@com_google_protobuf_protobuf_java//jar",
        "@io_grpc_grpc_context//jar",
        "@io_grpc_grpc_core//jar",
        "@io_grpc_grpc_protobuf//jar",
        "@io_grpc_grpc_protobuf_lite//jar",
        "@io_grpc_grpc_stub//jar",
        "@org_apache_logging_log4j_log4j_api//jar",
        "@org_apache_logging_log4j_log4j_core//jar",
    ],
)

java_library(
    name = "tikv-java-lib",
    srcs = glob(
        ["**/*.java"],
    ),
    deps = [
        "//src/main/proto:java",
        "@com_fasterxml_jackson_core_jackson_annotations//jar",
        "@com_fasterxml_jackson_core_jackson_core//jar",
        "@com_fasterxml_jackson_core_jackson_databind//jar",
        "@com_google_code_findbugs_jsr305//jar",
        "@com_google_code_gson_gson//jar",
        "@com_google_errorprone_error_prone_annotations//jar",
        "@com_google_guava_guava//jar",
        "@com_google_protobuf_protobuf_java//jar",
        "@io_grpc_grpc_context//jar",
        "@io_grpc_grpc_core//jar",
        "@io_grpc_grpc_protobuf//jar",
        "@io_grpc_grpc_protobuf_lite//jar",
        "@io_grpc_grpc_stub//jar",
        "@org_apache_logging_log4j_log4j_api//jar",
        "@org_apache_logging_log4j_log4j_core//jar",
    ],
)

filegroup(
    name = "srcs",
    srcs = ["BUILD"] + glob(["**/*.java"]),
)

