package(default_visibility = ["//visibility:public"])

filegroup(
    name = "protos",
    srcs = glob([
         "*.proto",
         "gogoproto/*.proto",
    ]),
)

load("@org_pubref_rules_protobuf//java:rules.bzl", "java_proto_library")

java_proto_library(
    name = "java",
    protos = [":protos"],
    imports = [
            'external/com_github_google_protobuf/src',
            'src/main/proto',
              
    ],
    verbose = 0, # 0=no output, 1=show protoc command, 2+ more...
    inputs = ["@com_github_google_protobuf//:well_known_protos"],
)