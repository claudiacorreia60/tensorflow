# Description:
# Prisma file system implementation.

package(default_visibility = ["//visibility:public"])

licenses(["notice"])  # Apache 2.0

exports_files(["LICENSE"])

load(
    "//tensorflow:tensorflow.bzl",
    "tf_cc_binary",
)

tf_cc_binary(
    name = "libprisma_file_system_shared.so",
    srcs = [
        "prisma_file_system.cc",
        "prisma_file_system.h",
    ],
    linkshared = 1,
    deps = [
        "//tensorflow/core:framework_headers_lib",
  	"//third_party/prisma:prisma",
    ]
)

cc_library(
    name = "prisma_file_system",
    srcs = [
        "prisma_file_system.cc",
    ],
    hdrs = [
        "prisma_file_system.h",
    ],
    deps = [
        "//tensorflow/core:lib",
        "//tensorflow/core:lib_internal",
  	"//third_party/prisma:prisma", 
    ]
)
