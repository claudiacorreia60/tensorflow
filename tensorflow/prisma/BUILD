# Description:
# DLCaching library implementation.

package(default_visibility = ["//visibility:public"])

licenses(["notice"])  # Apache 2.0

exports_files(["LICENSE"])

cc_library(
	name = "prisma",
	srcs = ["prisma.cpp",
		"file_content.cpp",
		"file_info.cpp",
		"configs.cpp",
		"autotuner.cpp",
		"logger.cpp",
		"profiler.cpp",
	],
	hdrs = ["prisma.h",
		"file_content.h",
		"file_info.h",
		"configs.h",
		"autotuner.h",
		"logger.h",
		"profiler.h",
	],
	deps = ["//third_party/tbb:tbb",
		"//third_party/ctpl:ctpl",
	]
)
