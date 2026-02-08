include(FetchContent)
include(FetchGTest)

FetchContent_Declare(
  benchmark
  GIT_REPOSITORY https://github.com/google/benchmark.git
  GIT_TAG 12235e24652fc7f809373e7c11a5f73c5763fc4c
)

FetchContent_GetProperties(benchmark)
if (NOT benchmark_POPULATED)
  FetchContent_Populate(benchmark)
  add_subdirectory(${benchmark_SOURCE_DIR} ${benchmark_BINARY_DIR}
                   EXCLUDE_FROM_ALL)
endif ()
