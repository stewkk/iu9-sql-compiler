include(FetchContent)

set(ANTLR_BUILD_STATIC ON)
set(ANTLR_BUILD_SHARED OFF)
set(DISABLE_WARNINGS ON)
set(ANTLR_BUILD_CPP_TESTS OFF)
set(ANTLR_EXECUTABLE $ENV{ANTLR_JAR})

FetchContent_Declare(
  antlr4
  GIT_REPOSITORY https://github.com/antlr/antlr4.git
  GIT_TAG cc82115a4e7f53d71d9d905caa2c2dfa4da58899
  SOURCE_SUBDIR  runtime/Cpp
)

FetchContent_MakeAvailable(antlr4)
include(${antlr4_SOURCE_DIR}/runtime/Cpp/cmake/FindANTLR.cmake)
