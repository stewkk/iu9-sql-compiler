include(FetchGTest)
include(GoogleTest)

add_executable(unittests)
target_sources(unittests PRIVATE
  ${PROJECT_SOURCE_DIR}/src/stewkk/sql/logic/parser/parser_test.cpp
  ${PROJECT_SOURCE_DIR}/src/stewkk/sql/logic/executor/executor_test.cpp
)
target_compile_features(unittests PRIVATE cxx_std_23)
set_target_properties(unittests PROPERTIES
  CXX_STANDART 23
  CXX_STANDART_REQUIRED YES
  CXX_EXTENSIONS YES
)
target_include_directories(
  unittests PUBLIC $<INSTALL_INTERFACE:include>
                    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
                    ${ANTLR4_INCLUDE_DIRS}
                    ${LLVM_INCLUDE_DIRS}
)
target_compile_options(unittests PRIVATE ${BASE_COMPILE_FLAGS})
target_link_options(unittests PRIVATE ${BASE_COMPILE_FLAGS})
target_link_libraries(unittests PRIVATE gmock_main libsql antlr4_static
  Boost::asio
  Boost::thread
  Boost::filesystem
)
gtest_discover_tests(unittests)
