include(FetchGBenchmark)

add_executable(benchmarks main.cpp)
target_compile_features(benchmarks PRIVATE cxx_std_23)
set_target_properties(benchmarks PROPERTIES
  CXX_STANDART 23
  CXX_STANDART_REQUIRED YES
  CXX_EXTENSIONS YES
)
target_compile_options(benchmarks PRIVATE ${BASE_COMPILE_FLAGS})
target_link_options(benchmarks PRIVATE ${BASE_LINK_FLAGS})
target_link_libraries(benchmarks PRIVATE stewkk::libsql benchmark::benchmark
  Boost::asio
  Boost::thread
  Boost::filesystem
)
