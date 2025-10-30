include(FetchAntlr4)
include(SetupLLVM)

add_definitions(-DANTLR4CPP_STATIC)

add_executable(sql
  main.cpp
  codegen/TParser.cpp
  codegen/TLexer.cpp
  codegen/TParserBaseVisitor.cpp
  codegen/TParserVisitor.cpp
  logic/llvm_ir_builder.cpp
)

target_compile_features(sql PRIVATE cxx_std_20)
set_target_properties(sql PROPERTIES
  CXX_STANDART 20
  CXX_STANDART_REQUIRED YES
  CXX_EXTENSIONS YES
)

target_compile_options(sql PRIVATE ${BASE_COMPILE_FLAGS})
target_include_directories(sql PRIVATE
  ${ANTLR4_INCLUDE_DIRS}
  ${LLVM_INCLUDE_DIRS}
  $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/src>
)
llvm_map_components_to_libnames(llvm_libs support core irreader)
target_link_libraries(sql PRIVATE antlr4_static ${llvm_libs}
)
