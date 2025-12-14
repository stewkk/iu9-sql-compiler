include(FetchAntlr4)
include(SetupLLVM)
include(FetchBoost)

add_definitions(-DANTLR4CPP_STATIC)

add_library(libsql
  logic/parser/codegen/PostgreSQLLexer.cpp
  logic/parser/codegen/PostgreSQLParser.cpp
  logic/parser/codegen/PostgreSQLParserBaseListener.cpp
  logic/parser/codegen/PostgreSQLParserBaseVisitor.cpp
  logic/parser/codegen/PostgreSQLParserListener.cpp
  logic/parser/codegen/PostgreSQLParserVisitor.cpp
  logic/parser/PostgreSQLLexerBase.cpp
  logic/parser/PostgreSQLParserBase.cpp
  logic/parser/parser.cpp
  logic/parser/visitor.cpp
  models/parser/relational_algebra_ast.cpp
  logic/result/error.cpp
  logic/result/result.cpp
  logic/executor/executor.cpp
  logic/executor/sequential_scan.cpp
)
add_library(stewkk::libsql ALIAS libsql)
target_compile_features(libsql PUBLIC cxx_std_23)
set_target_properties(libsql PROPERTIES
  CXX_STANDART 23
  CXX_STANDART_REQUIRED YES
  CXX_EXTENSIONS YES
)
set_target_properties(libsql PROPERTIES OUTPUT_NAME stewkk_sql)
target_include_directories(
  libsql PUBLIC $<INSTALL_INTERFACE:include>
                    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
                    ${ANTLR4_INCLUDE_DIRS}
                    ${LLVM_INCLUDE_DIRS}
                    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/src>
                    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/src/stewkk/sql/logic/parser>
                    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/src/stewkk/sql/logic/parser/codegen>
)
target_compile_options(libsql PRIVATE ${BASE_COMPILE_FLAGS})
target_link_options(libsql PRIVATE ${BASE_LINK_FLAGS})
llvm_map_components_to_libnames(llvm_libs support core irreader)
target_link_libraries(libsql PRIVATE
  antlr4_static
  ${llvm_libs}
  Boost::asio
  Boost::thread
)

add_executable(sql
  main.cpp
)
target_compile_features(sql PRIVATE cxx_std_23)
set_target_properties(sql PROPERTIES
  CXX_STANDART 23
  CXX_STANDART_REQUIRED YES
  CXX_EXTENSIONS YES
)
target_compile_options(sql PRIVATE ${BASE_COMPILE_FLAGS})
target_link_libraries(sql PRIVATE
  libsql
)
