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
  models/parser/expression.cpp
  models/parser/join_type.cpp
  models/parser/relational_algebra_ast.cpp
  logic/result/error.cpp
  logic/result/result.cpp
  logic/executor/plan.cpp
  logic/executor/plan_serializer.cpp
  logic/executor/executor.cpp
  logic/executor/sequential_scan.cpp
  models/executor/tuple.cpp
  logic/executor/materialization.cpp
  logic/executor/llvm.cpp
  logic/optimizer/optimizer.cpp
  logic/optimizer/reachability.cpp
  logic/optimizer/cardinality.cpp
  logic/optimizer/schema_catalog.cpp
  logic/optimizer/winner_key.cpp
  logic/optimizer/sort_enforcer.cpp
  logic/optimizer/group.cpp
  logic/optimizer/memo.cpp
  logic/optimizer/rules.cpp
  logic/optimizer/rules_applier.cpp
  logic/optimizer/rule.cpp
  logic/transformation_rules/predicate_utils.cpp
  logic/transformation_rules/join_commutativity.cpp
  logic/transformation_rules/join_associativity.cpp
  logic/transformation_rules/filter_split.cpp
  logic/transformation_rules/filter_merge.cpp
  logic/transformation_rules/filter_pushdown_through_projection.cpp
  logic/transformation_rules/filter_pushdown_through_join.cpp
  logic/transformation_rules/in_to_or_chain.cpp
  logic/implementation_rules/implement_table.cpp
  logic/implementation_rules/implement_filter.cpp
  logic/implementation_rules/implement_projection.cpp
  logic/implementation_rules/implement_join.cpp
  logic/implementation_rules/implement_cross_join.cpp
  logic/implementation_rules/implement_hash_join.cpp
  logic/implementation_rules/implement_aggregation.cpp
  logic/optimizer/properties/sort_order.cpp
  logic/optimizer/properties/sort_property.cpp
  logic/optimizer/properties/property_set.cpp
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
target_link_directories(libsql PUBLIC ${LLVM_LIBRARY_DIRS})
if(TARGET LLVM)
  set(llvm_libs LLVM)
else()
  llvm_map_components_to_libnames(llvm_libs Support Core IRReader OrcJIT X86CodeGen X86AsmParser X86Desc X86Info)
endif()
target_link_libraries(libsql PRIVATE
  antlr4_static
  ${llvm_libs}
  Boost::asio
  Boost::thread
  Boost::filesystem
  Boost::scope
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
  Boost::asio
  Boost::thread
  Boost::filesystem
)
