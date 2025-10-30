include(FetchAntlr4)
include(SetupLLVM)

add_definitions(-DANTLR4CPP_STATIC)

add_executable(sql
  main.cpp
  logic/parser/codegen/PostgreSQLLexer.cpp
  logic/parser/codegen/PostgreSQLParser.cpp
  logic/parser/codegen/PostgreSQLParserBaseListener.cpp
  logic/parser/codegen/PostgreSQLParserBaseVisitor.cpp
  logic/parser/codegen/PostgreSQLParserListener.cpp
  logic/parser/codegen/PostgreSQLParserVisitor.cpp
  logic/parser/PostgreSQLLexerBase.cpp
  logic/parser/PostgreSQLParserBase.cpp
)
target_include_directories(sql PRIVATE
  $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/src/stewkk/sql/logic/parser>
  $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/src/stewkk/sql/logic/parser/codegen>
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
