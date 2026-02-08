CURRENT_DIR := $(shell pwd)
CODEGEN_DIR := $(CURRENT_DIR)/src/stewkk/sql/logic/parser/codegen
PARSER_SOURCE_DIR := $(CURRENT_DIR)/src/stewkk/sql/logic/parser

build:
	cmake --build build -- -j 6

codegen:
	@antlr -Dlanguage=Cpp -visitor -o $(CODEGEN_DIR) -package stewkk::sql::codegen $(PARSER_SOURCE_DIR)/PostgreSQLParser.g4 $(PARSER_SOURCE_DIR)/PostgreSQLLexer.g4

.PHONY: codegen build
