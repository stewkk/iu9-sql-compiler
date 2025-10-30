
codegen:
	@cd docs && antlr -Dlanguage=Cpp -visitor -o ../src/stewkk/sql/codegen -package stewkk::sql::codegen TParser.g4 TLexer.g4

.PHONY: codegen
