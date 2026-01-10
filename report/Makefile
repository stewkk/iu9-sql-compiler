.PHONY: build

all: build

build:
	@latexmk -f -pdf -output-directory=build -shell-escape ./report.tex

bib:
	@cp ./biblio.bib ./build
	@cd build && biber report

clean:
	@rm -rf build

uml:
	@plantuml dbms_scheme.puml

view:
	@evince ./build/report.pdf

