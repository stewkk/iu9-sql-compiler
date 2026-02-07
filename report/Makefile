.PHONY: build all bib clean uml view pres

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
	@plantuml dbms_scheme_impl.puml

view:
	@evince ./build/report.pdf

pres:
	@latexmk -f -pdf -output-directory=build -shell-escape ./presentation.tex
