# Requires: texlive-latex-recommended texlive-latex-extra texlive-publishers
#           texlive-fonts-recommended texlive-fonts-extra cm-super
#           texlive-bibtex-extra texlive-pictures latexmk
# (acmart.cls is in texlive-publishers; cm-super is required or pdfTeX fails
#  with "auto expansion is only possible with scalable fonts")
all: bounded-clock-sweep.pdf
bounded-clock-sweep.pdf: bounded-clock-sweep.tex refs.bib $(wildcard *.dat)
	latexmk -pdf -interaction=nonstopmode $<
clean:
	latexmk -C
.PHONY: all clean
