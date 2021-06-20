file=tiny-search-engine
sed "s/    \\\end{lstlisting}/\\\end{lstlisting}/g" $file.tex >t.tex
xelatex -synctex=1 -interaction=nonstopmode t.tex
bibtex t
xelatex -synctex=1 -interaction=nonstopmode t.tex
