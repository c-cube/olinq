
TARGETS=olinq.cmxa olinq.cma olinq.cmxs
OPTIONS=-use-ocamlfind -classic-display

all:
	ocamlbuild $(OPTIONS) $(TARGETS)

clean:
	ocamlbuild -clean

DONTTEST=myocamlbuild.ml setup.ml $(wildcard src/**/*.cppo.*)
QTESTABLE=$(filter-out $(DONTTEST), \
	$(wildcard src/*.ml) \
	$(wildcard src/*.mli) \
	)

qtest-clean:
	@rm -rf qtest/

QTEST_PREAMBLE='open OLinq;; '

qtest-gen:
	@mkdir -p qtest
	@if which qtest > /dev/null ; then \
		qtest extract --preamble $(QTEST_PREAMBLE) \
			-o qtest/run_qtest.ml \
			$(QTESTABLE) 2> /dev/null ; \
	else touch qtest/run_qtest.ml ; \
	fi

clean-generated:
	rm **/*.{mldylib,mlpack,mllib} myocamlbuild.ml -f

test: qtest-gen
	ocamlbuild $(OPTIONS) -package ppx_deriving.show -package sequence \
	  -package oUnit -package qcheck -I src -I qtest qtest/run_qtest.native
	./run_qtest.native

DOCDIR=olinq.docdir

doc:
	mkdir -p $(DOCDIR)
	ocamldoc -I _build/src/ -html -d $(DOCDIR) src/*.mli

# FIXME update
VERSION=$(shell awk '/^Version:/ {print $$2}' _oasis)

update_next_tag:
	@echo "update version to $(VERSION)..."
	zsh -c 'sed -i "s/NEXT_VERSION/$(VERSION)/g" **/*.ml **/*.mli'
	zsh -c 'sed -i "s/NEXT_RELEASE/$(VERSION)/g" **/*.ml **/*.mli'

upload_doc: doc
	rsync -tavu olinq.docdir/* cedeela.fr:~/simon/root/files/doc/olinq/

watch:
	while find src/ -print0 | xargs -0 inotifywait -e delete_self -e modify ; do \
		echo "============ at `date` ==========" ; \
		make ; \
	done

.PHONY: all clean qtest-gen qtest-clean test update_next_tag doc
