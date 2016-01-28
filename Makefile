
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

VERSION=$(shell awk '/^Version:/ {print $$2}' _oasis)

update_next_tag:
	@echo "update version to $(VERSION)..."
	zsh -c 'sed -i "s/NEXT_VERSION/$(VERSION)/g" **/*.ml **/*.mli'
	zsh -c 'sed -i "s/NEXT_RELEASE/$(VERSION)/g" **/*.ml **/*.mli'

watch:
	while find src/ benchs/ -print0 | xargs -0 inotifywait -e delete_self -e modify ; do \
		echo "============ at `date` ==========" ; \
		make ; \
	done

.PHONY: all clean qtest-gen qtest-clean update_next_tag
