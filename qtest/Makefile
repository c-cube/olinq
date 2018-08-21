
QTEST_PREAMBLE=''

DONTTEST=$(wildcard ../src/**/*.cppo.*)
QTESTABLE=$(filter-out $(DONTTEST), \
	$(wildcard ../src/*.ml) \
	$(wildcard ../src/*.mli) \
	)
	# $(wildcard ../src/backends/*.ml) \
	# $(wildcard ../src/backends/*.mli) \

qtest-gen:
	@if which qtest > /dev/null ; then \
	  	echo "generate qtest"; \
		qtest extract --preamble $(QTEST_PREAMBLE) \
			-o run_qtest.ml \
			$(QTESTABLE) 2> /dev/null ; \
	else touch run_qtest.ml ; \
	fi
