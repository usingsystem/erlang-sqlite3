REBAR=./rebar
REBAR_COMPILE=$(REBAR) get-deps compile
PLT=dialyzer/sqlite3.plt

all: compile

compile:
	$(REBAR_COMPILE)

test:
	$(REBAR_COMPILE) skip_deps=true eunit

clean:
	-rm -rf deps ebin priv doc/* .eunit c_src/*.o

docs:
	$(REBAR_COMPILE) doc

static:
	$(REBAR_COMPILE)
ifeq ($(wildcard $(PLT)),)
	dialyzer --build_plt --apps kernel stdlib erts --output_plt $(PLT) 
else
	dialyzer --plt $(PLT) -r ebin
endif

cross_compile: clean
	$(REBAR_COMPILE) -C rebar.cross_compile.config

.PHONY: all compile test clean docs static
