REBAR=./rebar
REBAR_COMPILE=$(REBAR) get-deps compile

all: compile

compile:
	$(REBAR_COMPILE)

test:
	$(REBAR_COMPILE) skip_deps=true eunit

clean:
	-rm -rf deps ebin priv doc/* .eunit

docs:
	$(REBAR_COMPILE) doc

ifeq ($(wildcard dialyzer/sqlite3.plt),)
static:
	$(REBAR_COMPILE) build_plt analyze
else
static:
	$(REBAR_COMPILE) analyze
endif

.PHONY: all compile test clean docs static
