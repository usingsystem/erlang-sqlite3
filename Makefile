REBAR=./rebar

all: compile

compile:
	$(REBAR) get-deps compile

test: all
	$(REBAR) skip_deps=true eunit

clean:
	-rm -rf deps ebin priv doc/*

docs:
	$(REBAR) doc

ifeq ($(wildcard dialyzer/sqlite3.plt),)
static:
	$(REBAR) build_plt analyze
else
static:
	$(REBAR) analyze
endif