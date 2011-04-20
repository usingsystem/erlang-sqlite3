REBAR=rebar
REBAR_COMPILE=$(REBAR) get-deps compile
PLT=dialyzer\sqlite3.plt
ERL_INTERFACE=$(ERL_ROOT)\lib\erl_interface-3.7.2
ERTS=$(ERL_ROOT)\erts-5.8.2
SQLITE_SRC=F:\MyProgramming\sqlite-amalgamation

all: compile

compile: 
	$(REBAR_COMPILE)

debug:
	$(REBAR_COMPILE) -C rebar.debug.config

tests:
	$(REBAR) eunit

clean:
	del /Q deps ebin priv doc\* .eunit c_src\*.o

docs:
	$(REBAR_COMPILE) doc

static: compile
	@if not exist $(PLT) \
		(mkdir dialyzer & dialyzer --build_plt --apps kernel stdlib erts --output_plt $(PLT)); \
	else \
		(dialyzer --plt $(PLT) -r ebin)

cross_compile: clean
	$(REBAR_COMPILE) -C rebar.cross_compile.config
