REBAR=rebar
REBAR_COMPILE=$(REBAR) get-deps compile
PLT=dialyzer\sqlite3.plt
ERL_INTERFACE=$(ERL_ROOT)\lib\erl_interface-3.7.2
ERTS=$(ERL_ROOT)\erts-5.8.2
SQLITE_SRC=F:\MyProgramming\sqlite-amalgamation

all: compile

compile: compile_c src\*
	erlc -I include -o ebin src\*.erl

# while rebar doesn't support Windows ports
compile_c: c_src\* 
	cl /W4 /wd4100 /wd4204 /I$(ERL_INTERFACE)/include /I$(ERTS)/include /I$(SQLITE_SRC) /Ic_src c_src/*.c /link /dll /LIBPATH:$(ERL_INTERFACE)\lib ei.lib $(SQLITE_SRC)\sqlite3.lib /out:priv\sqlite3_drv.dll

test: compile_c test\*
	erlc -DTEST=true -I include -o .eunit src\*.erl test\*.erl
	$(REBAR) eunit

clean:
	-rm -rf deps ebin priv doc\* .eunit c_src\*.o

docs: compile
	$(REBAR) doc

# static: compile
# ifeq ($(wildcard $(PLT)),)
#	dialyzer --build_plt --apps kernel stdlib erts --output_plt $(PLT) 
# else
#	dialyzer --plt $(PLT) -r ebin
# endif

cross_compile: clean
	$(REBAR_COMPILE) -C rebar.cross_compile.config

.PHONY: all compile test clean docs static
