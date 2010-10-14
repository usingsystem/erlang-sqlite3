ERL=/usr/local/bin/erl
ERLC=/usr/local/bin/erlc
ERL_ERTS=/usr/local/lib/erlang/lib/erts-5.8.1
ERL_KERNEL=/usr/local/lib/erlang/lib/kernel-2.14.1
ERL_STDLIB=/usr/local/lib/erlang/lib/stdlib-1.17.1
ERL_CRYPTO=/usr/local/lib/erlang/lib/crypto-2.0.1
ERL_COMPILER=/usr/local/lib/erlang/lib/compiler-4.7.1
ERL_HIPE=/usr/local/lib/erlang/lib/hipe-3.7.7
ERL_SYNTAX_TOOLS=/usr/local/lib/erlang/lib/syntax_tools-1.6.6
OTP_TOP=/usr/lib/erlang/lib
PLT_SRC=$(ERL_ERTS)/ebin $(ERL_KERNEL)/ebin $(ERL_STDLIB)/ebin $(ERL_CRYPTO)/ebin $(ERL_COMPILER)/ebin $(ERL_HIPE)/ebin $(ERL_SYNTAX_TOOLS)/ebin

all: compile docs

test: compile_test
	$(ERL) -noshell -pa ebin \
		-eval 'eunit:test({dir, "ebin"})' \
		-s init stop

compile: compile_c
	$(ERL) -make

compile_test: compile_c
	erl -noshell -eval "make:all([{d, 'TEST'}])." -s init stop

compile_c:
	test -d ebin || mkdir ebin
	cd priv && make

ifeq ($(wildcard sqlite3.plt),)
static:
	dialyzer --build_plt --output_plt sqlite3.plt -r $(PLT_SRC)
	dialyzer --add_to_plt --plt sqlite3.plt -r ebin --get_warnings
else
static:
	dialyzer --add_to_plt --plt sqlite3.plt -r ebin --get_warnings
endif

clean:
	- rm -rf ebin/*.beam doc/* sqlite3.plt src/test/*.beam
	- rm -rf ct_run* all_runs.html variables* index.html
	- find . -name "*~" | xargs rm
	cd priv && make clean

docs:
	$(ERL) -noshell -run edoc_run application "'sqlite3'" '"."' '[{title,"Welcome to sqlite3"},{hidden,false},{private,false}]' -s erlang halt
