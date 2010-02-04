ERL=erl
OTP_TOP=/usr/lib/erlang/lib
PLT_SRC=$(OTP_TOP)/kernel-2.12.5/ebin $(OTP_TOP)/stdlib-1.15.5/ebin/ $(OTP_TOP)/crypto-1.5.3/ebin $(OTP_TOP)/compiler-4.5.5/ebin $(OTP_TOP)/hipe-3.6.9/ebin/ $(OTP_TOP)/syntax_tools-1.5.6/ebin $(OTP_TOP)/hipe-3.6.9/ebin ebin

all: compile docs

compile:
	test -d ebin || mkdir ebin
	$(ERL) -make
	cd priv && make

static:
	dialyzer --build_plt --output_plt sqlite3.plt -r ebin $(PLT_SRC)

clean:
	- rm -rf ebin/*.beam doc/* sqlite3.plt src/test/*.beam
	- rm -rf ct_run* all_runs.html variables* index.html
	- find . -name "*~" | xargs rm
	cd priv && make clean

docs:
	$(ERL) -noshell -run edoc_run application "'sqlite3'" '"."' '[{title,"Welcome to sqlite3"},{hidden,false},{private,false}]' -s erlang halt
