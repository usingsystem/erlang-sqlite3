ERL_INTERFACE=/usr/local/lib/erlang/lib/erl_interface-3.7.1
ERL_ROOT=/usr/local/lib/erlang
GCC=gcc
LDFLAGS=-shared -L/usr/lib -lsqlite3 -undefined -fPIC -lerl_interface -L$(ERL_INTERFACE)/lib -lei
SRCS=sqlite3_drv.c sqlite3_drv.h
OUTPUT=sqlite3_drv.so
CFLAGS=-o ../ebin/${OUTPUT} -I$(ERL_ROOT)/usr/include/ -I$(ERL_INTERFACE)/include/ -DNULL_ATOM=null

all: ../ebin/${OUTPUT}


../ebin/${OUTPUT}: sqlite3_drv.c sqlite3_drv.h
	${GCC} sqlite3_drv.c ${LDFLAGS} ${CFLAGS}

clean:
	rm -rf ../ebin/${OUTPUT}
