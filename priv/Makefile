GCC=gcc
LDFLAGS=-shared -L/usr/local/lib -lsqlite3 -bundle -flat_namespace -undefined suppress -fPIC -lerl_interface -L/usr/local/lib/erlang/lib/erl_interface-3.6.1/lib -lei
SRCS=sqlite3_drv.c sqlite3_drv.h
OUTPUT=sqlite3_drv.so
CFLAGS=-o ../ebin/${OUTPUT} -I/usr/local/lib/erlang/usr/include/ -I/usr/local/lib/erlang/lib/erl_interface-3.6.1/include/ -arch x86_64

all: ../ebin/${OUTPUT}


../ebin/${OUTPUT}: sqlite3_drv.c sqlite3_drv.h
	${GCC} sqlite3_drv.c ${LDFLAGS} ${CFLAGS}

clean:
	rm -rf ${OUTPUT}
