// cl.exe defines macro _WIN32, but erl_interface.h checks for __WIN32__
#define _CRT_SECURE_NO_WARNINGS // secure functions aren't cross-platform

#include <erl_driver.h>
#include <erl_interface.h>
#include <ei.h>
#include <sqlite3.h>
#include <stdio.h>
#include <ctype.h>
#include <string.h>
#include <assert.h>

#include "list.h"

#if SQLITE_VERSION_NUMBER < 3006001
#error "SQLite3 of version 3.6.1 minumum required"
#endif

#if defined(_MSC_VER)
#pragma warning(disable: 4201)
#endif

#define DEBUG 1

// Path to file where data will be stored. 
// It will be created if it doesn't exist
#define DB_PATH "./store.db"
#define LOG_PATH "/tmp/erlang-sqlite3-drv.log"

// Binary commands between Erlang VM and Driver
#define CMD_SQL_EXEC 2
// #define CMD_DEL 3
#define CMD_SQL_BIND_AND_EXEC 4
#define CMD_PREPARE 5
#define CMD_PREPARED_BIND 6
#define CMD_PREPARED_STEP 7
#define CMD_PREPARED_RESET 8
#define CMD_PREPARED_CLEAR_BINDINGS 9
#define CMD_PREPARED_FINALIZE 10
#define CMD_PREPARED_COLUMNS 11
#define CMD_SQL_EXEC_SCRIPT 12

// Number of bytes for each key
// (160 bits for SHA1 hash)
#define KEY_SIZE 20

// Define struct to hold state across calls
typedef struct Sqlite3Drv {
  ErlDrvPort port;
  unsigned int key;
  struct sqlite3 *db;
  long async_handle;
  FILE *log;
  sqlite3_stmt **prepared_stmts;
  unsigned int prepared_count;
  unsigned int prepared_alloc;
  ErlDrvTermData atom_blob;
  ErlDrvTermData atom_error;
  ErlDrvTermData atom_columns;
  ErlDrvTermData atom_rows;
  ErlDrvTermData atom_null;
  ErlDrvTermData atom_rowid;
  ErlDrvTermData atom_ok;
  ErlDrvTermData atom_done;
  ErlDrvTermData atom_unknown_cmd;
} Sqlite3Drv;

typedef enum async_sqlite3_command_type {t_stmt, t_script} async_sqlite3_command_type;

typedef struct async_sqlite3_command {
  Sqlite3Drv *driver_data;
  async_sqlite3_command_type type;
  union {
    sqlite3_stmt *statement;
    struct {
      char *script;
      char *end;
    };
  };
  ErlDrvTermData *dataset;
  int term_count;
  int term_allocated;
  int row_count;
  list *ptrs;
  list *binaries;
  int finalize_statement_on_free;
  int error_code;
} async_sqlite3_command;


static ErlDrvData start(ErlDrvPort port, char* cmd);
static void stop(ErlDrvData handle);
static int control(ErlDrvData drv_data, unsigned int command, char *buf, 
                   int len, char **rbuf, int rlen);
static int sql_exec(Sqlite3Drv *drv, char *buf, int len);
static int sql_bind_and_exec(Sqlite3Drv *drv, char *buf, int len);
static int sql_exec_script(Sqlite3Drv *drv, char *buf, int len);
static int prepare(Sqlite3Drv *drv, char *buf, int len);
static int prepared_bind(Sqlite3Drv *drv, char *buf, int len);
static int prepared_step(Sqlite3Drv *drv, char *buf, int len);
static int prepared_reset(Sqlite3Drv *drv, char *buf, int len);
static int prepared_clear_bindings(Sqlite3Drv *drv, char *buf, int len);
static int prepared_finalize(Sqlite3Drv *drv, char *buf, int len);
static int prepared_columns(Sqlite3Drv *drv, char *buf, int len);
static void sql_exec_async(void *async_command);
static void sql_free_async(void *async_command);
static void ready_async(ErlDrvData drv_data, ErlDrvThreadData thread_data);
static int unknown(Sqlite3Drv *bdb_drv, char *buf, int len);


#if defined(_MSC_VER)
#pragma warning(default: 4201)
#endif
