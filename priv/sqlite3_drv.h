#include <erl_driver.h>
#include <ei.h>
#include <stdio.h>
#include <string.h>
#include <sqlite3.h>
#include <erl_interface.h>

#if SQLITE_VERSION_NUMBER < 3006001
#error "SQLite3 of version 3.6.1 minumum required"
#endif



// Path to file where data will be stored. 
// It will be created if it doesn't exist
#define DB_PATH "./store.db"

// Binary commands between Erlang VM and Driver
#define CMD_SQL_EXEC 2
// #define CMD_DEL 3

// Number of bytes for each key
// (160 bits for SHA1 hash)
#define KEY_SIZE 20

// Define struct to hold state across calls
typedef struct sqlite3_drv_t {
  ErlDrvPort port;
  unsigned int key;
  struct sqlite3 *db;
  long async_handle;
  FILE *log;
} sqlite3_drv_t;

typedef struct async_sqlite3_command {
  sqlite3_drv_t *driver_data;
  sqlite3_stmt *statement;
  ErlDrvTermData *dataset;
  int term_count;
  int row_count;
  double *floats;
  int binaries_count;
  ErlDrvBinary **binaries;
} async_sqlite3_command;


static ErlDrvData start(ErlDrvPort port, char* cmd);
static void stop(ErlDrvData handle);
static int control(ErlDrvData drv_data, unsigned int command, char *buf, 
                   int len, char **rbuf, int rlen);
static int sql_exec(sqlite3_drv_t *drv, char *buf, int len);
static void sql_exec_async(void *async_command);
static void sql_free_async(void *async_command);
static void ready_async(ErlDrvData drv_data, ErlDrvThreadData thread_data);
static int unknown(sqlite3_drv_t *bdb_drv, char *buf, int len);
