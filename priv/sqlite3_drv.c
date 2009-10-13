#include "sqlite3_drv.h"

// Callback Array
static ErlDrvEntry basic_driver_entry = {
    NULL,                             /* init */
    start,                            /* startup (defined below) */
    stop,                             /* shutdown (defined below) */
    NULL,                             /* output */
    NULL,                             /* ready_input */
    NULL,                             /* ready_output */
    "sqlite3_drv",                    /* the name of the driver */
    NULL,                             /* finish */
    NULL,                             /* handle */
    control,                          /* control */
    NULL,                             /* timeout */
    NULL,                             /* outputv (defined below) */
    NULL,                             /* ready_async */
    NULL,                             /* flush */
    NULL,                             /* call */
    NULL,                             /* event */
    ERL_DRV_EXTENDED_MARKER,          /* ERL_DRV_EXTENDED_MARKER */
    ERL_DRV_EXTENDED_MAJOR_VERSION,   /* ERL_DRV_EXTENDED_MAJOR_VERSION */
    ERL_DRV_EXTENDED_MAJOR_VERSION,   /* ERL_DRV_EXTENDED_MINOR_VERSION */
    ERL_DRV_FLAG_USE_PORT_LOCKING     /* ERL_DRV_FLAGs */
};

DRIVER_INIT(basic_driver) {
  return &basic_driver_entry;
}

// Driver Start
static ErlDrvData start(ErlDrvPort port, char* cmd) {
  sqlite3_drv_t* retval = (sqlite3_drv_t*) driver_alloc(sizeof(sqlite3_drv_t));
  struct sqlite3 *db;
  int status;
  
  // Create and open the database
  sqlite3_open(DB_PATH, &db);
  status = sqlite3_errcode(db);

  if(status != SQLITE_OK) {
    fprintf(stderr, "Unabled to open file: %s because %s\n\n", DB_PATH, sqlite3_errmsg(db));
  }
  fprintf(stderr, "Opened file %s\n", DB_PATH);

  // Set the state for the driver
  retval->port = port;
  retval->db = db;
  
  return (ErlDrvData) retval;
}


// Driver Stop
static void stop(ErlDrvData handle) {
  sqlite3_drv_t* driver_data = (sqlite3_drv_t*) handle;

  sqlite3_close(driver_data->db);
  driver_free(driver_data);
}

// Handle input from Erlang VM
static int control(ErlDrvData drv_data, unsigned int command, char *buf, 
                   int len, char **rbuf, int rlen) {
  sqlite3_drv_t* driver_data = (sqlite3_drv_t*) drv_data;
  
  switch(command) {
    case CMD_SQL_EXEC:
      sql_exec(driver_data, buf, len);
      break;
    default:
      unknown(driver_data, buf, len);
  }
  return 0;
}

static void ready_async(ErlDrvData drv_data, ErlDrvThreadData thread_data)
{
  
}

static inline int return_error(sqlite3_drv_t *drv, const char *error) {
  ErlDrvTermData spec[] = {ERL_DRV_ATOM, driver_mk_atom("error"),
		     ERL_DRV_STRING, (ErlDrvTermData)error, strlen(error),
		     ERL_DRV_TUPLE, 2};

  return driver_output_term(drv->port, spec, sizeof(spec) / sizeof(spec[0]));
  if (error) {
    // sqlite3_free((char *)error);
  }
}

static int sql_exec(sqlite3_drv_t *drv, char *command, int command_size) {

  ErlDrvTermData *dataset;

  int result, next_row, column_count, row_count = 0, term_count = 0;
  char *error = NULL;
  char *rest = NULL;
  sqlite3_stmt *statement;
  
  double *floats = NULL;
  int float_count = 0;
  
  ErlDrvBinary **binaries = NULL;
  int binaries_count = 0;
  int i;
  

  fprintf(stderr, "Exec: %.*s\n", command_size, command);

  result = sqlite3_prepare_v2(drv->db, command, command_size, &statement, (const char **)&rest);
  if(result != SQLITE_OK) { 
    return return_error(drv, sqlite3_errmsg(drv->db)); 
  }
  
  column_count = sqlite3_column_count(statement);
  dataset = NULL;
  
  fprintf(stderr, "Going to read some rows with %d columns\n", column_count);
  while ((next_row = sqlite3_step(statement)) == SQLITE_ROW) {
    if (row_count == 0) {
      term_count = 2 + column_count*2 + 2 + 2;
      dataset = realloc(dataset, sizeof(*dataset) * term_count);
      dataset[0] = ERL_DRV_ATOM;
      dataset[1] = driver_mk_atom("columns");
      for (i = 0; i < column_count; i++) {
        dataset[2 + (i*2)] = ERL_DRV_ATOM;
        fprintf(stderr, "Column: %s\n", sqlite3_column_name(statement, i));
        dataset[2 + (i*2) + 1] = driver_mk_atom((char *)sqlite3_column_name(statement, i));
      }
      dataset[2 + column_count*2] = ERL_DRV_LIST;
      dataset[2 + column_count*2 + 1] = column_count;
      dataset[2 + column_count*2 + 2] = ERL_DRV_TUPLE;
      dataset[2 + column_count*2 + 3] = 2;
    }
    
    for (i = 0; i < column_count; i++) {
      fprintf(stderr, "Column %d type: %d\n", i, sqlite3_column_type(statement, i));
      switch (sqlite3_column_type(statement, i)) {
        case SQLITE_INTEGER: {
          term_count += 2;
          dataset = realloc(dataset, sizeof(*dataset) * term_count);
          dataset[term_count - 2] = ERL_DRV_INT;
          dataset[term_count - 1] = sqlite3_column_int(statement, i);
          break;
        }
        // case SQLITE_FLOAT: {
        //   float_count++;
        //   floats = realloc(floats, sizeof(double) * float_count);
        //   floats[float_count - 1] = sqlite3_column_double(statement, i);
        //   
        //   term_count += 2;
        //   dataset = realloc(dataset, sizeof(*dataset) * term_count);
        //   dataset[term_count - 2] = ERL_DRV_FLOAT;
        //   dataset[term_count - 1] = (ErlDrvTermData)&floats[float_count - 1];
        //   break;
        // }
        case SQLITE_BLOB: 
        case SQLITE_TEXT: {
          int bytes = sqlite3_column_bytes(statement, i);
          binaries_count++;
          binaries = realloc(binaries, sizeof(*binaries) * binaries_count);
          binaries[binaries_count - 1] = driver_alloc_binary(bytes);
          binaries[binaries_count - 1]->orig_size = bytes;
          memcpy(binaries[binaries_count - 1]->orig_bytes, sqlite3_column_blob(statement, i), bytes);
        
          term_count += 2;
          dataset = realloc(dataset, sizeof(*dataset) * term_count);
          dataset[term_count - 2] = ERL_DRV_FLOAT;
          dataset[term_count - 1] = (ErlDrvTermData)&floats[float_count - 1];
          break;
        }
        case SQLITE_NULL: {
          break;
        }
      }
    }
    term_count += 2;
    dataset = realloc(dataset, sizeof(*dataset) * term_count);
    dataset[term_count - 2] = ERL_DRV_TUPLE;
    dataset[term_count - 1] = column_count;
    
    row_count++;
  }
  
  if (next_row == SQLITE_BUSY) {
    sqlite3_finalize(statement);
    return return_error(drv, "SQLite3 database is busy"); 
  }

  term_count += 2;
  dataset = realloc(dataset, sizeof(*dataset) * term_count);
  dataset[term_count - 2] = ERL_DRV_LIST;
  dataset[term_count - 1] = 2;
  
  
  int res = driver_output_term(drv->port, dataset, term_count);
  fprintf(stderr, "Total term count: %d, rows count: %d (%d)\n", term_count, row_count, res);
  // free(dataset);
  
  // if (floats) {
  //   free(floats);
  // }
  // for (i = 0; i < binaries_count; i++) {
  //   driver_free_binary(binaries[i]);
  // }
  // if(binaries) {
  //   free(binaries);
  // }
  // sqlite3_finalize(statement);
}

// Unkown Command
static int unknown(sqlite3_drv_t *drv, char *command, int command_size) {
  // Return {error, unkown_command}
  ErlDrvTermData spec[] = {ERL_DRV_ATOM, driver_mk_atom("error"),
			   ERL_DRV_ATOM, driver_mk_atom("uknown_command"),
			   ERL_DRV_TUPLE, 2};
  return driver_output_term(drv->port, spec, sizeof(spec) / sizeof(spec[0]));
}
