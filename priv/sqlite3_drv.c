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
    NULL,                             /* control */
    NULL,                             /* timeout */
    outputv,                          /* outputv (defined below) */
    ready_async,                             /* ready_async */
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
static void outputv(ErlDrvData handle, ErlIOVec *ev) {
  sqlite3_drv_t* driver_data = (sqlite3_drv_t*) handle;
  ErlDrvBinary* data = ev->binv[1];
  int command = data->orig_bytes[0]; // First byte is the command
  
  fprintf(stderr, "Command: %s\n", data->orig_bytes);
  
  switch(command) {
    case CMD_LIST_TABLES:
      list_tables(driver_data, ev);
      break;
  
    // case CMD_GET:
    //   get(driver_data, ev);
    //   break;
    //   
    // case CMD_DEL:
    //   del(driver_data, ev);
    //   break;
  
    default:
      unkown(driver_data, ev);
  }
}

static void ready_async(ErlDrvData drv_data, ErlDrvThreadData thread_data)
{
  
}

static void list_tables(sqlite3_drv_t *drv, ErlIOVec *ev) {
}

#if 0
// Insert or replace record in the database
static void put(sqlite3_drv_t *bdb_drv, ErlIOVec *ev) {
  ErlDrvBinary* input = ev->binv[1];
  char *bytes = input->orig_bytes;
  char *key_bytes = bytes+1;
  char *value_bytes = bytes+1+KEY_SIZE;
  int value_size = input->orig_size - 1 - KEY_SIZE;
  
  DB *db = bdb_drv->db;
  DBT key;
  DBT value;
  int status;

  // Erase bytes to get rid of residual data
  bzero(&key, sizeof(DBT));
  bzero(&value, sizeof(DBT));
  
  key.data = key_bytes;
  key.size = KEY_SIZE;
  
  value.data = value_bytes;
  value.size = value_size;
  
  // Insert the record and then write it to disk
  status = db->put(db, NULL, &key, &value, 0);
  db->sync(db, 0);

  if(status == 0) {
  	// Insert went OK
  	// Prepare return value to Erlang VM, returns atom 'ok'
    ErlDrvTermData spec[] = {ERL_DRV_ATOM, driver_mk_atom("ok")};

	// Return the value to the Erlang VM
    driver_output_term(bdb_drv->port, spec, sizeof(spec) / sizeof(spec[0]));
  } else {
  	// There was an error return {error, Reason}
    char * error_reason;

    switch(status) {
    case DB_LOCK_DEADLOCK:
      error_reason = "deadlock";
      break;
    case EACCES:
      error_reason = "readonly";
      break;
    case EINVAL:
      error_reason = "badflag";
      break;
    case ENOSPC:
      error_reason = "btree_max";
      break;
    case DB_RUNRECOVERY:
      error_reason = "run_recovery";
      break;
    default:
      error_reason = "unkown";
    }
    
    // Returns tuple {error, Reason}
    ErlDrvTermData spec[] = {ERL_DRV_ATOM, driver_mk_atom("error"),
			     ERL_DRV_ATOM, driver_mk_atom(error_reason),
			     ERL_DRV_TUPLE, 2};

    driver_output_term(bdb_drv->port, spec, sizeof(spec) / sizeof(spec[0]));
  }
}

// Retrieve a record from the database, if it exists
static void get(sqlite3_drv_t *bdb_drv, ErlIOVec *ev) {
  ErlDrvBinary* input = ev->binv[1];
  ErlDrvBinary *output_bytes;
  char *bytes = input->orig_bytes;
  char *key_bytes = bytes+1;
  
  DB *db = bdb_drv->db;
  DBT key;
  DBT value;
  int status;
  
  bzero(&key, sizeof(DBT));
  bzero(&value, sizeof(DBT));
    
  key.data = key_bytes;
  key.size = KEY_SIZE;
  
  // Have BerkeleyDB allocate memory big enough to store the value
  value.flags = DB_DBT_MALLOC; // Don't forget to free it later
  
  // Retrieve the record
  status = db->get(db, NULL, &key, &value, 0);

  if(status == 0) {
  	// Get went OK
  	
  	// Copy the record value to an output structure to return to Erlang VM
    output_bytes = driver_alloc_binary(value.size);
    output_bytes->orig_size = value.size;
    memcpy(output_bytes->orig_bytes, value.data, value.size);
    free(value.data);
    
    // TODO:Figure out if we can somehow use this original memory without recopying a la:
    //binary->orig_bytes = (char *)&data.data;
    
    // Returns tuple {ok, Data}
    ErlDrvTermData spec[] = {ERL_DRV_ATOM, driver_mk_atom("ok"),
			     ERL_DRV_BINARY, (ErlDrvTermData) output_bytes, output_bytes->orig_size, 0,
			     ERL_DRV_TUPLE, 2};
    
    driver_output_term(bdb_drv->port, spec, sizeof(spec) / sizeof(spec[0]));
    driver_free_binary(output_bytes);
  } else {
  	// there was an error
    char *error_reason;

    switch(status) {
    case DB_LOCK_DEADLOCK:
      error_reason = "deadlock";
      break;
    case DB_SECONDARY_BAD:
      error_reason = "bad_secondary_index";
      break;
    case ENOMEM:
      error_reason = "insufficient_memory";
      break;
    case EINVAL:
      error_reason = "bad_flag";
      break;
    case DB_RUNRECOVERY:
      error_reason = "run_recovery";
      break;
    default:
      error_reason = "unknown";
    }
    
    // Return tuple {error, Reason}
    ErlDrvTermData spec[] = {ERL_DRV_ATOM, driver_mk_atom("error"),
			     ERL_DRV_ATOM, driver_mk_atom(error_reason),
			     ERL_DRV_TUPLE, 2};
    driver_output_term(bdb_drv->port, spec, sizeof(spec) / sizeof(spec[0]));
  }
}

// Delete a record from the database
static void del(sqlite3_drv_t *bdb_drv, ErlIOVec *ev) {
  ErlDrvBinary* data = ev->binv[1];
  char *bytes = data->orig_bytes;
  char *key_bytes = bytes+1;
  
  DB *db = bdb_drv->db;
  DBT key;
  int status;
  
  bzero(&key, sizeof(DBT));
    
  key.data = key_bytes;
  key.size = KEY_SIZE;
  
  status = db->del(db, NULL, &key, 0);
  db->sync(db, 0);
  
  if(status == 0) {
  	// Delete went OK, return atom 'ok'
    ErlDrvTermData spec[] = {ERL_DRV_ATOM, driver_mk_atom("ok")};
    
    driver_output_term(bdb_drv->port, spec, sizeof(spec) / sizeof(spec[0]));
    
  } else {
  	// There was an error
    char *error_reason;

    switch(status) {
    case DB_NOTFOUND:
      error_reason = "not_found";
      break;
    case DB_LOCK_DEADLOCK:
      error_reason = "deadlock";
      break;
    case DB_SECONDARY_BAD:
      error_reason = "bad_secondary_index";
      break;
    case EINVAL:
      error_reason = "bad_flag";
      break;
    case EACCES:
      error_reason = "readonly";
      break;
    case DB_RUNRECOVERY:
      error_reason = "run_recovery";
      break;
    default:
      error_reason = "unknown";
    }
    
    // Return tuple {error, Reason}
    ErlDrvTermData spec[] = {ERL_DRV_ATOM, driver_mk_atom("error"),
			     ERL_DRV_ATOM, driver_mk_atom(error_reason),
			     ERL_DRV_TUPLE, 2};

    driver_output_term(bdb_drv->port, spec, sizeof(spec) / sizeof(spec[0]));
  }
}
#endif

// Unkown Command
static void unkown(sqlite3_drv_t *bdb_drv, ErlIOVec *ev) {
  // Return {error, unkown_command}
  ErlDrvTermData spec[] = {ERL_DRV_ATOM, driver_mk_atom("error"),
			   ERL_DRV_ATOM, driver_mk_atom("uknown_command"),
			   ERL_DRV_TUPLE, 2};
  driver_output_term(bdb_drv->port, spec, sizeof(spec) / sizeof(spec[0]));
}
