#include "sqlite3_drv.h"

#ifndef max // macro in Windows
static inline int max(int a, int b);
#endif
static inline int sql_is_insert(const char *sql);
#ifdef DEBUG
static void fprint_dataset(FILE* log, ErlDrvTermData* dataset, int term_count);
#endif

static void dataset_realloc(ErlDrvTermData **dataset, int *count, int *allocated, int size);

static unsigned int hash(char *s);

static ErlDrvEntry sqlite3_driver_entry = {
    NULL, /* init */
    start, /* startup (defined below) */
    stop, /* shutdown (defined below) */
    NULL, /* output */
    NULL, /* ready_input */
    NULL, /* ready_output */
    "sqlite3_drv", /* the name of the driver */
    NULL, /* finish */
    NULL, /* handle */
    control, /* control */
    NULL, /* timeout */
    NULL, /* outputv */
    ready_async, /* ready_async (defined below) */
    NULL, /* flush */
    NULL, /* call */
    NULL, /* event */
    ERL_DRV_EXTENDED_MARKER, /* ERL_DRV_EXTENDED_MARKER */
    ERL_DRV_EXTENDED_MAJOR_VERSION, /* ERL_DRV_EXTENDED_MAJOR_VERSION */
    ERL_DRV_EXTENDED_MINOR_VERSION, /* ERL_DRV_EXTENDED_MINOR_VERSION */
    ERL_DRV_FLAG_USE_PORT_LOCKING, /* ERL_DRV_FLAGs */
    NULL /* handle2 */,
    NULL /* process_exit */,
    NULL /* stop_select */
};

DRIVER_INIT(sqlite3_driver) {
  return &sqlite3_driver_entry;
}

// required because driver_free(_binary) are macros in Windows
static void driver_free_fun(void *p) {
    driver_free(p); 
}

static void driver_free_binary_fun(void *ptr) {
    driver_free_binary((ErlDrvBinary *) ptr); 
}

static unsigned int hash(char *s)
{
	unsigned int x = 378511;
	unsigned int y = 63689;
	unsigned int hash = 0;
	while(*s) {
		hash = hash*y+(*s++);
		hash *= x;
	}
	return (hash & 0x7fffffff);
}

// Driver Start
static ErlDrvData start(ErlDrvPort port, char* cmd) {
  Sqlite3Drv* drv = (Sqlite3Drv*) driver_alloc(sizeof(Sqlite3Drv));
  struct sqlite3 *db = NULL;
  int status = 0;
  char *db_name;

  drv->log = fopen(LOG_PATH, "a+");
  if (!drv->log) {
    fprintf(stderr, "Error creating log file: %s\n", LOG_PATH);
    // if we can't open the log file we shouldn't hide the data or the problem
    drv->log = stderr; // noisy
  }

  fprintf(drv->log,
          "--- Start erlang-sqlite3 driver\nCommand line: [%s]\n", cmd);

  fprintf(drv->log, "sqlite3 is thread safe?: %d\n", (int)sqlite3_threadsafe());

  db_name = strstr(cmd, " ");
  if (!db_name) {
    fprintf(drv->log,
            "ERROR: DB name should be passed at command line\n");
    db_name = DB_PATH;
  } else {
    ++db_name; // move to first character after ' '
  }

  // Create and open the database
  sqlite3_open(db_name, &db);
  status = sqlite3_errcode(db);

  if (status != SQLITE_OK) {
    fprintf(drv->log, "ERROR: Unable to open file: %s because %s\n\n",
            db_name, sqlite3_errmsg(db));
  } else {
    fprintf(drv->log, "Opened file %s\n", db_name);
  }

  // Set the state for the driver
  drv->port = port;
  drv->db = db;
  drv->key = hash(db_name);
  drv->async_handle = 0;
  drv->prepared_stmts = NULL;
  drv->prepared_count = 0;
  drv->prepared_alloc = 0;

  drv->atom_blob = driver_mk_atom("blob");
  drv->atom_error = driver_mk_atom("error");
  drv->atom_columns = driver_mk_atom("columns");
  drv->atom_rows = driver_mk_atom("rows");
  drv->atom_null = driver_mk_atom("null");
  drv->atom_rowid = driver_mk_atom("rowid");
  drv->atom_ok = driver_mk_atom("ok");
  drv->atom_done = driver_mk_atom("done");
  drv->atom_unknown_cmd = driver_mk_atom("unknown_command");

  fflush(drv->log);
  return (ErlDrvData) drv;
}

// Driver Stop
static void stop(ErlDrvData handle) {
  Sqlite3Drv* drv = (Sqlite3Drv*) handle;
  unsigned int i;

  if (drv->prepared_stmts) {
    for (i = 0; i < drv->prepared_count; i++) {
      sqlite3_finalize(drv->prepared_stmts[i]);
    }
    driver_free(drv->prepared_stmts);
  }
  sqlite3_close(drv->db);
  fclose(drv->log);
  drv->log = NULL;

  driver_free(drv);
}

// Handle input from Erlang VM
static int control(
    ErlDrvData drv_data, unsigned int command, char *buf,
    int len, char **rbuf, int rlen) {
  Sqlite3Drv* drv = (Sqlite3Drv*) drv_data;
  switch (command) {
  case CMD_SQL_EXEC:
    sql_exec(drv, buf, len);
    break;
  case CMD_SQL_BIND_AND_EXEC:
    sql_bind_and_exec(drv, buf, len);
    break;
  case CMD_PREPARE:
    prepare(drv, buf, len);
    break;
  case CMD_PREPARED_BIND:
    prepared_bind(drv, buf, len);
    break;
  case CMD_PREPARED_STEP:
    prepared_step(drv, buf, len);
    break;
  case CMD_PREPARED_RESET:
    prepared_reset(drv, buf, len);
    break;
  case CMD_PREPARED_CLEAR_BINDINGS:
    prepared_clear_bindings(drv, buf, len);
    break;
  case CMD_PREPARED_FINALIZE:
    prepared_finalize(drv, buf, len);
    break;
  case CMD_PREPARED_COLUMNS:
    prepared_columns(drv, buf, len);
    break;
  case CMD_SQL_EXEC_SCRIPT:
    sql_exec_script(drv, buf, len);
    break;
  default:
    unknown(drv, buf, len);
  }
  return 0;
}

static inline int return_error(
    Sqlite3Drv *drv, int error_code, const char *error,
    ErlDrvTermData **p_dataset, int *p_term_count, int *p_term_allocated,
    int* p_error_code) {
	if (p_error_code) {
		*p_error_code = error_code;
	}
	dataset_realloc(p_dataset, p_term_count, p_term_allocated, 9);
	(*p_dataset)[*p_term_count - 9] = ERL_DRV_ATOM;
	(*p_dataset)[*p_term_count - 8] = drv->atom_error;
	(*p_dataset)[*p_term_count - 7] = ERL_DRV_INT;
	(*p_dataset)[*p_term_count - 6] = error_code;
	(*p_dataset)[*p_term_count - 5] = ERL_DRV_STRING;
	(*p_dataset)[*p_term_count - 4] = (ErlDrvTermData) error;
	(*p_dataset)[*p_term_count - 3] = strlen(error);
	(*p_dataset)[*p_term_count - 2] = ERL_DRV_TUPLE;
	(*p_dataset)[*p_term_count - 1] = 3;
	return 0;
}

static inline int output_error(
    Sqlite3Drv *drv, int error_code, const char *error) {
  int term_count = 2, term_allocated = 13;
  ErlDrvTermData *dataset = driver_alloc(sizeof(ErlDrvTermData) * term_allocated);
  dataset[0] = ERL_DRV_PORT;
  dataset[1] = driver_mk_port(drv->port);
  return_error(drv, error_code, error, &dataset, &term_count, &term_allocated, NULL);
  term_count += 2;
  dataset[11] = ERL_DRV_TUPLE;
  dataset[12] = 2;
  driver_output_term(drv->port, dataset, term_count);
  return 0;
}

static inline int output_db_error(Sqlite3Drv *drv) {
  return output_error(drv, sqlite3_errcode(drv->db), sqlite3_errmsg(drv->db));
}

static inline int output_ok(Sqlite3Drv *drv) {
  // Return {Port, ok}
  ErlDrvTermData spec[] = {
      ERL_DRV_PORT, driver_mk_port(drv->port),
      ERL_DRV_ATOM, drv->atom_ok,
      ERL_DRV_TUPLE, 2
  };
  return driver_output_term(drv->port, spec, sizeof(spec) / sizeof(spec[0]));
}

static inline async_sqlite3_command *make_async_command_statement(
    Sqlite3Drv *drv, sqlite3_stmt *statement) {
  async_sqlite3_command *async_cmd=
      (async_sqlite3_command *) driver_alloc(sizeof(async_sqlite3_command));
  memset(async_cmd, 0, sizeof(async_sqlite3_command));

  async_cmd->driver_data = drv;
  async_cmd->type = t_stmt;
  async_cmd->statement = statement;
  async_cmd->ptrs = listCreate();
  listSetFreeMethod(async_cmd->ptrs, driver_free_fun);
  async_cmd->binaries = listCreate();
  listSetFreeMethod(async_cmd->binaries, driver_free_binary_fun);
  return async_cmd;
}

static inline async_sqlite3_command *make_async_command_script(
    Sqlite3Drv *drv, char *script, int script_length) {
  async_sqlite3_command *async_cmd =
      (async_sqlite3_command *) driver_alloc(sizeof(async_sqlite3_command));
  char *script_copy = driver_alloc(sizeof(char) * script_length);
  memset(async_cmd, 0, sizeof(async_sqlite3_command));
  memcpy(script_copy, script, sizeof(char) * script_length);

  async_cmd->driver_data = drv;
  async_cmd->type = t_script;
  async_cmd->script = script_copy;
  async_cmd->end = script_copy + script_length;
  async_cmd->ptrs = listCreate();
  listSetFreeMethod(async_cmd->ptrs, driver_free_fun);
  async_cmd->binaries = listCreate();
  listSetFreeMethod(async_cmd->binaries, driver_free_binary_fun);
  return async_cmd;
}

static inline int sql_exec_statement(
    Sqlite3Drv *drv, sqlite3_stmt *statement) {
  async_sqlite3_command *async_command = make_async_command_statement(drv, statement);

#ifdef DEBUG
  fprintf(drv->log, "Driver async: %d %p\n", SQLITE_VERSION_NUMBER, async_command->statement);
  fflush(drv->log);
#endif

  if (sqlite3_threadsafe()) {
    drv->async_handle = driver_async(drv->port, &drv->key, sql_exec_async,
                                     async_command, sql_free_async);
  } else {
    sql_exec_async(async_command);
    ready_async((ErlDrvData) drv, (ErlDrvThreadData) async_command);
  }
  return 0;
}

static int sql_exec(Sqlite3Drv *drv, char *command, int command_size) {
  int result;
  const char *rest;
  sqlite3_stmt *statement;

#ifdef DEBUG
  fprintf(drv->log, "Preexec: %.*s\n", command_size, command);
  fflush(drv->log);
#endif
  result = sqlite3_prepare_v2(drv->db, command, command_size, &statement, &rest);
  if (result != SQLITE_OK) {
    return output_db_error(drv);
  } else if (statement == NULL) {
    return output_error(drv, SQLITE_MISUSE, "empty statement");
  }

  return sql_exec_statement(drv, statement);
}

static int sql_exec_script(Sqlite3Drv *drv, char *command, int command_size) {
  async_sqlite3_command *async_command = make_async_command_script(drv, command, command_size);

#ifdef DEBUG
  fprintf(drv->log, "Driver async: %d %p\n", SQLITE_VERSION_NUMBER, async_command->statement);
  fflush(drv->log);
#endif

  if (sqlite3_threadsafe()) {
    drv->async_handle = driver_async(drv->port, &drv->key, sql_exec_async,
                                     async_command, sql_free_async);
  } else {
    sql_exec_async(async_command);
    ready_async((ErlDrvData) drv, (ErlDrvThreadData) async_command);
  }
  return 0;
}

static inline int decode_and_bind_param(
    Sqlite3Drv *drv, char *buffer, int *p_index,
    sqlite3_stmt *statement, int param_index, int *p_type, int *p_size) {
  int result;
  sqlite3_int64 int64_val;
  double double_val;
  char* char_buf_val;
  long bin_size;

  ei_get_type(buffer, p_index, p_type, p_size);
  switch (*p_type) {
  case ERL_SMALL_INTEGER_EXT:
  case ERL_INTEGER_EXT:
  case ERL_SMALL_BIG_EXT:
  case ERL_LARGE_BIG_EXT:
    ei_decode_longlong(buffer, p_index, &int64_val);
    result = sqlite3_bind_int64(statement, param_index, int64_val);
    break;
  case ERL_FLOAT_EXT:
#ifdef NEW_FLOAT_EXT
  case NEW_FLOAT_EXT: // what's the difference?
#endif
    ei_decode_double(buffer, p_index, &double_val);
    result = sqlite3_bind_double(statement, param_index, double_val);
    break;
  case ERL_ATOM_EXT:
    // include space for null separator
    char_buf_val = driver_alloc((*p_size + 1) * sizeof(char));
    ei_decode_atom(buffer, p_index, char_buf_val);
    if (strncmp(char_buf_val, "null", 5) == 0) {
      result = sqlite3_bind_null(statement, param_index);
    }
    else {
      output_error(drv, SQLITE_MISUSE, "Non-null atom as parameter");
      return 1;
    }
    break;
  case ERL_STRING_EXT:
    // include space for null separator
    char_buf_val = driver_alloc((*p_size + 1) * sizeof(char));
    ei_decode_string(buffer, p_index, char_buf_val);
    result = sqlite3_bind_text(statement, param_index, char_buf_val, *p_size, &driver_free_fun);
    break;
  case ERL_BINARY_EXT:
    char_buf_val = driver_alloc(*p_size * sizeof(char));
    ei_decode_binary(buffer, p_index, char_buf_val, &bin_size);
    result = sqlite3_bind_text(statement, param_index, char_buf_val, *p_size, &driver_free_fun);
    break;
  case ERL_SMALL_TUPLE_EXT:
    // assume this is {blob, Blob}
    ei_get_type(buffer, p_index, p_type, p_size);
    ei_decode_tuple_header(buffer, p_index, p_size);
    if (*p_size != 2) {
      output_error(drv, SQLITE_MISUSE, "bad parameter type");
      return 1;
    }
    ei_skip_term(buffer, p_index); // skipped the atom 'blob'
    ei_get_type(buffer, p_index, p_type, p_size);
    if (*p_type != ERL_BINARY_EXT) {
      output_error(drv, SQLITE_MISUSE, "bad parameter type");
      return 1;
    }
    char_buf_val = driver_alloc(*p_size * sizeof(char));
    ei_decode_binary(buffer, p_index, char_buf_val, &bin_size);
    result = sqlite3_bind_blob(statement, param_index, char_buf_val, *p_size, &driver_free_fun);
    break;
  default:
    output_error(drv, SQLITE_MISUSE, "bad parameter type");
    return 1;
  }
  if (result != SQLITE_OK) {
    output_db_error(drv);
    return result;
  }
  return SQLITE_OK;
}

static int bind_parameters(
    Sqlite3Drv *drv, char *buffer, int buffer_size, int *p_index,
    sqlite3_stmt *statement, int *p_type, int *p_size) {
  // decoding parameters
  int i, cur_list_size = -1, param_index = 1, param_indices_are_explicit = 0, result = 0;
  long param_index_long;
  char param_name[MAXATOMLEN + 1]; // parameter names shouldn't be longer than 256!
  char *acc_string;
  result = ei_decode_list_header(buffer, p_index, &cur_list_size);
  if (result) {
    // probably all parameters are integers between 0 and 255
    // and the list was encoded as string (see ei documentation)
    ei_get_type(buffer, p_index, p_type, p_size);
    if (*p_type != ERL_STRING_EXT) {
      return output_error(drv, SQLITE_ERROR,
                          "error while binding parameters");
    }
    acc_string = driver_alloc(sizeof(char*) * (*p_size + 1));
    ei_decode_string(buffer, p_index, acc_string);
    for (param_index = 1; param_index <= *p_size; param_index++) {
      sqlite3_bind_int(statement, param_index, (int) acc_string[param_index - 1]);
    }
    driver_free(acc_string);
    return 0;
  }
  for (i = 0; i < cur_list_size; i++) {
    if (*p_index >= buffer_size) {
      return output_error(drv, SQLITE_ERROR,
                          "error while binding parameters");
    }
    ei_get_type(buffer, p_index, p_type, p_size);
    if (*p_type == ERL_SMALL_TUPLE_EXT) {
      int old_index = *p_index;
      // param with name or explicit index
      param_indices_are_explicit = 1;
      if (*p_size != 2) {
        return output_error(drv, SQLITE_MISUSE,
                            "tuple should contain index or name, and value");
      }
      ei_decode_tuple_header(buffer, p_index, p_size);
      ei_get_type(buffer, p_index, p_type, p_size);
      // first element of tuple is int (index), atom, or string (name)
      switch (*p_type) {
      case ERL_SMALL_INTEGER_EXT:
      case ERL_INTEGER_EXT:
        ei_decode_long(buffer, p_index, &param_index_long);
        param_index = param_index_long;
        break;
      case ERL_ATOM_EXT:
        ei_decode_atom(buffer, p_index, param_name);
        // insert zero terminator
        param_name[*p_size] = '\0';
        if (strncmp(param_name, "blob", 5) == 0) {
          // this isn't really a parameter name!
          *p_index = old_index;
          param_indices_are_explicit = 0;
          goto IMPLICIT_INDEX; // yuck
        }
        else {
          param_index = sqlite3_bind_parameter_index(statement, param_name);
        }
        break;
      case ERL_STRING_EXT:
        if (*p_size >= MAXATOMLEN) {
          return output_error(drv, SQLITE_TOOBIG, "parameter name too long");
        }
        ei_decode_string(buffer, p_index, param_name);
        // insert zero terminator
        param_name[*p_size] = '\0';
        param_index = sqlite3_bind_parameter_index(statement, param_name);
        break;
      default:
        return output_error(
            drv, SQLITE_MISMATCH,
            "parameter index must be given as integer, atom, or string");
      }
      result = decode_and_bind_param(
          drv, buffer, p_index, statement, param_index, p_type, p_size);
      if (result != SQLITE_OK) {
        return result; // error has already been output
      }
    }
    else {
      IMPLICIT_INDEX:
      if (param_indices_are_explicit) {
        return output_error(
            drv, SQLITE_MISUSE,
            "parameters without indices shouldn't follow indexed or named parameters");
      }

      result = decode_and_bind_param(
          drv, buffer, p_index, statement, param_index, p_type, p_size);
      if (result != SQLITE_OK) {
        return result; // error has already been output
      }
      ++param_index;
    }
  }
  return result;
}

static void get_columns(
    Sqlite3Drv *drv, sqlite3_stmt *statement, int column_count, int base,
    int *p_term_count, int *p_term_allocated, ErlDrvTermData **p_dataset) {
  int i;

  dataset_realloc(p_dataset, p_term_count, p_term_allocated, column_count * 3 + 3);

  for (i = 0; i < column_count; i++) {
    char *column_name = (char *) sqlite3_column_name(statement, i);
#ifdef DEBUG
    fprintf(drv->log, "Column: %s\n", column_name);
    fflush(drv->log);
#endif

    (*p_dataset)[base + (i * 3)] = ERL_DRV_STRING;
    (*p_dataset)[base + (i * 3) + 1] = (ErlDrvTermData) column_name;
    (*p_dataset)[base + (i * 3) + 2] = strlen(column_name);
  }
  (*p_dataset)[base + column_count * 3 + 0] = ERL_DRV_NIL;
  (*p_dataset)[base + column_count * 3 + 1] = ERL_DRV_LIST;
  (*p_dataset)[base + column_count * 3 + 2] = column_count + 1;
}

static int sql_bind_and_exec(Sqlite3Drv *drv, char *buffer, int buffer_size) {
  int result;
  int index = 0;
  int type, size;
  const char *rest;
  sqlite3_stmt *statement;
  long bin_size;
  char *command;

#ifdef DEBUG
  fprintf(drv->log, "Preexec: %.*s\n", buffer_size, buffer);
  fflush(drv->log);
#endif

  ei_decode_version(buffer, &index, NULL);
  result = ei_decode_tuple_header(buffer, &index, &size);
  if (result || (size != 2)) {
    return output_error(drv, SQLITE_MISUSE,
                        "Expected a tuple of SQL command and params");
  }

  // decode SQL statement
  ei_get_type(buffer, &index, &type, &size);
  // TODO support any iolists
  if (type != ERL_BINARY_EXT) {
    return output_error(drv, SQLITE_MISUSE,
                        "SQL should be sent as an Erlang binary");
  }

  command = driver_alloc(size * sizeof(char));
  ei_decode_binary(buffer, &index, command, &bin_size);
  // assert(bin_size == size)
  result = sqlite3_prepare_v2(drv->db, command, size, &statement, &rest);
  driver_free(command);

  if (result != SQLITE_OK) {
    return output_db_error(drv);
  } else if (statement == NULL) {
    return output_error(drv, SQLITE_MISUSE, "empty statement");
  }

  result = bind_parameters(drv, buffer, buffer_size, &index, statement, &type, &size);
  if (result == SQLITE_OK) {
    return sql_exec_statement(drv, statement);
  } else {
    return result; // error has already been output
  }
}

static void sql_free_async(void *_async_command) {
  async_sqlite3_command *async_command =
      (async_sqlite3_command *) _async_command;
  driver_free(async_command->dataset);

  async_command->driver_data->async_handle = 0;

  listRelease(async_command->ptrs);

  listRelease(async_command->binaries);

  if ((async_command->type == t_stmt) &&
      async_command->finalize_statement_on_free &&
      async_command->statement) {
    sqlite3_finalize(async_command->statement);
    async_command->statement = NULL;
  } else if (async_command->type == t_script) {
    driver_free(async_command->script);
  }
  driver_free(async_command);
}

static int sql_exec_one_statement(
    sqlite3_stmt *statement, async_sqlite3_command *async_command,
    int *p_term_count, int *p_term_allocated, ErlDrvTermData **p_dataset) {
  int column_count = sqlite3_column_count(statement);
  int row_count = 0, next_row;
  int base_term_count;
  Sqlite3Drv *drv = async_command->driver_data;
  list *ptrs_p = async_command->ptrs;
  list *binaries_p = async_command->binaries;
  // printf("\nsql_exec_one_statement. SQL:\n%s\n Term count: %d, terms alloc: %d\n", sqlite3_sql(statement), *p_term_count, *p_term_allocated);

  int i;

  if (column_count > 0) {
    dataset_realloc(p_dataset, p_term_count, p_term_allocated, 2);

    (*p_dataset)[*p_term_count - 2] = ERL_DRV_ATOM;
    (*p_dataset)[*p_term_count - 1] = drv->atom_columns;
    base_term_count = *p_term_count;
    get_columns(
        drv, statement, column_count, base_term_count, p_term_count, p_term_allocated, p_dataset);
    dataset_realloc(p_dataset, p_term_count, p_term_allocated, 4);

    (*p_dataset)[base_term_count + column_count * 3 + 3] = ERL_DRV_TUPLE;
    (*p_dataset)[base_term_count + column_count * 3 + 4] = 2;

    (*p_dataset)[base_term_count + column_count * 3 + 5] = ERL_DRV_ATOM;
    (*p_dataset)[base_term_count + column_count * 3 + 6] = drv->atom_rows;
  }

#ifdef DEBUG
  fprintf(drv->log, "Exec: %s\n", sqlite3_sql(statement));
  fflush(drv->log);
#endif

  while ((next_row = sqlite3_step(statement)) == SQLITE_ROW) {
    for (i = 0; i < column_count; i++) {
#ifdef DEBUG
      fprintf(drv->log, "Column %d type: %d\n", i, sqlite3_column_type(statement, i));
      fflush(drv->log);
#endif
      switch (sqlite3_column_type(statement, i)) {
      case SQLITE_INTEGER: {
        ErlDrvSInt64 *int64_ptr = driver_alloc(sizeof(ErlDrvSInt64));
        *int64_ptr = (ErlDrvSInt64) sqlite3_column_int64(statement, i);
        listAddNodeTail(ptrs_p, int64_ptr);

		dataset_realloc(p_dataset, p_term_count, p_term_allocated, 2);

        (*p_dataset)[*p_term_count - 2] = ERL_DRV_INT64;
        (*p_dataset)[*p_term_count - 1] = (ErlDrvTermData) int64_ptr;
        break;
      }
      case SQLITE_FLOAT: {
        double *float_ptr = driver_alloc(sizeof(double));
        *float_ptr = sqlite3_column_double(statement, i);
        listAddNodeTail(ptrs_p, float_ptr);

		dataset_realloc(p_dataset, p_term_count, p_term_allocated, 2);

        (*p_dataset)[*p_term_count - 2] = ERL_DRV_FLOAT;
        (*p_dataset)[*p_term_count - 1] = (ErlDrvTermData) float_ptr;
        break;
      }
      case SQLITE_BLOB: {
        int bytes = sqlite3_column_bytes(statement, i);
        ErlDrvBinary* binary = driver_alloc_binary(bytes);
        binary->orig_size = bytes;
        memcpy(binary->orig_bytes,
               sqlite3_column_blob(statement, i), bytes);
        listAddNodeTail(binaries_p, binary);

		dataset_realloc(p_dataset, p_term_count, p_term_allocated, 8);

        (*p_dataset)[*p_term_count - 8] = ERL_DRV_ATOM;
        (*p_dataset)[*p_term_count - 7] = drv->atom_blob;
        (*p_dataset)[*p_term_count - 6] = ERL_DRV_BINARY;
        (*p_dataset)[*p_term_count - 5] = (ErlDrvTermData) binary;
        (*p_dataset)[*p_term_count - 4] = bytes;
        (*p_dataset)[*p_term_count - 3] = 0;
        (*p_dataset)[*p_term_count - 2] = ERL_DRV_TUPLE;
        (*p_dataset)[*p_term_count - 1] = 2;
        break;
      }
      case SQLITE_TEXT: {
        int bytes = sqlite3_column_bytes(statement, i);
        ErlDrvBinary* binary = driver_alloc_binary(bytes);
        binary->orig_size = bytes;
        memcpy(binary->orig_bytes,
               sqlite3_column_blob(statement, i), bytes);
        listAddNodeTail(binaries_p, binary);

		dataset_realloc(p_dataset, p_term_count, p_term_allocated, 4);

        (*p_dataset)[*p_term_count - 4] = ERL_DRV_BINARY;
        (*p_dataset)[*p_term_count - 3] = (ErlDrvTermData) binary;
        (*p_dataset)[*p_term_count - 2] = bytes;
        (*p_dataset)[*p_term_count - 1] = 0;
        break;
      }
      case SQLITE_NULL: {
		dataset_realloc(p_dataset, p_term_count, p_term_allocated, 2);

        (*p_dataset)[*p_term_count - 2] = ERL_DRV_ATOM;
        (*p_dataset)[*p_term_count - 1] = drv->atom_null;
        break;
      }
      }
    }
	dataset_realloc(p_dataset, p_term_count, p_term_allocated, 2);

    (*p_dataset)[*p_term_count - 2] = ERL_DRV_TUPLE;
    (*p_dataset)[*p_term_count - 1] = column_count;

    row_count++;
  }

  if (next_row == SQLITE_BUSY) {
    return_error(drv, SQLITE_BUSY, "SQLite3 database is busy",
                 p_dataset, p_term_count,
                 p_term_allocated, &async_command->error_code);
    async_command->finalize_statement_on_free = 1;
    return next_row;
  }
  if (next_row != SQLITE_DONE) {
    return_error(drv, next_row, sqlite3_errmsg(drv->db),
                 p_dataset, p_term_count,
                 p_term_allocated, &async_command->error_code);
    async_command->finalize_statement_on_free = 1;
    return next_row;
  }

  if (column_count > 0) {
	dataset_realloc(p_dataset, p_term_count, p_term_allocated, 3+2+3);

    (*p_dataset)[*p_term_count - 8] = ERL_DRV_NIL;
    (*p_dataset)[*p_term_count - 7] = ERL_DRV_LIST;
    (*p_dataset)[*p_term_count - 6] = row_count + 1;

    (*p_dataset)[*p_term_count - 5] = ERL_DRV_TUPLE;
    (*p_dataset)[*p_term_count - 4] = 2;

    (*p_dataset)[*p_term_count - 3] = ERL_DRV_NIL;
    (*p_dataset)[*p_term_count - 2] = ERL_DRV_LIST;
    (*p_dataset)[*p_term_count - 1] = 3;
  } else if (sql_is_insert(sqlite3_sql(statement))) {
    ErlDrvSInt64 *rowid_ptr = driver_alloc(sizeof(ErlDrvSInt64));
    *rowid_ptr = (ErlDrvSInt64) sqlite3_last_insert_rowid(drv->db);
    listAddNodeTail(ptrs_p, rowid_ptr);

	dataset_realloc(p_dataset, p_term_count, p_term_allocated, 6);

    (*p_dataset)[*p_term_count - 6] = ERL_DRV_ATOM;
    (*p_dataset)[*p_term_count - 5] = drv->atom_rowid;
    (*p_dataset)[*p_term_count - 4] = ERL_DRV_INT64;
    (*p_dataset)[*p_term_count - 3] = (ErlDrvTermData) rowid_ptr;
    (*p_dataset)[*p_term_count - 2] = ERL_DRV_TUPLE;
    (*p_dataset)[*p_term_count - 1] = 2;
  } else {
	dataset_realloc(p_dataset, p_term_count, p_term_allocated, 2);

    (*p_dataset)[*p_term_count - 2] = ERL_DRV_ATOM;
    (*p_dataset)[*p_term_count - 1] = drv->atom_ok;
  }

#ifdef DEBUG
  fprintf(drv->log, "Total term count: %p %d, rows count: %dx%d\n", statement, *p_term_count, column_count, row_count);
  fflush(drv->log);
#endif
  async_command->finalize_statement_on_free = 1;

  return 0;
}

static void sql_exec_async(void *_async_command) {
  async_sqlite3_command *async_command =
      (async_sqlite3_command *) _async_command;

  sqlite3_stmt *statement = NULL;
  int result;
  const char *rest;
  const char *end;
  int num_statements = 0;
  int term_count = 0, term_allocated = 0;
  ErlDrvTermData *dataset = NULL;

  Sqlite3Drv *drv = async_command->driver_data;

  dataset_realloc(&dataset, &term_count, &term_allocated, 2);

  dataset[term_count - 2] = ERL_DRV_PORT;
  dataset[term_count - 1] = driver_mk_port(drv->port);

  switch (async_command->type) {
  case t_stmt:
    statement = async_command->statement;
    sql_exec_one_statement(statement, async_command, &term_count,
                           &term_allocated, &dataset);
    break;
  case t_script:
    rest = async_command->script;
    end = async_command->end;

    while ((rest < end) && !(async_command->error_code)) {
      if (statement) {
        sqlite3_finalize(statement);
      }
      result = sqlite3_prepare_v2(drv->db, rest, end - rest, &statement, &rest);
      if (result != SQLITE_OK) {
        num_statements++;
        return_error(drv, result, sqlite3_errmsg(drv->db), &dataset,
                     &term_count, &term_allocated, &async_command->error_code);
        break;
      } else if (statement == NULL) {
        break;
      } else {
        num_statements++;
        result = sql_exec_one_statement(statement, async_command, &term_count,
                                        &term_allocated, &dataset);
        if (result) {
          break;
        }
      }
    }

	dataset_realloc(&dataset, &term_count, &term_allocated, 3);

    dataset[term_count - 3] = ERL_DRV_NIL;
    dataset[term_count - 2] = ERL_DRV_LIST;
    dataset[term_count - 1] = num_statements + 1;
  }

	dataset_realloc(&dataset, &term_count, &term_allocated, 2);

  dataset[term_count - 2] = ERL_DRV_TUPLE;
  dataset[term_count - 1] = 2;

  // print_dataset(dataset, term_count);

  async_command->term_count = term_count;
  async_command->term_allocated = term_allocated;
  async_command->dataset = dataset;
}

static void sql_step_async(void *_async_command) {
  async_sqlite3_command *async_command =
      (async_sqlite3_command *) _async_command;
  int term_count = 0;
  int term_allocated = 0;
  ErlDrvTermData *dataset = NULL;
  Sqlite3Drv *drv = async_command->driver_data;

  int column_count = 0;
  sqlite3_stmt *statement = async_command->statement;

  list *ptrs = async_command->ptrs;
  list *binaries = async_command->binaries;
  int i;
  int result;

  switch(result = sqlite3_step(statement)) {
  case SQLITE_ROW:
    column_count = sqlite3_column_count(statement);
	dataset_realloc(&dataset, &term_count, &term_allocated, 2);
    dataset[term_count - 2] = ERL_DRV_PORT;
    dataset[term_count - 1] = driver_mk_port(drv->port);

    for (i = 0; i < column_count; i++) {
#ifdef DEBUG
      fprintf(drv->log, "Column %d type: %d\n", i, sqlite3_column_type(statement, i));
      fflush(drv->log);
#endif
      switch (sqlite3_column_type(statement, i)) {
      case SQLITE_INTEGER: {
        ErlDrvSInt64 *int64_ptr = driver_alloc(sizeof(ErlDrvSInt64));
        *int64_ptr = (ErlDrvSInt64) sqlite3_column_int64(statement, i);
        listAddNodeTail(ptrs, int64_ptr);

		dataset_realloc(&dataset, &term_count, &term_allocated, 2);
        dataset[term_count - 2] = ERL_DRV_INT64;
        dataset[term_count - 1] = (ErlDrvTermData) int64_ptr;
        break;
      }
      case SQLITE_FLOAT: {
        double *float_ptr = driver_alloc(sizeof(double));
        *float_ptr = sqlite3_column_double(statement, i);
        listAddNodeTail(ptrs, float_ptr);

		dataset_realloc(&dataset, &term_count, &term_allocated, 2);
        dataset[term_count - 2] = ERL_DRV_FLOAT;
        dataset[term_count - 1] = (ErlDrvTermData) float_ptr;
        break;
      }
      case SQLITE_BLOB: {
        int bytes = sqlite3_column_bytes(statement, i);
        ErlDrvBinary* binary = driver_alloc_binary(bytes);
        binary->orig_size = bytes;
        memcpy(binary->orig_bytes,
               sqlite3_column_blob(statement, i), bytes);
        listAddNodeTail(binaries, binary);

		dataset_realloc(&dataset, &term_count, &term_allocated, 8);

        dataset[term_count - 8] = ERL_DRV_ATOM;
        dataset[term_count - 7] = drv->atom_blob;
        dataset[term_count - 6] = ERL_DRV_BINARY;
        dataset[term_count - 5] = (ErlDrvTermData) binary;
        dataset[term_count - 4] = bytes;
        dataset[term_count - 3] = 0;
        dataset[term_count - 2] = ERL_DRV_TUPLE;
        dataset[term_count - 1] = 2;
        break;
      }
      case SQLITE_TEXT: {
        int bytes = sqlite3_column_bytes(statement, i);
        ErlDrvBinary* binary = driver_alloc_binary(bytes);
        binary->orig_size = bytes;
        memcpy(binary->orig_bytes,
               sqlite3_column_blob(statement, i), bytes);
        listAddNodeTail(binaries, binary);

		dataset_realloc(&dataset, &term_count, &term_allocated, 4);
        dataset[term_count - 4] = ERL_DRV_BINARY;
        dataset[term_count - 3] = (ErlDrvTermData) binary;
        dataset[term_count - 2] = bytes;
        dataset[term_count - 1] = 0;
        break;
      }
      case SQLITE_NULL: {
		dataset_realloc(&dataset, &term_count, &term_allocated, 2);
        dataset[term_count - 2] = ERL_DRV_ATOM;
        dataset[term_count - 1] = drv->atom_null;
        break;
      }
      }
    }
	dataset_realloc(&dataset, &term_count, &term_allocated, 2);
    dataset[term_count - 2] = ERL_DRV_TUPLE;
    dataset[term_count - 1] = column_count;

    async_command->ptrs = ptrs;
    async_command->binaries = binaries;
    break;
  case SQLITE_DONE:
	dataset_realloc(&dataset, &term_count, &term_allocated, 4);
    dataset[term_count - 4] = ERL_DRV_PORT;
    dataset[term_count - 3] = driver_mk_port(drv->port);
    dataset[term_count - 2] = ERL_DRV_ATOM;
    dataset[term_count - 1] = drv->atom_done;
    sqlite3_reset(statement);
    break;
  case SQLITE_BUSY:
    return_error(drv, SQLITE_BUSY, "SQLite3 database is busy",
                 &dataset, &term_count, &term_allocated,
                 &async_command->error_code);
    sqlite3_reset(statement);
    goto POPULATE_COMMAND;
    break;
  default:
    return_error(drv, result, sqlite3_errmsg(drv->db),
                 &dataset, &term_count, &term_allocated,
                 &async_command->error_code);
    sqlite3_reset(statement);
    goto POPULATE_COMMAND;
  }

  dataset_realloc(&dataset, &term_count, &term_allocated, 2);
  dataset[term_count - 2] = ERL_DRV_TUPLE;
  dataset[term_count - 1] = 2;

POPULATE_COMMAND:
  async_command->dataset = dataset;
  async_command->term_count = term_count;
  async_command->ptrs = ptrs;
  async_command->binaries = binaries;
  async_command->row_count = 1;
#ifdef DEBUG
  fprintf(drv->log, "Total term count: %p %d, columns count: %d\n", statement, term_count, column_count);
  fflush(drv->log);
#endif
}

static void ready_async(ErlDrvData drv_data, ErlDrvThreadData thread_data) {
  async_sqlite3_command *async_command =
      (async_sqlite3_command *) thread_data;
  Sqlite3Drv *drv = async_command->driver_data;

  int res = driver_output_term(drv->port,
                               async_command->dataset,
                               async_command->term_count);
  (void) res; // suppress unused warning
#ifdef DEBUG
  if (res != 1) {
    fprintf(drv->log, "driver_output_term returned %d\n", res);
    fprint_dataset(drv->log, async_command->dataset, async_command->term_count);
  }

  fprintf(drv->log, "Total term count: %p %d, rows count: %d (%d)\n", async_command->statement, async_command->term_count, async_command->row_count, res);
  fflush(drv->log);
#endif
  sql_free_async(async_command);
}

static int prepare(Sqlite3Drv *drv, char *command, int command_size) {
  int result;
  const char *rest;
  sqlite3_stmt *statement;
  ErlDrvTermData spec[6];

#ifdef DEBUG
  fprintf(drv->log, "Preparing statement: %.*s\n", command_size, command);
  fflush(drv->log);
#endif
  result = sqlite3_prepare_v2(drv->db, command, command_size, &statement, &rest);
  if (result != SQLITE_OK) {
    return output_db_error(drv);
  } else if (statement == NULL) {
    return output_error(drv, SQLITE_MISUSE, "empty statement");
  }

  if (drv->prepared_count >= drv->prepared_alloc) {
    drv->prepared_alloc =
        (drv->prepared_alloc != 0) ? 2*drv->prepared_alloc : 4;
    drv->prepared_stmts =
        driver_realloc(drv->prepared_stmts,
                       drv->prepared_alloc * sizeof(sqlite3_stmt *));
  }
  drv->prepared_stmts[drv->prepared_count] = statement;
  drv->prepared_count++;

  spec[0] = ERL_DRV_PORT;
  spec[1] = driver_mk_port(drv->port);
  spec[2] = ERL_DRV_UINT;
  spec[3] = drv->prepared_count - 1;
  spec[4] = ERL_DRV_TUPLE;
  spec[5] = 2;
  return driver_output_term(drv->port, spec, sizeof(spec) / sizeof(spec[0]));
}

static int prepared_bind(Sqlite3Drv *drv, char *buffer, int buffer_size) {
  int result;
  unsigned int prepared_index;
  long long_prepared_index;
  int index = 0, type, size;
  sqlite3_stmt *statement;

#ifdef DEBUG
  fprintf(drv->log, "Finalizing prepared statement: %.*s\n", buffer_size, buffer);
  fflush(drv->log);
#endif

  ei_decode_version(buffer, &index, NULL);
  ei_decode_tuple_header(buffer, &index, &size);
  // assert(size == 2);
  ei_decode_long(buffer, &index, &long_prepared_index);
  prepared_index = (unsigned int) long_prepared_index;

  if (prepared_index >= drv->prepared_count) {
    return output_error(drv, SQLITE_MISUSE,
                        "Trying to bind non-existent prepared statement");
  }

  statement = drv->prepared_stmts[prepared_index];
  result =
      bind_parameters(drv, buffer, buffer_size, &index, statement, &type, &size);
  if (result == SQLITE_OK) {
    return output_ok(drv);
  } else {
    return result; // error has already been output
  }
}

static int prepared_columns(Sqlite3Drv *drv, char *buffer, int buffer_size) {
  unsigned int prepared_index;
  long long_prepared_index;
  int index = 0, term_count = 0, term_allocated = 0, column_count;
  sqlite3_stmt *statement;
  ErlDrvTermData *dataset = NULL;

  ei_decode_version(buffer, &index, NULL);
  ei_decode_long(buffer, &index, &long_prepared_index);
  prepared_index = (unsigned int) long_prepared_index;

  if (prepared_index >= drv->prepared_count) {
#ifdef DEBUG
    fprintf(drv->log, "Tried to get columns for prepared statement #%d, but maximum possible is #%d\n", prepared_index, drv->prepared_count - 1);
    fflush(drv->log);
#endif      
    return output_error(drv, SQLITE_MISUSE,
                        "Trying to reset non-existent prepared statement");
  }

#ifdef DEBUG
  fprintf(drv->log, "Getting the columns for prepared statement #%d\n", prepared_index);
  fflush(drv->log);
#endif

  statement = drv->prepared_stmts[prepared_index];

  dataset_realloc(&dataset, &term_count, &term_allocated, 4); 

  dataset[term_count - 4] = ERL_DRV_PORT;
  dataset[term_count - 3] = driver_mk_port(drv->port);

  column_count = sqlite3_column_count(statement);

  get_columns(
      drv, statement, column_count, 2, &term_count, &term_allocated, &dataset);
  dataset[term_count - 2] = ERL_DRV_TUPLE;
  dataset[term_count - 1] = 2;

  return driver_output_term(drv->port, dataset, term_count);
}

static int prepared_step(Sqlite3Drv *drv, char *buffer, int buffer_size) {
  unsigned int prepared_index;
  long long_prepared_index;
  int index = 0;
  sqlite3_stmt *statement;
  async_sqlite3_command *async_command;

  ei_decode_version(buffer, &index, NULL);
  ei_decode_long(buffer, &index, &long_prepared_index);
  prepared_index = (unsigned int) long_prepared_index;

  if (prepared_index >= drv->prepared_count) {
#ifdef DEBUG
    fprintf(drv->log, "Tried to make a step in prepared statement #%d, but maximum possible is #%d\n", prepared_index, drv->prepared_count - 1);
    fflush(drv->log);
#endif      
    return output_error(drv, SQLITE_MISUSE,
                        "Trying to evaluate non-existent prepared statement");
  }

#ifdef DEBUG
  fprintf(drv->log, "Making a step in prepared statement #%d\n", prepared_index);
  fflush(drv->log);
#endif

  statement = drv->prepared_stmts[prepared_index];
  async_command = make_async_command_statement(drv, statement);

  if (sqlite3_threadsafe()) {
    drv->async_handle = driver_async(drv->port, &drv->key, sql_step_async,
                                     async_command, sql_free_async);
  } else {
    sql_step_async(async_command);
    ready_async((ErlDrvData) drv, (ErlDrvThreadData) async_command);
  }
  return 0;
}

static int prepared_reset(Sqlite3Drv *drv, char *buffer, int buffer_size) {
  unsigned int prepared_index;
  long long_prepared_index;
  int index = 0;
  sqlite3_stmt *statement;

  ei_decode_version(buffer, &index, NULL);
  ei_decode_long(buffer, &index, &long_prepared_index);
  prepared_index = (unsigned int) long_prepared_index;

  if (prepared_index >= drv->prepared_count) {
#ifdef DEBUG
    fprintf(drv->log, "Tried to reset prepared statement #%d, but maximum possible is #%d\n", prepared_index, drv->prepared_count - 1);
    fflush(drv->log);
#endif      
    return output_error(drv, SQLITE_MISUSE,
                        "Trying to reset non-existent prepared statement");
  }

#ifdef DEBUG
  fprintf(drv->log, "Resetting prepared statement #%d\n", prepared_index);
  fflush(drv->log);
#endif
  // don't bother about error code, any errors should already be shown by step
  statement = drv->prepared_stmts[prepared_index];
  sqlite3_reset(statement);
  return output_ok(drv);
}

static int prepared_clear_bindings(Sqlite3Drv *drv, char *buffer, int buffer_size) {
  unsigned int prepared_index;
  long long_prepared_index;
  int index = 0;
  sqlite3_stmt *statement;

  ei_decode_version(buffer, &index, NULL);
  ei_decode_long(buffer, &index, &long_prepared_index);
  prepared_index = (unsigned int) long_prepared_index;

  if (prepared_index >= drv->prepared_count) {
#ifdef DEBUG
    fprintf(drv->log, "Tried to clear bindings of prepared statement #%d, but maximum possible is #%d\n", prepared_index, drv->prepared_count - 1);
    fflush(drv->log);
#endif      
    return output_error(drv, SQLITE_MISUSE,
                        "Trying to clear bindings of non-existent prepared statement");
  }

#ifdef DEBUG
  fprintf(drv->log, "Clearing bindings of prepared statement #%d\n", prepared_index);
  fflush(drv->log);
#endif
  statement = drv->prepared_stmts[prepared_index];
  sqlite3_clear_bindings(statement);
  return output_ok(drv);
}

static int prepared_finalize(Sqlite3Drv *drv, char *buffer, int buffer_size) {
  unsigned int prepared_index;
  long long_prepared_index;
  int index = 0;

  ei_decode_version(buffer, &index, NULL);
  ei_decode_long(buffer, &index, &long_prepared_index);
  prepared_index = (unsigned int) long_prepared_index;

  if (prepared_index >= drv->prepared_count) {
#ifdef DEBUG
    fprintf(drv->log, "Tried to finalize prepared statement #%d, but maximum possible is #%d\n", prepared_index, drv->prepared_count - 1);
    fflush(drv->log);
#endif      
    return output_error(drv, SQLITE_MISUSE,
                        "Trying to finalize non-existent prepared statement");
  }

#ifdef DEBUG
  fprintf(drv->log, "Finalizing prepared statement #%d\n", prepared_index);
  fflush(drv->log);
#endif
  // finalize the statement and make sure it isn't accidentally executed again
  sqlite3_finalize(drv->prepared_stmts[prepared_index]);
  drv->prepared_stmts[prepared_index] = NULL;

  // if the statement is at the end of the array, space can be reused;
  // otherwise don't bother
  if (prepared_index == drv->prepared_count - 1) {
    drv->prepared_count--;
  }
  return output_ok(drv);
}

// Unknown Command
static int unknown(Sqlite3Drv *drv, char *command, int command_size) {
  // Return {Port, error, unknown_command}
  ErlDrvTermData spec[] = {
      ERL_DRV_PORT, driver_mk_port(drv->port),
      ERL_DRV_ATOM, drv->atom_error,
      ERL_DRV_INT, (ErlDrvTermData) ((ErlDrvSInt) -1),
      ERL_DRV_ATOM, drv->atom_unknown_cmd,
      ERL_DRV_TUPLE, 4
  };
  return driver_output_term(drv->port, spec, sizeof(spec) / sizeof(spec[0]));
}

#ifndef max // macro in Windows
static inline int max(int a, int b) {
  return a >= b ? a : b;
}
#endif

static inline int sql_is_insert(const char *sql) {
  // neither strcasestr nor strnicmp are portable, so have to do this
  int i;
  char *insert = "insert";
  for (i = 0; i < 6; i++) {
    if ((tolower(sql[i]) != insert[i]) && (sql[i] != ' '))
      return 0;
  }
  return 1;
}

static void dataset_realloc(ErlDrvTermData **dataset, int *count, int *allocated, int size) {
	*count += size;
    if (*count > *allocated) {
      *allocated = max(*count, *allocated *2);
      *dataset = driver_realloc(*dataset, sizeof(ErlDrvTermData) * *allocated);
    }
}

#ifdef DEBUG
static void fprint_dataset(FILE* log, ErlDrvTermData *dataset, int term_count) {
  int i = 0, stack_size = 0;
  ErlDrvUInt length;

  fprintf(log, "\nPrinting dataset\n");
  while (i < term_count) {
    switch (dataset[i]) {
    case ERL_DRV_NIL:
      fprintf(log, "%d: []", i);
      i++;
      stack_size++;
      break;
    case ERL_DRV_ATOM:
      fprintf(log, "%d-%d: an atom", i, i+1);
      i += 2;
      stack_size++;
      break;
    case ERL_DRV_INT:
      fprintf(log, "%d-%d: int %ld", i, i+1, (ErlDrvSInt) dataset[i+1]);
      i += 2;
      stack_size++;
      break;
    case ERL_DRV_PORT:
      fprintf(log, "%d-%d: a port", i, i+1);
      i += 2;
      stack_size++;
      break;
    case ERL_DRV_BINARY:
      fprintf(log, "%d-%d: a binary (length %lu, offset %lu)",
             i, i+3, (ErlDrvUInt) dataset[i+2], (ErlDrvUInt) dataset[i+3]);
      i += 4;
      stack_size++;
      break;
    case ERL_DRV_BUF2BINARY:
      fprintf(log, "%d-%d: a string used as binary (length %lu)", i, i+2, (ErlDrvUInt) dataset[i+2]);
      i += 3;
      stack_size++;
      break;
    case ERL_DRV_STRING:
      fprintf(log, "%d-%d: a string (length %lu)", i, i+2, (ErlDrvUInt) dataset[i+2]);
      i += 3;
      stack_size++;
      break;
    case ERL_DRV_TUPLE:
      length = (ErlDrvUInt) dataset[i+1];
      fprintf(log, "%d-%d: a tuple (size %lu)", i, i+1, length);
      i += 2;
      stack_size -= length - 1;
      break;
    case ERL_DRV_LIST:
      length = (ErlDrvUInt) dataset[i+1];
      fprintf(log, "%d-%d: a list (length %lu)", i, i+1, length);
      i += 2;
      stack_size -= length - 1;
      break;
    case ERL_DRV_PID:
      fprintf(log, "%d-%d: a pid", i, i+1);
      i += 2;
      stack_size++;
      break;
    case ERL_DRV_STRING_CONS:
      length = (ErlDrvUInt) dataset[i+2];
      fprintf(log, "%d-%d: a string inside surrounding list (length %lu)", i, i+2, length);
      i += 3;
      break;
    case ERL_DRV_FLOAT:
      fprintf(log, "%d-%d: float %f", i, i+1, (double) dataset[i+1]);
      i += 2;
      stack_size++;
      break;
    case ERL_DRV_EXT2TERM:
      fprintf(log, "%d-%d: a term in external format of length %lu", i, i+1, (ErlDrvUInt) dataset[i+1]);
      i += 2;
      stack_size++;
      break;
    case ERL_DRV_INT64:
#if defined(_MSC_VER)
      fprintf(log, "%d-%d: int %I64d", i, i+1, (ErlDrvSInt64) dataset[i+1]);
#else
      fprintf(log, "%d-%d: int %lld", i, i+1, (ErlDrvSInt64) dataset[i+1]);
#endif
      i += 2;
      stack_size++;
      break;
    case ERL_DRV_UINT64:
#if defined(_MSC_VER)
      fprintf(log, "%d-%d: int %I64lu", i, i+1, (ErlDrvUInt64) dataset[i+1]);
#else
      fprintf(log, "%d-%d: int %llu", i, i+1, (ErlDrvUInt64) dataset[i+1]);
#endif
      i += 2;
      stack_size++;
      break;
    default:
      fprintf(log, "%d: unexpected type", i);
      i++;
      break;
    }
    fprintf(log, ".\tStack size: %d\n", stack_size);
  }
}
#endif

