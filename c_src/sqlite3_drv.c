#include "sqlite3_drv.h"
#include "ei.h"
#include "assert.h"

// MSVC needs "__inline" instead of "inline" in C-source files.
#if defined(_MSC_VER)
#  define inline __inline
#endif

static ErlDrvEntry basic_driver_entry = {
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
    ERL_DRV_EXTENDED_MAJOR_VERSION, /* ERL_DRV_EXTENDED_MINOR_VERSION */
    ERL_DRV_FLAG_USE_PORT_LOCKING /* ERL_DRV_FLAGs */
};

DRIVER_INIT(basic_driver) {
  return &basic_driver_entry;
}

static int print_dataset(ErlDrvTermData *dataset, int term_count);
static inline ptr_list *add_to_ptr_list(ptr_list *list, void *value_ptr);
static inline void free_ptr_list(ptr_list *list);

// Driver Start
static ErlDrvData start(ErlDrvPort port, char* cmd) {
  sqlite3_drv_t* retval = (sqlite3_drv_t*) driver_alloc(sizeof(sqlite3_drv_t));
  struct sqlite3 *db = 0;
  int status = 0;

  retval->log = fopen("/tmp/erlang-sqlite3-drv.log", "a+");
  if (!retval->log) {
    fprintf(stderr, "Can't create log file\n");
  }

  fprintf(retval->log,
      "--- Start erlang-sqlite3 driver\nCommand line: [%s]\n", cmd);

  const char *db_name = strstr(cmd, " ");
  if (!db_name) {
    fprintf(retval->log,
        "ERROR: DB name should be passed at command line\n");
    db_name = DB_PATH;
  } else {
    ++db_name; // move to first character after ' '
  }

  // Create and open the database
  sqlite3_open(db_name, &db);
  status = sqlite3_errcode(db);

  if (status != SQLITE_OK) {
    fprintf(retval->log, "ERROR: Unable to open file: %s because %s\n\n",
            db_name, sqlite3_errmsg(db));
  } else {
    fprintf(retval->log, "Opened file %s\n", db_name);
  }

  // Set the state for the driver
  retval->port = port;
  retval->db = db;
  retval->key = 42; //FIXME: Just a magic number, make real key

  retval->atom_error = driver_mk_atom("error");
  retval->atom_columns = driver_mk_atom("columns");
  retval->atom_rows = driver_mk_atom("rows");
  retval->atom_null = driver_mk_atom("null");
  retval->atom_id = driver_mk_atom("id");
  retval->atom_ok = driver_mk_atom("ok");
  retval->atom_unknown_cmd = driver_mk_atom("unknown_command");

  fflush(retval->log);
  return (ErlDrvData) retval;
}

// Driver Stop
static void stop(ErlDrvData handle) {
  sqlite3_drv_t* driver_data = (sqlite3_drv_t*) handle;

  sqlite3_close(driver_data->db);
  fclose(driver_data->log);
  driver_data->log = 0;

  driver_free(driver_data);
}

// Handle input from Erlang VM
static int control(ErlDrvData drv_data, unsigned int command, char *buf,
    int len, char **rbuf, int rlen) {
  sqlite3_drv_t* driver_data = (sqlite3_drv_t*) drv_data;
  switch (command) {
  case CMD_SQL_EXEC:
    sql_exec(driver_data, buf, len);
    break;
  case CMD_SQL_BIND_AND_EXEC:
    sql_bind_and_exec(driver_data, buf, len);
    break;
  default:
    unknown(driver_data, buf, len);
  }
  return 0;
}

static inline int return_error(sqlite3_drv_t *drv, const char *error,
    ErlDrvTermData **spec, int *term_count) {
  *spec = (ErlDrvTermData *) malloc(9 * sizeof(ErlDrvTermData));
  (*spec)[0] = ERL_DRV_PORT;
  (*spec)[1] = driver_mk_port(drv->port);
  (*spec)[2] = ERL_DRV_ATOM;
  (*spec)[3] = drv->atom_error;
  (*spec)[4] = ERL_DRV_STRING;
  (*spec)[5] = (ErlDrvTermData) error;
  (*spec)[6] = strlen(error);
  (*spec)[7] = ERL_DRV_TUPLE;
  (*spec)[8] = 3;
  *term_count = 9;
  return 0;
}

static inline int output_error(sqlite3_drv_t *drv, const char *error) {
  ErlDrvTermData *dataset;
  int term_count;
  return_error(drv, error, &dataset, &term_count);
  driver_output_term(drv->port, dataset, term_count);
  return 0;
}

static inline int output_db_error(sqlite3_drv_t *drv) {
  return output_error(drv, sqlite3_errmsg(drv->db));
}

static inline int sql_exec_statement(sqlite3_drv_t *drv, sqlite3_stmt *statement) {
  async_sqlite3_command *async_command =
    (async_sqlite3_command *) calloc(1, sizeof(async_sqlite3_command));
  async_command->driver_data = drv;
  async_command->statement = statement;

  // fprintf(drv->log, "Driver async: %d %p\n", SQLITE_VERSION_NUMBER, async_command->statement);
  // fflush(drv->log);

  if (sqlite3_threadsafe()) {
    drv->async_handle =
      driver_async(drv->port, &drv->key, sql_exec_async,
             async_command, sql_free_async);
  } else {
    sql_exec_async(async_command);
    ready_async((ErlDrvData) drv, (ErlDrvThreadData) async_command);
    sql_free_async(async_command);
  }
  return 0;
}

static int sql_exec(sqlite3_drv_t *drv, char *command, int command_size) {
  int result;
  char *rest = NULL;
  sqlite3_stmt *statement;

  // fprintf(drv->log, "Preexec: %.*s\n", command_size, command);
  // fflush(drv->log);
  result = sqlite3_prepare_v2(drv->db, command, command_size, &statement,
                              (const char **) &rest);
  if (result != SQLITE_OK) {
    return output_db_error(drv);
  }

  return sql_exec_statement(drv, statement);
}

static inline int decode_and_bind_param(
    sqlite3_drv_t *drv, char *buffer, int *index, sqlite3_stmt *statement, int param_index, int *type, int *size) {
  int result;
  ei_get_type(buffer, index, type, size);
  long long_val;
  sqlite3_int64 int64_val;
  double double_val;
  char* char_buf_val;
  switch (*type) {
//  case ERL_SMALL_INTEGER_EXT:
//    ei_decode_long(buffer, index, &long_val);
//    result = sqlite3_bind_int(statement, param_index, long_val);
//    break;
  case ERL_SMALL_INTEGER_EXT:
  case ERL_INTEGER_EXT:
  case ERL_SMALL_BIG_EXT:
  case ERL_LARGE_BIG_EXT:
    ei_decode_longlong(buffer, index, &int64_val);
    result = sqlite3_bind_int64(statement, param_index, int64_val);
    break;
  case ERL_FLOAT_EXT:
  case NEW_FLOAT_EXT: // what's the difference?
    ei_decode_double(buffer, index, &double_val);
    result = sqlite3_bind_double(statement, param_index, double_val);
    break;
  case ERL_ATOM_EXT:
    char_buf_val = malloc((*size + 1) * sizeof(char));
    ei_decode_atom(buffer, index, char_buf_val);
    if (strncmp(char_buf_val, "null", 5) == 0) {
      result = sqlite3_bind_null(statement, param_index);
    }
    else {
      output_error(drv, "Non-null atom as parameter");
      return 1;
    }
    break;
  case ERL_STRING_EXT:
    char_buf_val = malloc((*size + 1) * sizeof(char)); // space for null separator
    ei_decode_string(buffer, index, char_buf_val);
    result = sqlite3_bind_text(statement, param_index, char_buf_val, *size, &free);
    break;
  case ERL_BINARY_EXT:
    char_buf_val = malloc(*size * sizeof(char));
    ei_decode_binary(buffer, index, char_buf_val, size);
    result = sqlite3_bind_text(statement, param_index, char_buf_val, *size, &free);
    break;
  case ERL_SMALL_TUPLE_EXT:
    // assume this is {blob, Blob}
    ei_get_type(buffer, index, type, size);
    ei_decode_tuple_header(buffer, index, size);
    assert (*size == 2);
    ei_skip_term(buffer, index); // skipped the atom 'blob'
    ei_get_type(buffer, index, type, size);
    assert (*type == ERL_BINARY_EXT);
    char_buf_val = malloc(*size * sizeof(char));
    ei_decode_binary(buffer, index, char_buf_val, size);
    result = sqlite3_bind_blob(statement, param_index, char_buf_val, *size, &free);
    break;
  default:
    output_error(drv, "bad parameter type");
    return 1;
  }
  if (result != SQLITE_OK) {
    output_db_error(drv);
    return result;
  }
  return SQLITE_OK;
}

static int sql_bind_and_exec(sqlite3_drv_t *drv, char *buffer, int buffer_size) {
  int result;
  int index = 0;
  int type, size;
  char *rest = NULL;
  sqlite3_stmt *statement;

  // fprintf(drv->log, "Preexec: %.*s\n", command_size, command);
  // fflush(drv->log);

  ei_decode_version(buffer, &index, NULL);
  result = ei_decode_tuple_header(buffer, &index, &size);
  if (size != 2) {
    return output_error(drv, "bad argument");
  }

  // decode SQL statement
  ei_get_type(buffer, &index, &type, &size);
  if (type != ERL_BINARY_EXT) {
    return output_error(drv, "bad argument");
  }

  char *command = malloc(size * sizeof(char));
  ei_decode_binary(buffer, &index, command, &size);
  // printf("size: %d, command: %.*s\n", size, size, command);
  result = sqlite3_prepare_v2(drv->db, command, size, &statement,
                              (const char **) &rest);
  free(command);

  if (result != SQLITE_OK) {
    return output_db_error(drv);
  }

  // decoding parameters
  int i, cur_list_size = -1, param_index = 1, param_indices_are_explicit = 0;
  char param_name[MAXATOMLEN + 1]; // parameter names shouldn't be longer than 256!
  while (index < buffer_size) {
    ei_decode_list_header(buffer, &index, &cur_list_size);
    // note the finish condition; the last element is the tail and we shouldn't decode it!
    for (i = 0; i < cur_list_size; i++) {
      ei_get_type(buffer, &index, &type, &size);
      if (type == ERL_SMALL_TUPLE_EXT) {
        int old_index = index;
        // param with name or explicit index
        param_indices_are_explicit = 1;
        if (size != 2) {
          return output_error(drv, "bad argument");
        }
        ei_decode_tuple_header(buffer, &index, &size);
        ei_get_type(buffer, &index, &type, &size);
        // first element of tuple is int (index), atom, or string (name)
        switch (type) {
        case ERL_SMALL_INTEGER_EXT:
          ei_decode_long(buffer, &index, &param_index);
          break;
        case ERL_ATOM_EXT:
          ei_decode_atom(buffer, &index, param_name);
          // insert zero terminator
          param_name[size] = '\0';
          if (strncmp(param_name, "blob", 5) == 0) {
            // this isn't really a parameter name!
            index = old_index;
            param_indices_are_explicit = 0;
            goto IMPLICIT_INDEX; // yuck
          }
          else {
            param_index = sqlite3_bind_parameter_index(statement, param_name);
          }
          break;
        case ERL_STRING_EXT:
          if (size >= MAXATOMLEN) {
            return output_error(drv, "parameter name too long");
          }
          ei_decode_string(buffer, &index, param_name);
          // insert zero terminator
          param_name[size] = '\0';
          param_index = sqlite3_bind_parameter_index(statement, param_name);
          break;
        default:
          return output_error(drv, "parameter index must be given as integer, atom, or string");
        }
        result = decode_and_bind_param(drv, buffer, &index, statement, param_index, &type, &size);
        if (result != SQLITE_OK) {
          return result; // error has already been output
        }
      }
      else {
        IMPLICIT_INDEX:
        if (param_indices_are_explicit) {
          return output_error(drv,
            "parameters without indices shouldn't follow indexed or named parameters");
        }

        result = decode_and_bind_param(drv, buffer, &index, statement, param_index, &type, &size);
        if (result != SQLITE_OK) {
          return result; // error has already been output
        }
        ++param_index;
      }
    }
  }

  return sql_exec_statement(drv, statement);
}

static void sql_free_async(void *_async_command) {
  int i;
  async_sqlite3_command *async_command =
    (async_sqlite3_command *) _async_command;
  free(async_command->dataset);

  async_command->driver_data->async_handle = 0;

  free_ptr_list(async_command->ptrs);
  for (i = 0; i < async_command->binaries_count; i++) {
    driver_free_binary(async_command->binaries[i]);
  }
  if (async_command->binaries) {
    free(async_command->binaries);
  }
  if (async_command->statement) {
    sqlite3_finalize(async_command->statement);
  }
  free(async_command);
}

static void sql_exec_async(void *_async_command) {
  async_sqlite3_command *async_command =
    (async_sqlite3_command *) _async_command;
  int term_count = async_command->term_count;
  int term_allocated = term_count <= 4 ? 4 : term_count;
  ErlDrvTermData *dataset = malloc(sizeof(*dataset) * term_allocated);
  int row_count = async_command->row_count;
  sqlite3_drv_t *drv = async_command->driver_data;

  int result, next_row, column_count;
  char *error = NULL;
  char *rest = NULL;
  sqlite3_stmt *statement = async_command->statement;

  ptr_list *ptrs = NULL;

  ErlDrvBinary **binaries = NULL;
  int binaries_count = 0;
  int i;

  column_count = sqlite3_column_count(statement);

  term_count += 2;
  if (term_count > term_allocated) {
    term_allocated =
      (term_count >= term_allocated*2) ? term_count : term_allocated*2;
    dataset = realloc(dataset, sizeof(*dataset) * term_allocated);
  }
  dataset[term_count - 2] = ERL_DRV_PORT;
  dataset[term_count - 1] = driver_mk_port(drv->port);

  if (column_count > 0) {
    int base = term_count;
    term_count += 2 + column_count * 3 + 1 + 2 + 2 + 2;
    if (term_count > term_allocated) {
      term_allocated =
        (term_count >= term_allocated*2) ? term_count : term_allocated*2;
      dataset = realloc(dataset, sizeof(*dataset) * term_allocated);
    }
    dataset[base] = ERL_DRV_ATOM;
    dataset[base + 1] = drv->atom_columns;
    for (i = 0; i < column_count; i++) {
      char *column_name = (char *) sqlite3_column_name(statement, i);
      // fprintf(drv->log, "Column: %s\n", column_name);
      // fflush(drv->log);

      dataset[base + 2 + (i * 3)] = ERL_DRV_STRING;
      dataset[base + 2 + (i * 3) + 1] = (ErlDrvTermData) column_name;
      dataset[base + 2 + (i * 3) + 2] = strlen(column_name);
    }
    dataset[base + 2 + column_count * 3 + 0] = ERL_DRV_NIL;
    dataset[base + 2 + column_count * 3 + 1] = ERL_DRV_LIST;
    dataset[base + 2 + column_count * 3 + 2] = column_count + 1;
    dataset[base + 2 + column_count * 3 + 3] = ERL_DRV_TUPLE;
    dataset[base + 2 + column_count * 3 + 4] = 2;

    dataset[base + 2 + column_count * 3 + 5] = ERL_DRV_ATOM;
    dataset[base + 2 + column_count * 3 + 6] = drv->atom_rows;
  }

  // fprintf(drv->log, "Exec: %s\n", sqlite3_sql(statement));
  // fflush(drv->log);

  while ((next_row = sqlite3_step(statement)) == SQLITE_ROW) {
    for (i = 0; i < column_count; i++) {
      // fprintf(drv->log, "Column %d type: %d\n", i, sqlite3_column_type(statement, i));
      // fflush(drv->log);
      switch (sqlite3_column_type(statement, i)) {
      case SQLITE_INTEGER: {
        ErlDrvSInt64 *int64_ptr = malloc(sizeof(ErlDrvSInt64));
        *int64_ptr = (ErlDrvSInt64) sqlite3_column_int64(statement, i);
        ptrs = add_to_ptr_list(ptrs, int64_ptr);

        term_count += 2;
        if (term_count > term_allocated) {
          term_allocated =
            (term_count >= term_allocated*2) ? term_count : term_allocated*2;
          dataset = realloc(dataset, sizeof(*dataset) * term_allocated);
        }
        dataset[term_count - 2] = ERL_DRV_INT64;
        dataset[term_count - 1] = (ErlDrvTermData) int64_ptr;
        break;
      }
      case SQLITE_FLOAT: {
        double *float_ptr = malloc(sizeof(double));
        *float_ptr = sqlite3_column_double(statement, i);
        ptrs = add_to_ptr_list(ptrs, float_ptr);

        term_count += 2;
        if (term_count > term_allocated) {
          term_allocated =
            (term_count >= term_allocated*2) ? term_count : term_allocated*2;
          dataset = realloc(dataset, sizeof(*dataset) * term_allocated);
        }
        dataset[term_count - 2] = ERL_DRV_FLOAT;
        dataset[term_count - 1] = (ErlDrvTermData) float_ptr;
        break;
      }
      case SQLITE_BLOB:
      case SQLITE_TEXT: {
        int bytes = sqlite3_column_bytes(statement, i);
        binaries_count++;
        binaries =
          realloc(binaries, sizeof(*binaries) * binaries_count);
        binaries[binaries_count - 1] = driver_alloc_binary(bytes);
        binaries[binaries_count - 1]->orig_size = bytes;
        memcpy(binaries[binaries_count - 1]->orig_bytes,
               sqlite3_column_blob(statement, i), bytes);

        term_count += 4;
        if (term_count > term_allocated) {
          term_allocated =
            (term_count >= term_allocated*2) ? term_count : term_allocated*2;
          dataset = realloc(dataset, sizeof(*dataset) * term_allocated);
        }
        dataset[term_count - 4] = ERL_DRV_BINARY;
        dataset[term_count - 3] =
          (ErlDrvTermData) binaries[binaries_count - 1];
        dataset[term_count - 2] = bytes;
        dataset[term_count - 1] = 0;
        break;
      }
      case SQLITE_NULL: {
        term_count += 2;
        if (term_count > term_allocated) {
          term_allocated =
            (term_count >= term_allocated*2) ? term_count : term_allocated*2;
          dataset = realloc(dataset, sizeof(*dataset) * term_allocated);
        }
        dataset[term_count - 2] = ERL_DRV_ATOM;
        dataset[term_count - 1] = drv->atom_null;
        break;
      }
      }
    }
    term_count += 2;
    if (term_count > term_allocated) {
      term_allocated =
        (term_count >= term_allocated*2) ? term_count : term_allocated*2;
      dataset = realloc(dataset, sizeof(*dataset) * term_allocated);
    }
    dataset[term_count - 2] = ERL_DRV_TUPLE;
    dataset[term_count - 1] = column_count;

    row_count++;
  }
  async_command->row_count = row_count;
  async_command->ptrs = ptrs;
  async_command->binaries = binaries;
  async_command->binaries_count = binaries_count;

  if (next_row == SQLITE_BUSY) {
    return_error(drv, "SQLite3 database is busy", &async_command->dataset,
        &async_command->term_count);
    return;
  }
  if (next_row != SQLITE_DONE) {
    return_error(drv, sqlite3_errmsg(drv->db), &async_command->dataset,
        &async_command->term_count);
    return;
  }

  if (column_count > 0) {
    term_count += 3+2+3;
    if (term_count > term_allocated) {
      term_allocated =
        (term_count >= term_allocated*2) ? term_count : term_allocated*2;
      dataset = realloc(dataset, sizeof(*dataset) * term_allocated);
    }
    dataset[term_count - 8] = ERL_DRV_NIL;
    dataset[term_count - 7] = ERL_DRV_LIST;
    dataset[term_count - 6] = row_count + 1;

    dataset[term_count - 5] = ERL_DRV_TUPLE;
    dataset[term_count - 4] = 2;

    dataset[term_count - 3] = ERL_DRV_NIL;
    dataset[term_count - 2] = ERL_DRV_LIST;
    dataset[term_count - 1] = 3;
  } else if (strcasestr(sqlite3_sql(statement), "INSERT")) {
    sqlite3_int64 rowid = sqlite3_last_insert_rowid(drv->db);
    term_count += 6;
    if (term_count > term_allocated) {
      term_allocated =
        (term_count >= term_allocated*2) ? term_count : term_allocated*2;
      dataset = realloc(dataset, sizeof(*dataset) * term_allocated);
    }
    dataset[term_count - 6] = ERL_DRV_ATOM;
    dataset[term_count - 5] = drv->atom_id;
    dataset[term_count - 4] = ERL_DRV_INT;
    dataset[term_count - 3] = rowid;
    dataset[term_count - 2] = ERL_DRV_TUPLE;
    dataset[term_count - 1] = 2;
  } else {
    term_count += 2;
    if (term_count > term_allocated) {
      term_allocated =
        (term_count >= term_allocated*2) ? term_count : term_allocated*2;
      dataset = realloc(dataset, sizeof(*dataset) * term_allocated);
    }
    dataset[term_count - 2] = ERL_DRV_ATOM;
    dataset[term_count - 1] = drv->atom_ok;
  }

  term_count += 2;
  if (term_count > term_allocated) {
    term_allocated =
      (term_count >= term_allocated*2) ? term_count : term_allocated*2;
    dataset = realloc(dataset, sizeof(*dataset) * term_allocated);
  }
  dataset[term_count - 2] = ERL_DRV_TUPLE;
  dataset[term_count - 1] = 2;

  async_command->dataset = dataset;
  async_command->term_count = term_count;
  // fprintf(drv->log, "Total term count: %p %d, rows count: %dx%d\n", statement, term_count, column_count, row_count);
  // fflush(drv->log);
}

static void ready_async(ErlDrvData drv_data, ErlDrvThreadData thread_data) {
  async_sqlite3_command *async_command =
    (async_sqlite3_command *) thread_data;
  sqlite3_drv_t *drv = async_command->driver_data;

  int res = driver_output_term(drv->port,
                               async_command->dataset,
                               async_command->term_count);
  // fprintf(drv->log, "Total term count: %p %d, rows count: %d (%d)\n", async_command->statement, async_command->term_count, async_command->row_count, res);
  // fflush(drv->log);
  sql_free_async(async_command);
}

// Unknown Command
static int unknown(sqlite3_drv_t *drv, char *command, int command_size) {
  // Return {Port, error, unknown_command}
  ErlDrvTermData spec[] = {
    ERL_DRV_PORT, driver_mk_port(drv->port),
    ERL_DRV_ATOM, drv->atom_error,
    ERL_DRV_ATOM, drv->atom_unknown_cmd,
    ERL_DRV_TUPLE, 3
  };
  return driver_output_term(drv->port, spec, sizeof(spec) / sizeof(spec[0]));
}

static int print_dataset(ErlDrvTermData *dataset, int term_count) {
  ErlDrvTermData lastData = *dataset;
  ErlDrvTermData newData;
  int i;
  printf("dataset (%d terms):\n", term_count);
  for (i = 1; i < term_count; i++) {
    dataset++;
    newData = *dataset;
    switch (lastData) {
    case ERL_DRV_INT:
      printf("int: %ld\n", (ErlDrvSInt) newData);
      break;
    case ERL_DRV_INT64:
      printf("int64: %p:%lld\n", (void *) newData,
             *(ErlDrvSInt64 *) newData);
      break;
    case ERL_DRV_FLOAT:
      printf("int64: %p:%f\n", (void *) newData, *(double *) newData);
      break;
    //  case ERL_DRV_TUPLE:
    //    printf("tuple of size %d\n", (int) newData);
    //    break;
    //  case ERL_DRV_LIST:
    //    printf("list of length %d\n", (int) newData);
    //    break;
    default:
      break;
    }
    lastData = newData;
  }
  return 0;
}

static inline ptr_list *add_to_ptr_list(ptr_list *list, void *value_ptr) {
  ptr_list* new_node = malloc(sizeof(ptr_list));
  new_node->head = value_ptr;
  new_node->tail = NULL;
  if (list) {
    list->tail = new_node;
    return list;
  } else {
    return new_node;
  }
}

static inline void free_ptr_list(ptr_list *list) {
  ptr_list* tail;
  while (list) {
    tail = list->tail;
    free(list->head);
    free(list);
    list = tail;
  }
}
