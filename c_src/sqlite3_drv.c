
#include <stdio.h>
#include <stdlib.h>
#include <stdio.h>
#include <ctype.h>
#include <string.h>
#include <assert.h>
#include <erl_driver.h>
#include <erl_interface.h>
#include <ei.h>
#include <sqlite3.h>

#if SQLITE_VERSION_NUMBER < 3006001
#error "SQLite3 of version 3.6.1 minumum required"
#endif

#if ERL_DRV_EXTENDED_MAJOR_VERSION < 2
typedef int ErlDrvSizeT;
typedef int ErlDrvSSizeT;
#endif

#define LOG_PATH "/tmp/sqlite3-drv.log"

#define CMD_ALTER 1
#define CMD_ANALYZE 2
#define CMD_ATTACH 3
#define CMD_CREATE 4
#define CMD_DELETE 5
#define CMD_DROP 6
#define CMD_INSERT 7
#define CMD_PRAGMA 8
#define CMD_REPLACE 9
#define CMD_SELECT 10
#define CMD_UPDATE 11
#define CMD_VACCUM 12
#define CMD_BEGIN 100
#define CMD_COMMIT 101

typedef struct _Sqlite3Drv {
    ErlDrvPort port;
	sqlite3 *db;
	FILE *log;
	int async_id;
	int async_key;
	ErlDrvTermData atom_blob;
	ErlDrvTermData atom_error;
	ErlDrvTermData atom_columns;
	ErlDrvTermData atom_rows;
	ErlDrvTermData atom_null;
	ErlDrvTermData atom_rowid;
	ErlDrvTermData atom_ok;
	ErlDrvTermData atom_done;
} Sqlite3Drv;

#ifndef max 
static inline int max(int a, int b);
#endif

static unsigned int 
hash(char *s);

static void 
dataset_realloc(ErlDrvTermData **dataset, int *count, int *allocated, int size); 

static ErlDrvSSizeT 
handle_create_cmd(Sqlite3Drv *drv, char *buf, ErlDrvSizeT len);

static Sqlite3Drv *sqlite3_drv_create(ErlDrvPort port) {
    Sqlite3Drv *d = (Sqlite3Drv*)driver_alloc(sizeof(Sqlite3Drv));
    d->port = port;
	return d;
}

static void sqlite3_drv_free(Sqlite3Drv *d) {
	//TODO: free elements
	if(d->db) {
		sqlite3_close(d->db);
	}
    driver_free((char*)d);
}

static ErlDrvData sqlite3_drv_start(ErlDrvPort port, char *buff)
{
	int errcode = 0;
	char *file = NULL;
	sqlite3 *db = NULL;
    Sqlite3Drv* drv = NULL;

	drv = sqlite3_drv_create(port); 

	//create log
	drv->log = fopen(LOG_PATH, "a+");
	if (!drv->log) {
		fprintf(stderr, "Error creating log file: %s\n", LOG_PATH);
		drv->log = stderr; // noisy
	}

	//database file
	file = strstr(buff, " ");
	if(!file) {
		sqlite3_drv_free(drv);
		fprintf(drv->log, "ERROR: NO db file!\n");
		return ERL_DRV_ERROR_BADARG;
	} 
	++file;
	
	//open database
	sqlite3_open(file, &db);
	errcode = sqlite3_errcode(db);
	if(errcode != SQLITE_OK) {
		fprintf(drv->log, "ERROR: Unable to open file: %s, because %s\n", file, sqlite3_errmsg(db));	
	} else {
		fprintf(drv->log, "Opened file %s\n", file);
	}

	//initialize driver
	drv->db = db;
	drv->async_id = 0;
	drv->async_key = hash(file);

	drv->atom_blob = driver_mk_atom("blob");
	drv->atom_error = driver_mk_atom("error");
	drv->atom_columns = driver_mk_atom("columns");
	drv->atom_rows = driver_mk_atom("rows");
	drv->atom_null = driver_mk_atom("null");
	drv->atom_rowid = driver_mk_atom("rowid");
	drv->atom_ok = driver_mk_atom("ok");
	drv->atom_done = driver_mk_atom("done");

	fflush(drv->log);
    return (ErlDrvData)drv;
}

static void sqlite3_drv_stop(ErlDrvData handle)
{
	//CLOSE DB
	sqlite3_drv_free((Sqlite3Drv *)handle);
}

static ErlDrvSSizeT 
sqlite3_drv_control(ErlDrvData drv_data, unsigned int command, char *buf, ErlDrvSizeT len, char **rbuf, ErlDrvSizeT rlen) {
	int retlen = -1;
	Sqlite3Drv *d = (Sqlite3Drv *)drv_data;
	switch (command) {
	case CMD_ALTER:
	case CMD_ANALYZE:
	case CMD_ATTACH:
	case CMD_CREATE:
		retlen = handle_create_cmd(d, buf, len);
		break;
	case CMD_DELETE:
	case CMD_DROP:
	case CMD_INSERT:
	case CMD_PRAGMA:
	case CMD_REPLACE:
	case CMD_SELECT:
	case CMD_UPDATE:
	case CMD_VACCUM:
	case CMD_BEGIN:
	case CMD_COMMIT:
	default:
		retlen = -1;
	}
	return retlen;
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

static inline int 
output_error(
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

static inline int 
output_db_error(Sqlite3Drv *drv) {
  return output_error(drv, sqlite3_errcode(drv->db), sqlite3_errmsg(drv->db));
}

static inline int 
output_ok(Sqlite3Drv *drv) {
  // Return {Port, ok}
  ErlDrvTermData spec[] = {
      ERL_DRV_PORT, driver_mk_port(drv->port),
      ERL_DRV_ATOM, drv->atom_ok,
      ERL_DRV_TUPLE, 2
  };
  return driver_output_term(drv->port, spec, sizeof(spec) / sizeof(spec[0]));
}

static ErlDrvSSizeT 
handle_create_cmd(Sqlite3Drv *drv, char *buf, ErlDrvSizeT len) {
	int result;
	const char *rest;
	sqlite3_stmt *stmt;

#ifdef DEBUG
	fprintf(drv->log, "Preexec: %.*s\n", len, buf);
	fflush(drv->log);
#endif

	result = sqlite3_prepare_v2(drv->db, buf, len, &stmt, &rest);
	if (result != SQLITE_OK) {
		return output_db_error(drv);
	} else if (stmt == NULL) {
		return output_error(drv, SQLITE_MISUSE, "empty statement");
	}
	//sql_exec_statement(drv, statement);
	return output_ok(drv); 
}

static void sqlite3_drv_ready_async(ErlDrvData drv_data, ErlDrvThreadData thread_data) {
	return;
}

ErlDrvEntry sqlite3_driver_entry = {
    NULL,			/* F_PTR init, N/A */
    sqlite3_drv_start,		/* L_PTR start, called when port is opened */
    sqlite3_drv_stop,		/* F_PTR stop, called when port is closed */
    NULL,		/* F_PTR output, called when erlang has sent */
    NULL,			/* F_PTR ready_input, called when input descriptor ready */
    NULL,			/* F_PTR ready_output, called when output descriptor ready */
    "sqlite3_drv",		/* char *driver_name, the argument to open_port */
    NULL, /* finish */
    NULL, /* handle */
    sqlite3_drv_control, /* control */
    NULL, /* timeout */
    NULL, /* outputv */
    sqlite3_drv_ready_async, /* ready_async */
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

DRIVER_INIT(sqlite3_drv) /* must match name in driver_entry */
{
    return &sqlite3_driver_entry;
}

static void dataset_realloc(ErlDrvTermData **dataset, int *count, int *allocated, int size) {
	*count += size;
    if (*count > *allocated) {
      *allocated = max(*count, *allocated *2);
      *dataset = driver_realloc(*dataset, sizeof(ErlDrvTermData) * *allocated);
    }
}

#ifndef max
static inline int max(int a, int b) {
  return a >= b ? a : b;
}
#endif

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

