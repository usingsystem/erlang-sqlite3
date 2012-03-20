%%%-------------------------------------------------------------------
%%% File    : sqlite3.erl
%%% @author Tee Teoh
%%% @copyright 21 Jun 2008 by Tee Teoh
%%% @version 1.0.0
%%% @doc Library module for sqlite3
%%%
%%% @type table_id() = atom() | binary() | string()
%%% @end
%%%-------------------------------------------------------------------
-module(sqlite3).
-include("sqlite3.hrl").
-export_types([sql_value/0, sql_type/0, table_info/0, sqlite_error/0, 
               sql_params/0, sql_non_query_result/0, sql_result/0]).

-behaviour(gen_server).

%% API
-export([open/1, open/2]).
-export([start_link/1, start_link/2]).
-export([stop/0, close/1, close/2]).
-export([sql_exec/1, sql_exec/2, sql_exec/3, sql_exec/4,
         sql_exec_script/2, sql_exec_script/3]).
-export([prepare/2, bind/3, next/2, reset/2, clear_bindings/2, finalize/2,
         columns/2, prepare/3, bind/4, next/3,
         reset/3, clear_bindings/3, finalize/3,
         columns/3]).
-export([create_table/2, create_table/3, create_table/4, create_table/5]).
-export([list_tables/0, list_tables/1, list_tables/2, 
         table_info/1, table_info/2, table_info/3]).
-export([write/2, write/3, write/4, 
		 write_many/2, write_many/3, write_many/4]).
-export([update/3, update/4, update/5]).
-export([read_all/2, read_all/3, read_all/4, 
         read/2, read/3, read/4, read/5]).
-export([delete/2, delete/3, delete/4]).
-export([drop_table/1, drop_table/2, drop_table/3]).
-export([vacuum/0, vacuum/1, vacuum/2]).

%% -export([create_function/3]).

-export([value_to_sql/1, value_to_sql_unsafe/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define('DRIVER_NAME', 'sqlite3_drv').
-record(state, {port, ops = [], refs = dict:new()}).

%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% @doc 
%%   Opens the sqlite3 database in file Db.db in the working directory 
%%   (creating this file if necessary). This is the same as open/1.
%% @end
%%--------------------------------------------------------------------
-type option() :: {file, string()} | temporary | in_memory.
-type result() :: {'ok', pid()} | 'ignore' | {'error', any()}.

-spec start_link(atom()) -> result().
start_link(Db) ->
    open(Db, []).

%%--------------------------------------------------------------------
%% @doc 
%%   Opens a sqlite3 database creating one if necessary. By default the 
%%   database will be called Db.db in the current path. This can be changed 
%%   by passing the option {file, DbFile :: String()}. DbFile must be the 
%%   full path to the sqlite3 db file. start_link/1 can be use with stop/0, 
%%   sql_exec/1, create_table/2, list_tables/0, table_info/1, write/2, 
%%   read/2, delete/2 and drop_table/1. This is the same as open/2.
%% @end
%%--------------------------------------------------------------------
-spec start_link(atom(), [option()]) -> result().
start_link(Db, Options) ->
    open(Db, Options).

%%--------------------------------------------------------------------
%% @doc
%%   Opens the sqlite3 database in file Db.db in the working directory 
%%   (creating this file if necessary). This is the same as open/1.
%% @end
%%--------------------------------------------------------------------
-spec open(atom()) -> result().
open(Db) ->
    open(Db, []).

%%--------------------------------------------------------------------
%% @spec open(Db :: atom(), Options :: [option()]) -> {ok, Pid :: pid()} | ignore | {error, Error}
%% @type option() = {file, DbFile :: string()} | in_memory | temporary
%%   
%% @doc
%%   Opens a sqlite3 database creating one if necessary. By default the database
%%   will be called Db.db in the current path. This can be changed by
%%   passing the option {file, DbFile :: string()}. DbFile must be the full
%%   path to the sqlite3 db file. Can be use to open multiple sqlite3 databases
%%   per node. Must be use in conjunction with stop/1, sql_exec/2,
%%   create_table/3, list_tables/1, table_info/2, write/3, read/3, delete/3
%%   and drop_table/2.
%% @end
%%--------------------------------------------------------------------
-spec open(atom(), [option()]) -> result().
open(Db, Options) ->
    Opts = case proplists:lookup(file, Options) of
               none ->
                   DbName = case proplists:is_defined(temporary, Options) of
                                true -> 
                                    "";
                                false ->
                                    case proplists:is_defined(in_memory, Options) of
                                        true -> 
                                            ":memory:";
                                        false ->
                                            "./" ++ atom_to_list(Db) ++ ".db"
                                    end
                            end,
                   [{file, DbName} | Options];
               {file, _} -> 
                   Options
           end,
    gen_server:start_link({local, Db}, ?MODULE, Opts, []).

%%--------------------------------------------------------------------
%% @doc
%%   Closes the Db sqlite3 database.
%% @end
%%--------------------------------------------------------------------
-spec close(atom()) -> 'ok'.
close(Db) ->
    gen_server:call(Db, close).

%%--------------------------------------------------------------------
%% @doc
%%   Closes the Db sqlite3 database.
%% @end
%%--------------------------------------------------------------------
-spec close(atom(), timeout()) -> 'ok'.
close(Db, Timeout) ->
    gen_server:call(Db, close, Timeout).

%%--------------------------------------------------------------------
%% @doc
%%   Closes the sqlite3 database.
%% @end
%%--------------------------------------------------------------------
-spec stop() -> 'ok'.
stop() ->
    close(?MODULE).

%%--------------------------------------------------------------------
%% @doc
%%   Executes the Sql statement directly.
%% @end
%%--------------------------------------------------------------------
-spec sql_exec(iodata()) -> sql_result().
sql_exec(SQL) ->
    sql_exec(?MODULE, SQL).

%%--------------------------------------------------------------------
%% @doc
%%   Executes the Sql statement directly on the Db database. Returns the
%%   result of the Sql call.
%% @end
%%--------------------------------------------------------------------
-spec sql_exec(atom(), iodata()) -> sql_result().
sql_exec(Db, SQL) ->
    gen_server:call(Db, {sql_exec, SQL}).

%%--------------------------------------------------------------------
%% @doc
%%   Executes the Sql statement with parameters Params directly on the Db 
%%   database. Returns the result of the Sql call.
%% @end
%%--------------------------------------------------------------------
-spec sql_exec(atom(), iodata(), [sql_value() | {atom() | string() | integer(), sql_value()}]) -> 
       sql_result().
sql_exec(Db, SQL, Params) when is_list(Params) ->
    gen_server:call(Db, {sql_bind_and_exec, SQL, Params});

%%--------------------------------------------------------------------
%% @doc
%%   Executes the Sql statement directly on the Db database. Returns the
%%   result of the Sql call.
%% @end
%%--------------------------------------------------------------------
sql_exec(Db, SQL, Timeout) when is_integer(Timeout) ->
    gen_server:call(Db, {sql_exec, SQL}, Timeout).

%%--------------------------------------------------------------------
%% @doc
%%   Executes the Sql statement with parameters Params directly on the Db 
%%   database. Returns the result of the Sql call.
%% @end
%%--------------------------------------------------------------------
-spec sql_exec(atom(), iodata(), [sql_value() | {atom() | string() | integer(), sql_value()}], timeout()) -> 
       sql_result().
sql_exec(Db, SQL, Params, Timeout) ->
    gen_server:call(Db, {sql_bind_and_exec, SQL, Params}, Timeout).

%%--------------------------------------------------------------------
%% @doc
%%   Executes the Sql script (consisting of semicolon-separated statements) 
%%   directly on the Db database. 
%%
%%   If an error happens while executing a statement, no further statements are executed.
%%
%%   The return value is the list of results of all executed statements.
%% @end
%%--------------------------------------------------------------------
-spec sql_exec_script(atom(), iodata()) -> [sql_result()].
sql_exec_script(Db, SQL) ->
    gen_server:call(Db, {sql_exec_script, SQL}).

%%--------------------------------------------------------------------
%% @doc
%%   Executes the Sql script (consisting of semicolon-separated statements) 
%%   directly on the Db database.
%%
%%   If an error happens while executing a statement, no further statements are executed.
%%
%%   The return value is the list of results of all executed statements.
%% @end
%%--------------------------------------------------------------------
-spec sql_exec_script(atom(), iodata(), timeout()) -> [sql_result()].
sql_exec_script(Db, SQL, Timeout) ->
    gen_server:call(Db, {sql_exec_script, SQL}, Timeout).

-spec prepare(atom(), iodata()) -> {ok, reference()} | sqlite_error().
prepare(Db, SQL) ->
    gen_server:call(Db, {prepare, SQL}).

-spec bind(atom(), reference(), sql_params()) -> sql_non_query_result().
bind(Db, Ref, Params) ->
    gen_server:call(Db, {bind, Ref, Params}).

-spec next(atom(), reference()) -> tuple() | done | sqlite_error().
next(Db, Ref) ->
    gen_server:call(Db, {next, Ref}).

-spec reset(atom(), reference()) -> sql_non_query_result().
reset(Db, Ref) ->
    gen_server:call(Db, {reset, Ref}).

-spec clear_bindings(atom(), reference()) -> sql_non_query_result().
clear_bindings(Db, Ref) ->
    gen_server:call(Db, {clear_bindings, Ref}).

-spec finalize(atom(), reference()) -> sql_non_query_result().
finalize(Db, Ref) ->
    gen_server:call(Db, {finalize, Ref}).

-spec columns(atom(), reference()) -> sql_non_query_result().
columns(Db, Ref) ->
    gen_server:call(Db, {columns, Ref}).

-spec prepare(atom(), iodata(), timeout()) -> {ok, reference()} | sqlite_error().
prepare(Db, SQL, Timeout) ->
    gen_server:call(Db, {prepare, SQL}, Timeout).

-spec bind(atom(), reference(), sql_params(), timeout()) -> sql_non_query_result().
bind(Db, Ref, Params, Timeout) ->
    gen_server:call(Db, {bind, Ref, Params}, Timeout).

-spec next(atom(), reference(), timeout()) -> tuple() | done | sqlite_error().
next(Db, Ref, Timeout) ->
    gen_server:call(Db, {next, Ref}, Timeout).

-spec reset(atom(), reference(), timeout()) -> sql_non_query_result().
reset(Db, Ref, Timeout) ->
    gen_server:call(Db, {reset, Ref}, Timeout).

-spec clear_bindings(atom(), reference(), timeout()) -> sql_non_query_result().
clear_bindings(Db, Ref, Timeout) ->
    gen_server:call(Db, {clear_bindings, Ref}, Timeout).

-spec finalize(atom(), reference(), timeout()) -> sql_non_query_result().
finalize(Db, Ref, Timeout) ->
    gen_server:call(Db, {finalize, Ref}, Timeout).

-spec columns(atom(), reference(), timeout()) -> sql_non_query_result().
columns(Db, Ref, Timeout) ->
    gen_server:call(Db, {columns, Ref}, Timeout).

%%--------------------------------------------------------------------
%% @doc
%%   Creates the Tbl table using TblInfo as the table structure. The
%%   table structure is a list of {column name, column type} pairs.
%%   e.g. [{name, text}, {age, integer}]
%%
%%   Returns the result of the create table call.
%% @end
%%--------------------------------------------------------------------
-spec create_table(table_id(), table_info()) -> sql_non_query_result().
create_table(Tbl, Columns) ->
    create_table(?MODULE, Tbl, Columns).

%%--------------------------------------------------------------------
%% @doc
%%   Creates the Tbl table in Db using Columns as the table structure. 
%%   The table structure is a list of {column name, column type} pairs. 
%%   e.g. [{name, text}, {age, integer}]
%%
%%   Returns the result of the create table call.
%% @end
%%--------------------------------------------------------------------
-spec create_table(atom(), table_id(), table_info()) -> sql_non_query_result().
create_table(Db, Tbl, Columns) ->
    gen_server:call(Db, {create_table, Tbl, Columns}).

%%--------------------------------------------------------------------
%% @doc
%%   Creates the Tbl table in Db using Columns as the table structure. 
%%   The table structure is a list of {column name, column type} pairs. 
%%   e.g. [{name, text}, {age, integer}]
%%
%%   Returns the result of the create table call.
%% @end
%%--------------------------------------------------------------------
-spec create_table(atom(), table_id(), table_info(), timeout()) -> sql_non_query_result().
create_table(Db, Tbl, Columns, Timeout) when is_integer(Timeout) ->
    gen_server:call(Db, {create_table, Tbl, Columns}, Timeout);

%%--------------------------------------------------------------------
%% @doc
%%   Creates the Tbl table in Db using Columns as the table structure and
%%   Constraints as table constraints. 
%%   The table structure is a list of {column name, column type} pairs. 
%%   e.g. [{name, text}, {age, integer}]
%%
%%   Returns the result of the create table call.
%% @end
%%--------------------------------------------------------------------
%-spec create_table(atom(), table_id(), table_info(), table_constraints()) -> 
%          sql_non_query_result().
create_table(Db, Tbl, Columns, Constraints) ->
    gen_server:call(Db, {create_table, Tbl, Columns, Constraints}).

%%--------------------------------------------------------------------
%% @doc
%%   Creates the Tbl table in Db using Columns as the table structure and
%%   Constraints as table constraints. 
%%   The table structure is a list of {column name, column type} pairs. 
%%   e.g. [{name, text}, {age, integer}]
%%
%%   Returns the result of the create table call.
%% @end
%%--------------------------------------------------------------------
-spec create_table(atom(), table_id(), table_info(), table_constraints(), timeout()) -> 
          sql_non_query_result().
create_table(Db, Tbl, Columns, Constraints, Timeout) ->
    gen_server:call(Db, {create_table, Tbl, Columns, Constraints}, Timeout).

%%--------------------------------------------------------------------
%% @doc
%%   Returns a list of tables.
%% @end
%%--------------------------------------------------------------------
-spec list_tables() -> [table_id()].
list_tables() ->
    list_tables(?MODULE).

%%--------------------------------------------------------------------
%% @doc
%%   Returns a list of tables for Db.
%% @end
%%--------------------------------------------------------------------
-spec list_tables(atom()) -> [table_id()].
list_tables(Db) ->
    gen_server:call(Db, list_tables).

%%--------------------------------------------------------------------
%% @doc
%%   Returns a list of tables for Db.
%% @end
%%--------------------------------------------------------------------
-spec list_tables(atom(), timeout()) -> [table_id()].
list_tables(Db, Timeout) ->
    gen_server:call(Db, list_tables, Timeout).

%%--------------------------------------------------------------------
%% @doc
%%    Returns table schema for Tbl.
%% @end
%%--------------------------------------------------------------------
-spec table_info(table_id()) -> table_info().
table_info(Tbl) ->
    table_info(?MODULE, Tbl).

%%--------------------------------------------------------------------
%% @doc
%%   Returns table schema for Tbl in Db.
%% @end
%%--------------------------------------------------------------------
-spec table_info(atom(), table_id()) -> table_info().
table_info(Db, Tbl) ->
    gen_server:call(Db, {table_info, Tbl}).

%%--------------------------------------------------------------------
%% @doc
%%   Returns table schema for Tbl in Db.
%% @end
%%--------------------------------------------------------------------
-spec table_info(atom(), table_id(), timeout()) -> table_info().
table_info(Db, Tbl, Timeout) ->
    gen_server:call(Db, {table_info, Tbl}, Timeout).

%%--------------------------------------------------------------------
%% @doc
%%   Write Data into Tbl table. Value must be of the same type as 
%%   determined from table_info/2.
%% @end
%%--------------------------------------------------------------------
-spec write(table_id(), [{column_id(), sql_value()}]) -> sql_non_query_result().
write(Tbl, Data) ->
    write(?MODULE, Tbl, Data).

%%--------------------------------------------------------------------
%% @doc
%%   Write Data into Tbl table in Db database. Value must be of the 
%%   same type as determined from table_info/3.
%% @end
%%--------------------------------------------------------------------
-spec write(atom(), table_id(), [{column_id(), sql_value()}]) -> sql_non_query_result().
write(Db, Tbl, Data) ->
    gen_server:call(Db, {write, Tbl, Data}).

%%--------------------------------------------------------------------
%% @doc
%%   Write Data into Tbl table in Db database. Value must be of the 
%%   same type as determined from table_info/3.
%% @end
%%--------------------------------------------------------------------
-spec write(atom(), table_id(), [{column_id(), sql_value()}], timeout()) -> 
		  sql_non_query_result().
write(Db, Tbl, Data, Timeout) ->
    gen_server:call(Db, {write, Tbl, Data}, Timeout).

%%--------------------------------------------------------------------
%% @doc
%%   Write all records in Data into table Tbl. Value must be of the 
%%   same type as determined from table_info/2.
%% @end
%%--------------------------------------------------------------------
-spec write_many(table_id(), [[{column_id(), sql_value()}]]) -> [sql_result()].
write_many(Tbl, Data) ->
    write_many(?MODULE, Tbl, Data).

%%--------------------------------------------------------------------
%% @doc
%%   Write all records in Data into table Tbl in database Db. Value 
%%   must be of the same type as determined from table_info/3.
%% @end
%%--------------------------------------------------------------------
-spec write_many(atom(), table_id(), [[{column_id(), sql_value()}]]) -> [sql_result()].
write_many(Db, Tbl, Data) ->
    gen_server:call(Db, {write_many, Tbl, Data}).

%%--------------------------------------------------------------------
%% @doc
%%   Write all records in Data into table Tbl in database Db. Value 
%%   must be of the same type as determined from table_info/3.
%% @end
%%--------------------------------------------------------------------
-spec write_many(atom(), table_id(), [[{column_id(), sql_value()}]], timeout()) -> 
		  [sql_result()].
write_many(Db, Tbl, Data, Timeout) ->
    gen_server:call(Db, {write_many, Tbl, Data}, Timeout).

%%--------------------------------------------------------------------
%% @doc
%%    Updates rows into Tbl table such that the Value matches the
%%    value in Key with Data.
%% @end
%%--------------------------------------------------------------------
-spec update(table_id(), {column_id(), sql_value()}, [{column_id(), sql_value()}]) -> 
		  sql_non_query_result().
update(Tbl, {Key, Value}, Data) ->
    update(?MODULE, Tbl, {Key, Value}, Data).

%%--------------------------------------------------------------------
%% @doc
%%    Updates rows into Tbl table in Db database such that the Value
%%    matches the value in Key with Data.
%% @end
%%--------------------------------------------------------------------
-spec update(atom(), table_id(), {column_id(), sql_value()}, [{column_id(), sql_value()}]) -> 
          sql_non_query_result().
update(Db, Tbl, {Key, Value}, Data) ->
  gen_server:call(Db, {update, Tbl, Key, Value, Data}).

%%--------------------------------------------------------------------
%% @doc
%%    Updates rows into Tbl table in Db database such that the Value
%%    matches the value in Key with Data.
%% @end
%%--------------------------------------------------------------------
-spec update(atom(), table_id(), {column_id(), sql_value()}, [{column_id(), sql_value()}], timeout()) -> 
          sql_non_query_result().
update(Db, Tbl, {Key, Value}, Data, Timeout) ->
  gen_server:call(Db, {update, Tbl, Key, Value, Data}, Timeout).

%%--------------------------------------------------------------------
%% @doc
%%   Reads all rows from Table in Db.
%% @end
%%--------------------------------------------------------------------
-spec read_all(atom(), table_id()) -> sql_result().
read_all(Db, Tbl) ->
    gen_server:call(Db, {read, Tbl}).

%%--------------------------------------------------------------------
%% @doc
%%   Reads all rows from Table in Db.
%% @end
%%--------------------------------------------------------------------
-spec read_all(atom(), table_id(), timeout()) -> sql_result().
read_all(Db, Tbl, Timeout) when is_integer(Timeout) ->
    gen_server:call(Db, {read, Tbl}, Timeout);

%%--------------------------------------------------------------------
%% @doc
%%   Reads Columns in all rows from Table in Db.
%% @end
%%--------------------------------------------------------------------
%-spec read_all(atom(), table_id(), [column_id()]) -> sql_result().
read_all(Db, Tbl, Columns) when is_list(Columns) ->
    gen_server:call(Db, {read, Tbl, Columns}).

%%--------------------------------------------------------------------
%% @doc
%%   Reads Columns in all rows from Table in Db.
%% @end
%%--------------------------------------------------------------------
-spec read_all(atom(), table_id(), [column_id()], timeout()) -> sql_result().
read_all(Db, Tbl, Columns, Timeout) ->
    gen_server:call(Db, {read, Tbl, Columns}, Timeout).

%%--------------------------------------------------------------------
%% @doc
%%   Reads a row from Tbl table such that the Value matches the 
%%   value in Column. Value must have the same type as determined 
%%   from table_info/2.
%% @end
%%--------------------------------------------------------------------
-spec read(table_id(), {column_id(), sql_value()}) -> sql_result().
read(Tbl, Key) ->
    read(?MODULE, Tbl, Key).

%%--------------------------------------------------------------------
%% @doc
%%   Reads a row from Tbl table in Db database such that the Value 
%%   matches the value in Column. ColValue must have the same type 
%%   as determined from table_info/3.
%% @end
%%--------------------------------------------------------------------
-spec read(atom(), table_id(), {column_id(), sql_value()}) -> sql_result().
read(Db, Tbl, {Column, Value}) ->
    gen_server:call(Db, {read, Tbl, Column, Value}).

%%--------------------------------------------------------------------
%% @doc
%%    Reads a row from Tbl table in Db database such that the Value
%%    matches the value in Column. Value must have the same type as 
%%    determined from table_info/3.
%% @end
%%--------------------------------------------------------------------
-spec read(atom(), table_id(), {column_id(), sql_value()}, [column_id()]) -> sql_result().
read(Db, Tbl, {Key, Value}, Columns) when is_list(Columns) ->
    gen_server:call(Db, {read, Tbl, Key, Value, Columns});

%%--------------------------------------------------------------------
%% @doc
%%   Reads a row from Tbl table in Db database such that the Value 
%%   matches the value in Column. ColValue must have the same type 
%%   as determined from table_info/3.
%% @end
%%--------------------------------------------------------------------
%-spec read(atom(), table_id(), {column_id(), sql_value()}, timeout()) -> sql_result().
read(Db, Tbl, {Column, Value}, Timeout) when is_integer(Timeout) ->
    gen_server:call(Db, {read, Tbl, Column, Value}, Timeout).

%%--------------------------------------------------------------------
%% @doc
%%    Reads a row from Tbl table in Db database such that the Value
%%    matches the value in Column. Value must have the same type as 
%%    determined from table_info/3.
%% @end
%%--------------------------------------------------------------------
-spec read(atom(), table_id(), {column_id(), sql_value()}, [column_id()], timeout()) -> sql_result().
read(Db, Tbl, {Key, Value}, Columns, Timeout) ->
    gen_server:call(Db, {read, Tbl, Key, Value, Columns}, Timeout).

%%--------------------------------------------------------------------
%% @doc
%%   Delete a row from Tbl table in Db database such that the Value 
%%   matches the value in Column. 
%%   Value must have the same type as determined from table_info/3.
%% @end
%%--------------------------------------------------------------------
-spec delete(table_id(), {column_id(), sql_value()}) -> sql_non_query_result().
delete(Tbl, Key) ->
    delete(?MODULE, Tbl, Key).

%%--------------------------------------------------------------------
%% @doc
%%   Delete a row from Tbl table in Db database such that the Value 
%%   matches the value in Column. 
%%   Value must have the same type as determined from table_info/3.
%% @end
%%--------------------------------------------------------------------
-spec delete(atom(), table_id(), {column_id(), sql_value()}, timeout()) -> sql_non_query_result().
delete(Db, Tbl, Key, Timeout) ->
    gen_server:call(Db, {delete, Tbl, Key}, Timeout).

%%--------------------------------------------------------------------
%% @doc
%%   Delete a row from Tbl table in Db database such that the Value 
%%   matches the value in Column. 
%%   Value must have the same type as determined from table_info/3.
%% @end
%%--------------------------------------------------------------------
-spec delete(atom(), table_id(), {column_id(), sql_value()}) -> sql_non_query_result().
delete(Db, Tbl, Key) ->
    gen_server:call(Db, {delete, Tbl, Key}).

%%--------------------------------------------------------------------
%% @doc
%%   Drop the table Tbl.
%% @end
%%--------------------------------------------------------------------
-spec drop_table(table_id()) -> sql_non_query_result().
drop_table(Tbl) ->
    drop_table(?MODULE, Tbl).

%%--------------------------------------------------------------------
%% @doc
%%   Drop the table Tbl from Db database.
%% @end
%%--------------------------------------------------------------------
-spec drop_table(atom(), table_id()) -> sql_non_query_result().
drop_table(Db, Tbl) ->
    gen_server:call(Db, {drop_table, Tbl}).

%%--------------------------------------------------------------------
%% @doc
%%   Drop the table Tbl from Db database.
%% @end
%%--------------------------------------------------------------------
-spec drop_table(atom(), table_id(), timeout()) -> sql_non_query_result().
drop_table(Db, Tbl, Timeout) ->
    gen_server:call(Db, {drop_table, Tbl}, Timeout).

%%--------------------------------------------------------------------
%% @doc
%%   Vacuum the default database.
%% @end
%%--------------------------------------------------------------------
-spec vacuum() -> sql_non_query_result().
vacuum() ->
    gen_server:call(?MODULE, vacuum).

%%--------------------------------------------------------------------
%% @doc
%%   Vacuum the Db database.
%% @end
%%--------------------------------------------------------------------
-spec vacuum(atom()) -> sql_non_query_result().
vacuum(Db) ->
    gen_server:call(Db, vacuum).

%%--------------------------------------------------------------------
%% @doc
%%   Vacuum the Db database.
%% @end
%%--------------------------------------------------------------------
-spec vacuum(atom(), timeout()) -> sql_non_query_result().
vacuum(Db, Timeout) ->
    gen_server:call(Db, vacuum, Timeout).

%% %%--------------------------------------------------------------------
%% %% @doc
%% %%   Creates function under name FunctionName.
%% %%
%% %% @end
%% %%--------------------------------------------------------------------
%% -spec create_function(atom(), atom(), function()) -> any().
%% create_function(Db, FunctionName, Function) ->
%%     gen_server:call(Db, {create_function, FunctionName, Function}).

%%--------------------------------------------------------------------
%% @doc
%%    Converts an Erlang term to an SQL string.
%%    Currently supports integers, floats, 'null' atom, and iodata
%%    (binaries and iolists) which are treated as SQL strings.
%%
%%    Note that it opens opportunity for injection if an iolist includes
%%    single quotes! Replace all single quotes (') with '' manually, or
%%    use value_to_sql/1 if you are not sure if your strings contain
%%    single quotes (e.g. can be entered by users).
%%
%%    Reexported from sqlite3_lib:value_to_sql/1 for user convenience.
%% @end
%%--------------------------------------------------------------------
-spec value_to_sql_unsafe(sql_value()) -> iolist().
value_to_sql_unsafe(X) -> sqlite3_lib:value_to_sql_unsafe(X).

%%--------------------------------------------------------------------
%% @doc
%%    Converts an Erlang term to an SQL string.
%%    Currently supports integers, floats, 'null' atom, and iodata
%%    (binaries and iolists) which are treated as SQL strings.
%%
%%    All single quotes (') will be replaced with ''.
%%
%%    Reexported from sqlite3_lib:value_to_sql/1 for user convenience.
%% @end
%%--------------------------------------------------------------------
-spec value_to_sql(sql_value()) -> iolist().
value_to_sql(X) -> sqlite3_lib:value_to_sql(X).

%%====================================================================
%% gen_server callbacks
%%====================================================================

%%--------------------------------------------------------------------
%% @doc Initiates the server
%% @end
%% @hidden
%%--------------------------------------------------------------------

% -type init_return() :: {'ok', tuple()} | {'ok', tuple(), integer()} | 'ignore' | {'stop', any()}.

-spec init([any()]) -> {'ok', #state{}} | {'stop', string()}.
init(Options) ->
    DbFile = proplists:get_value(file, Options),
    PrivDir = get_priv_dir(),
    case erl_ddll:load(PrivDir, atom_to_list(?DRIVER_NAME)) of
        ok ->
            Port = open_port({spawn, create_port_cmd(DbFile)}, [binary]),
            {ok, #state{port = Port, ops = Options}};
        {error, permanent} -> %% already loaded!
            Port = open_port({spawn, create_port_cmd(DbFile)}, [binary]),
            {ok, #state{port = Port, ops = Options}};            
        {error, Error} ->
            Msg = io_lib:format("Error loading ~p: ~s", 
                                [?DRIVER_NAME, erl_ddll:format_error(Error)]),
            {stop, lists:flatten(Msg)}
    end.

%%--------------------------------------------------------------------
%% @doc Handling call messages
%% @end
%% @hidden
%%--------------------------------------------------------------------

%% -type handle_call_return() :: {reply, any(), tuple()} | {reply, any(), tuple(), integer()} |
%%       {noreply, tuple()} | {noreply, tuple(), integer()} |
%%       {stop, any(), any(), tuple()} | {stop, any(), tuple()}.

-spec handle_call(any(), pid(), #state{}) -> {'reply', any(), #state{}} | {'stop', 'normal', 'ok', #state{}}.
handle_call(close, _From, State) ->
    Reply = ok,
    {stop, normal, Reply, State};
handle_call(list_tables, _From, State) ->
    SQL = "select name, sql from sqlite_master where type='table';",
    Data = do_sql_exec(SQL, State),
    TableList = proplists:get_value(rows, Data),
    TableNames = [cast_table_name(Name, SQLx) || {Name,SQLx} <- TableList],
    {reply, TableNames, State};
handle_call({table_info, Tbl}, _From, State) when is_atom(Tbl) ->
    % make sure we only get table info.
    SQL = io_lib:format("select sql from sqlite_master where tbl_name = '~p' and type='table';", [Tbl]),
    Data = do_sql_exec(SQL, State),
    TableSql = proplists:get_value(rows, Data),
    case TableSql of
        [{Info}] ->
            ColumnList = parse_table_info(binary_to_list(Info)),
            {reply, ColumnList, State};
        [] ->
            {reply, table_does_not_exist, State}
    end;
handle_call({table_info, _NotAnAtom}, _From, State) ->
    {reply, {error, badarg}, State};
handle_call({create_function, FunctionName, Function}, _From, #state{port = Port} = State) ->
    Reply = exec(Port, {create_function, FunctionName, Function}),
    {reply, Reply, State};
handle_call({sql_exec, SQL}, _From, State) ->
    do_handle_call_sql_exec(SQL, State);
handle_call({sql_bind_and_exec, SQL, Params}, _From, State) ->
    Reply = do_sql_bind_and_exec(SQL, Params, State),
    {reply, Reply, State};
handle_call({sql_exec_script, SQL}, _From, State) ->
    Reply = do_sql_exec_script(SQL, State),
    {reply, Reply, State};
handle_call({create_table, Tbl, Columns}, _From, State) ->
    try sqlite3_lib:create_table_sql(Tbl, Columns) of
        SQL -> do_handle_call_sql_exec(SQL, State)
    catch
        _:Exception ->
            {reply, {error, Exception}, State}
    end;
handle_call({create_table, Tbl, Columns, Constraints}, _From, State) ->
    try sqlite3_lib:create_table_sql(Tbl, Columns, Constraints) of
        SQL -> do_handle_call_sql_exec(SQL, State)
    catch
        _:Exception ->
            {reply, {error, Exception}, State}
    end;
handle_call({update, Tbl, Key, Value, Data}, _From, State) ->
    try sqlite3_lib:update_sql(Tbl, Key, Value, Data) of
        SQL -> do_handle_call_sql_exec(SQL, State)
    catch
        _:Exception ->
            {reply, {error, Exception}, State}
    end;
handle_call({write, Tbl, Data}, _From, State) ->
    % insert into t1 (data,num) values ('This is sample data',3);
    try sqlite3_lib:write_sql(Tbl, Data) of
        SQL -> do_handle_call_sql_exec(SQL, State)
    catch
        _:Exception ->
            {reply, {error, Exception}, State}
    end;
handle_call({write_many, Tbl, DataList}, _From, State) ->
    SQLScript = ["BEGIN;", 
                 [sqlite3_lib:write_sql(Tbl, Data) || Data <- DataList], 
                 "COMMIT;"],
    Reply = do_sql_exec_script(SQLScript, State),
    {reply, Reply, State};
handle_call({read, Tbl}, _From, State) ->
    % select * from  Tbl where Key = Value;
    try sqlite3_lib:read_sql(Tbl) of
        SQL -> do_handle_call_sql_exec(SQL, State)
    catch
        _:Exception ->
            {reply, {error, Exception}, State}
    end;
handle_call({read, Tbl, Columns}, _From, State) ->
    try sqlite3_lib:read_sql(Tbl, Columns) of
        SQL -> do_handle_call_sql_exec(SQL, State)
    catch
        _:Exception ->
            {reply, {error, Exception}, State}
    end;
handle_call({read, Tbl, Key, Value}, _From, State) ->
    % select * from  Tbl where Key = Value;
    try sqlite3_lib:read_sql(Tbl, Key, Value) of
        SQL -> do_handle_call_sql_exec(SQL, State)
    catch
        _:Exception ->
            {reply, {error, Exception}, State}
    end;
handle_call({read, Tbl, Key, Value, Columns}, _From, State) ->
    try sqlite3_lib:read_sql(Tbl, Key, Value, Columns) of
        SQL -> do_handle_call_sql_exec(SQL, State)
    catch
        _:Exception ->
            {reply, {error, Exception}, State}
    end;
handle_call({delete, Tbl, {Key, Value}}, _From, State) ->
    % delete from Tbl where Key = Value;
    try sqlite3_lib:delete_sql(Tbl, Key, Value) of
        SQL -> do_handle_call_sql_exec(SQL, State)
    catch
        _:Exception ->
            {reply, {error, Exception}, State}
    end;
handle_call({drop_table, Tbl}, _From, State) ->
    try sqlite3_lib:drop_table_sql(Tbl) of
        SQL -> do_handle_call_sql_exec(SQL, State)
    catch
        _:Exception ->
            {reply, {error, Exception}, State}
    end;
handle_call({prepare, SQL}, _From, State = #state{port = Port, refs = Refs}) ->
    case exec(Port, {prepare, SQL}) of
        Index when is_integer(Index) ->
            Ref = erlang:make_ref(),
            Reply = {ok, Ref},
            NewState = State#state{refs = dict:store(Ref, Index, Refs)};
        Error ->
            Reply = Error,
            NewState = State
    end,
    {reply, Reply, NewState};
handle_call({bind, Ref, Params}, _From, State = #state{port = Port, refs = Refs}) ->
    Reply = case dict:find(Ref, Refs) of
                {ok, Index} ->
                    exec(Port, {bind, Index, Params});
                error ->
                    {error, badarg}
            end,
    {reply, Reply, State};
handle_call({finalize, Ref}, _From, State = #state{port = Port, refs = Refs}) ->
    case dict:find(Ref, Refs) of
        {ok, Index} ->
            case exec(Port, {finalize, Index}) of
                ok ->
                    Reply = ok,
                    NewState = State#state{refs = dict:erase(Ref, Refs)};
                Error ->
                    Reply = Error,
                    NewState = State
            end;
        error ->
            Reply = {error, badarg},
            NewState = State
    end,
    {reply, Reply, NewState};
handle_call({Cmd, Ref}, _From, State = #state{port = Port, refs = Refs}) ->
    Reply = case dict:find(Ref, Refs) of
                {ok, Index} ->
                    exec(Port, {Cmd, Index});
                error ->
                    {error, badarg}
            end,
    {reply, Reply, State};
handle_call(vacuum, _From, State) ->
    SQL = "VACUUM;",
    do_handle_call_sql_exec(SQL, State);
handle_call(_Request, _From, State) ->
    Reply = unknown_request,
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% @doc Handling cast messages
%% @end
%% @hidden
%%--------------------------------------------------------------------

%% -type handle_cast_return() :: {noreply, tuple()} | {noreply, tuple(), integer()} |
%%       {stop, any(), tuple()}.

-spec handle_cast(any(), #state{}) -> {'noreply', #state{}}.
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @doc Handling all non call/cast messages
%% @end
%% @hidden
%%--------------------------------------------------------------------
-spec handle_info(any(), #state{}) -> {'noreply', #state{}}.
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @doc This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any necessary
%% cleaning up. When it returns, the gen_server terminates with Reason.
%% The return value is ignored.
%% @end
%% @hidden
%%--------------------------------------------------------------------
-spec terminate(atom(), tuple()) -> term().
terminate(_Reason, #state{port = Port}) ->
    case Port of
        undefined ->
            pass;
        _ ->
            port_command(Port, term_to_binary({close, nop})),
            port_close(Port)
    end,
    case erl_ddll:unload(?DRIVER_NAME) of
        ok -> 
            ok;
        {error, permanent} ->
            ok; %% FIXME is this the correct behavior?
        {error, ErrorDesc} ->
            error_logger:error_msg("Error unloading sqlite3 driver: ~s~n", 
                                   [erl_ddll:format_error(ErrorDesc)])
    end,
    ok.

%%--------------------------------------------------------------------
%% @doc Convert process state when code is changed
%% @end
%% @hidden
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------

get_priv_dir() ->
    case code:priv_dir(sqlite3) of
        {error, bad_name} ->
            %% application isn't in path, fall back
            {?MODULE, _, FileName} = code:get_object_code(?MODULE),
            filename:join(filename:dirname(FileName), "../priv");
        Dir ->
            Dir
    end.

-define(SQL_EXEC_COMMAND, 2).
-define(SQL_CREATE_FUNCTION, 3).
-define(SQL_BIND_AND_EXEC_COMMAND, 4).
-define(PREPARE, 5).
-define(PREPARED_BIND, 6).
-define(PREPARED_STEP, 7).
-define(PREPARED_RESET, 8).
-define(PREPARED_CLEAR_BINDINGS, 9).
-define(PREPARED_FINALIZE, 10).
-define(PREPARED_COLUMNS, 11).
-define(SQL_EXEC_SCRIPT, 12).

create_port_cmd(DbFile) ->
    atom_to_list(?DRIVER_NAME) ++ " " ++ DbFile.

do_handle_call_sql_exec(SQL, State) ->
    Reply = do_sql_exec(SQL, State),
    {reply, Reply, State}.

do_sql_exec(SQL, #state{port = Port}) ->
    ?dbgF("SQL: ~s~n", [SQL]),
    exec(Port, {sql_exec, SQL}).

do_sql_bind_and_exec(SQL, Params, #state{port = Port}) ->
    ?dbgF("SQL: ~s; Parameters: ~p~n", [SQL, Params]),
    exec(Port, {sql_bind_and_exec, SQL, Params}).

do_sql_exec_script(SQL, #state{port = Port}) ->
    ?dbgF("SQL: ~s~n", [SQL]),
    Results = exec(Port, {sql_exec_script, SQL}),
    %% last element of Results may be an error
    case Results of
        [_|_] ->
            case lists:last(Results) of
                {error, _Code, Reason} ->
                    error_logger:error_msg("sqlite3 driver error: ~s~n", 
                                           [Reason]);
                _ -> ok
            end;
        _ ->
            ok
    end,
    Results.

exec(_Port, {create_function, _FunctionName, _Function}) ->
    error_logger:error_report([{application, sqlite3}, "NOT IMPL YET"]);
%port_control(Port, ?SQL_CREATE_FUNCTION, list_to_binary(Cmd)),
%wait_result(Port);
exec(Port, {sql_exec, SQL}) ->
    port_control(Port, ?SQL_EXEC_COMMAND, SQL),
    wait_result(Port);
exec(Port, {sql_bind_and_exec, SQL, Params}) ->
    Bin = term_to_binary({iolist_to_binary(SQL), Params}),
    port_control(Port, ?SQL_BIND_AND_EXEC_COMMAND, Bin),
    wait_result(Port);
exec(Port, {sql_exec_script, SQL}) ->
    port_control(Port, ?SQL_EXEC_SCRIPT, SQL),
    wait_result(Port);
exec(Port, {prepare, SQL}) ->
    port_control(Port, ?PREPARE, SQL),
    wait_result(Port);
exec(Port, {bind, Index, Params}) ->
    Bin = term_to_binary({Index, Params}),
    port_control(Port, ?PREPARED_BIND, Bin),
    wait_result(Port);
exec(Port, {Cmd, Index}) when is_integer(Index) ->
    CmdCode = case Cmd of
                  next -> ?PREPARED_STEP;
                  reset -> ?PREPARED_RESET;
                  clear_bindings -> ?PREPARED_CLEAR_BINDINGS;
                  finalize -> ?PREPARED_FINALIZE;
                  columns -> ?PREPARED_COLUMNS
              end,
    Bin = term_to_binary(Index),
    port_control(Port, CmdCode, Bin),
    wait_result(Port).

wait_result(Port) ->
    receive
        {Port, Reply} ->
            case Reply of
                {error, Code, Reason} ->
                    error_logger:error_msg("sqlite3 driver error: ~s~n", 
                                           [Reason]),
                    % ?dbg("Error: ~p~n", [Reason]),
                    {error, Code, Reason};
                _ ->
                    % ?dbg("Reply: ~p~n", [Reply]),
                    Reply
            end;
        {'EXIT', Port, Reason} ->
            error_logger:error_msg("sqlite3 driver port closed with reason ~p~n", 
                                   [Reason]),
            % ?dbg("Error: ~p~n", [Reason]),
            {error, Reason};
        Other when is_tuple(Other), element(1, Other) =/= '$gen_call', element(1, Other) =/= '$gen_cast' ->
            error_logger:error_msg("sqlite3 unexpected reply ~p~n", 
                                   [Other]),
            Other
    end.

parse_table_info(Info) ->
	Info1 = re:replace(Info, <<"CHECK \\('(bin|lst|am)'='(bin|lst|am)'\\)\\)">>, "", [{return, list}]),
    {_, [$(|Rest]} = lists:splitwith(fun(C) -> C =/= $( end, Info1),
	%% remove ) at the end
	Rest1 = list_init(Rest),
    Cols = string:tokens(Rest1, ","),
    build_table_info(lists:map(fun(X) ->
                         string:tokens(X, " ") 
                     end, Cols), []).
   
build_table_info([], Acc) -> 
    lists:reverse(Acc);
build_table_info([[ColName, ColType] | Tl], Acc) -> 
    build_table_info(Tl, [{list_to_atom(ColName), sqlite3_lib:col_type_to_atom(ColType)}| Acc]); 
build_table_info([[ColName, ColType | Constraints] | Tl], Acc) ->
    build_table_info(Tl, [{list_to_atom(ColName), sqlite3_lib:col_type_to_atom(ColType), build_constraints(Constraints)} | Acc]).

%% TODO conflict-clause parsing
build_constraints([]) -> [];
build_constraints(["PRIMARY", "KEY" | Tail]) -> 
    {Constraint, Rest} = build_primary_key_constraint(Tail),
    [Constraint | build_constraints(Rest)];
build_constraints(["UNIQUE" | Tail]) -> 
	[unique | build_constraints(Tail)];
build_constraints(["NOT", "NULL" | Tail]) -> 
	[not_null | build_constraints(Tail)];
build_constraints(["DEFAULT", DefaultValue | Tail]) -> 
	[{default, sqlite3_lib:sql_to_value(DefaultValue)} | build_constraints(Tail)];
build_constraints(["CHECK", _ | Tail]) -> 
	%% currently ignored
	build_constraints(Tail).
% build_constraints(["REFERENCES", Check | Tail]) -> ...

build_primary_key_constraint(Tokens) -> build_primary_key_constraint(Tokens, []).

build_primary_key_constraint(["ASC" | Rest], Acc) ->
    build_primary_key_constraint(Rest, [asc | Acc]);
build_primary_key_constraint(["DESC" | Rest], Acc) ->
    build_primary_key_constraint(Rest, [desc | Acc]);
build_primary_key_constraint(["AUTOINCREMENT" | Rest], Acc) ->
    build_primary_key_constraint(Rest, [autoincrement | Acc]);
build_primary_key_constraint(Tail, []) ->
    {primary_key, Tail};
build_primary_key_constraint(Tail, Acc) ->
    {{primary_key, lists:reverse(Acc)}, Tail}.

cast_table_name(Bin, SQL) ->
    case re:run(SQL,<<"CHECK \\('(bin|lst|am)'='(bin|lst|am)'\\)\\)">>,[{capture,all_but_first,binary}]) of
	{match, [<<"bin">>, <<"bin">>]} ->
	    Bin;
	{match, [<<"lst">>, <<"lst">>]} ->
	    unicode:characters_to_list(Bin, latin1);
	{match, [<<"am">>, <<"am">>]} ->
	    binary_to_atom(Bin, latin1);
	_ ->
	    %% backwards compatible
	    binary_to_atom(Bin, latin1)
    end.

list_init([_]) -> [];
list_init([H|T]) -> [H|list_init(T)].

%% conflict_clause(["ON", "CONFLICT", ResolutionString | Tail]) ->
%%     Resolution = case ResolutionString of
%%                      "ROLLBACK" -> rollback;
%%                      "ABORT" -> abort;
%%                      "FAIL" -> fail;
%%                      "IGNORE" -> ignore;
%%                      "REPLACE" -> replace
%%                  end,
%%     {{on_conflict, Resolution}, Tail};
%% conflict_clause(NoOnConflictClause) ->
%%     {no_on_conflict, NoOnConflictClause}.

%%--------------------------------------------------------------------
%% @type sql_value() = null | number() | iodata() | {blob, binary()}.
%% 
%% Values accepted in SQL statements are atom 'null', numbers, 
%% strings (represented as iodata()) and blobs.
%% @end
%% @type sql_type() = integer | text | double | blob | atom() | string().
%% 
%% Types of SQLite columns are represented by atoms 'integer', 'text', 'double',
%% 'blob'. Other atoms and strings may also be used (e.g. "VARCHAR(20)", 'smallint', etc.)
%% See [http://www.sqlite.org/datatype3.html].
%% @end
%% @type pk_constraint() = autoincrement | desc | asc.
%% See {@link pk_constraints()}.
%% @type pk_constraints() = pk_constraint() | [pk_constraint()].
%% See {@link column_constraint()}.
%% @type column_constraint() = non_null | primary_key | {primary_key, pk_constraints()}
%%                             | unique | {default, sql_value()}.
%% See {@link column_constraints()}.
%% @type column_constraints() = column_constraint() | [column_constraint()].
%% See {@link table_info()}.
%% @type table_info() = [{atom(), sql_type()} | {atom(), sql_type(), column_constraints()}].
%% 
%% Describes the columns of an SQLite table: each tuple contains name, type and constraints (if any)
%% of one column.
%% @end
%% @type table_constraint() = {primary_key, [atom()]} | {unique, [atom()]}.
%% @type table_constraints() = table_constraint() | [table_constraint()].
%% 
%% Currently supported constraints for {@link table_info()} and {@link sqlite3:create_table/4}.
%% @end
%% @type sqlite_error() = {'error', integer(), string()} | {'error', any()}.
%% 
%% Errors occuring on the C side are represented by 3-element tuples containing 
%% atom 'error', SQLite result code ([http://www.sqlite.org/c3ref/c_abort.html], 
%% [http://www.sqlite.org/c3ref/c_busy_recovery.html]) and an English-language error
%% message.
%%
%% Errors occuring on the Erlang side are represented by 2-element tuples with
%% first element 'error'.
%% @end
%% @type sql_non_query_result() = ok | sqlite_error() | {rowid, integer()}.
%% The result returned by functions which call the database but don't return
%% any records.
%% @end
%% @type sql_result() = sql_non_query_result() | [{columns, [string()]} | {rows, [tuple()]}].
%% The result returned by functions which query the database.
%% @end
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% Tests
%%--------------------------------------------------------------------
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.
