%%%-------------------------------------------------------------------
%%% File    : sqlite3.erl
%%% @author Tee Teoh
%%% @copyright 21 Jun 2008 by Tee Teoh
%%% @version 1.0.0
%%% @doc Library module for sqlite3
%%% @end
%%%-------------------------------------------------------------------
-module(sqlite3).
-include("sqlite3.hrl").

-behaviour(gen_server).

%% API
-export([open/1, open/2]).
-export([start_link/1, start_link/2]).
-export([stop/0, close/1]).
-export([sql_exec/1, sql_exec/2]).

-export([create_table/2, create_table/3, create_table/4]).
-export([list_tables/0, list_tables/1, table_info/1, table_info/2]).
-export([write/2, write/3, write_many/2, write_many/3]).
-export([update/4, update/5]).
-export([read_all/2, read_all/3, read/2, read/3, read/4]).
-export([delete/2, delete/3]).
-export([drop_table/1, drop_table/2]).
-export([begin_transaction/1, commit_transaction/1, rollback_transaction/1]).

-export([create_function/3]).

-export([value_to_sql/1, value_to_sql_unsafe/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define('DRIVER_NAME', 'sqlite3_drv').
-record(state, {port, ops = []}).

%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% @spec start_link(Db :: atom()) -> {ok, Pid :: pid()} | ignore | {error, Error}
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
%% @spec start_link(Db :: atom(), Options) -> {ok, Pid :: pid()} | ignore | {error, Error}
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
%% @spec open(Db :: atom()) -> {ok, Pid :: pid()} | ignore | {error, Error}
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
%% @spec close(Db :: atom()) -> ok
%% @doc
%%   Closes the Db sqlite3 database.
%% @end
%%--------------------------------------------------------------------
-spec close(atom()) -> 'ok'.
close(Db) ->
    gen_server:call(Db, close).

%%--------------------------------------------------------------------
%% @spec stop() -> ok
%% @doc
%%   Closes the sqlite3 database.
%% @end
%%--------------------------------------------------------------------
-spec stop() -> 'ok'.
stop() ->
    close(?MODULE).

%%--------------------------------------------------------------------
%% @spec sql_exec(Sql :: iodata()) -> term()
%% @doc
%%   Executes the Sql statement directly.
%% @end
%%--------------------------------------------------------------------
-spec sql_exec(iodata()) -> any().
sql_exec(SQL) ->
    sql_exec(?MODULE, SQL).

%%--------------------------------------------------------------------
%% @spec sql_exec(Db :: atom(), Sql :: iodata()) -> any()
%% @doc
%%   Executes the Sql statement directly on the Db database. Returns the
%%   result of the Sql call.
%% @end
%%--------------------------------------------------------------------
-spec sql_exec(atom(), iodata()) -> any().
sql_exec(Db, SQL) ->
    gen_server:call(Db, {sql_exec, SQL}).

%%--------------------------------------------------------------------
%% @spec create_table(Tbl :: atom(), TblInfo :: [{atom(), atom()}]) -> any()
%% @doc
%%   Creates the Tbl table using TblInfo as the table structure. The
%%   table structure is a list of {column name, column type} pairs.
%%   e.g. [{name, text}, {age, integer}]
%%
%%   Returns the result of the create table call.
%% @end
%%--------------------------------------------------------------------
-spec create_table(atom(), [{atom(), atom()}]) -> any().
create_table(Tbl, Options) ->
    create_table(?MODULE, Tbl, Options).

%%--------------------------------------------------------------------
%% @spec create_table(Db :: atom(), Tbl :: atom(), Columns) -> any()
%%     Columns = [{atom(), atom()}]
%% @doc
%%   Creates the Tbl table in Db using Columns as the table structure. 
%%   The table structure is a list of {column name, column type} pairs. 
%%   e.g. [{name, text}, {age, integer}]
%%
%%   Returns the result of the create table call.
%% @end
%%--------------------------------------------------------------------
-spec create_table(atom(), atom(), [{atom(), atom()}]) -> any().
create_table(Db, Tbl, Columns) ->
    gen_server:call(Db, {create_table, Tbl, Columns}).

%%--------------------------------------------------------------------
%% @spec create_table(Db :: atom(), Tbl :: atom(), TblInfo, Constraints) -> any()
%%     Columns = [{atom(), atom()}]
%%     Constraints = [term()]
%% @doc
%%   Creates the Tbl table in Db using Columns as the table structure and
%%   Constraints as table constraints. 
%%   The table structure is a list of {column name, column type} pairs. 
%%   e.g. [{name, text}, {age, integer}]
%%
%%   Returns the result of the create table call.
%% @end
%%--------------------------------------------------------------------
-spec create_table(atom(), atom(), [{atom(), atom()}], [any()]) -> any().
create_table(Db, Tbl, Columns, Constraints) ->
    gen_server:call(Db, {create_table, Tbl, Columns, Constraints}).

%%--------------------------------------------------------------------
%% @spec list_tables() -> [atom()]
%% @doc
%%   Returns a list of tables.
%% @end
%%--------------------------------------------------------------------
-spec list_tables() -> [atom()].
list_tables() ->
    list_tables(?MODULE).

%%--------------------------------------------------------------------
%% @spec list_tables(Db :: atom()) -> [atom()]
%% @doc
%%   Returns a list of tables for Db.
%% @end
%%--------------------------------------------------------------------
-spec list_tables(atom()) -> [atom()].
list_tables(Db) ->
    gen_server:call(Db, list_tables).

%%--------------------------------------------------------------------
%% @spec table_info(Tbl :: atom()) -> [any()]
%% @doc
%%    Returns table schema for Tbl.
%% @end
%%--------------------------------------------------------------------
-spec table_info(atom()) -> [any()].
table_info(Tbl) ->
    table_info(?MODULE, Tbl).

%%--------------------------------------------------------------------
%% @spec table_info(Db :: atom(), Tbl :: atom()) -> [any()]
%% @doc
%%   Returns table schema for Tbl in Db.
%% @end
%%--------------------------------------------------------------------
-spec table_info(atom(), atom()) -> [any()].
table_info(Db, Tbl) ->
    gen_server:call(Db, {table_info, Tbl}).

%%--------------------------------------------------------------------
%% @spec write(Tbl :: atom(), Data) -> any()
%%         Data = [{Column :: atom(), Value :: sql_value()}]
%% @doc
%%   Write Data into Tbl table. Value must be of the same type as 
%%   determined from table_info/2.
%% @end
%%--------------------------------------------------------------------
-spec write(atom(), [{atom(), sql_value()}]) -> any().
write(Tbl, Data) ->
    write(?MODULE, Tbl, Data).

%%--------------------------------------------------------------------
%% @spec write(Db :: atom(), Tbl :: atom(), Data) -> term()
%%         Data = [{Column :: atom(), Value :: sql_value()}]
%% @doc
%%   Write Data into Tbl table in Db database. Value must be of the 
%%   same type as determined from table_info/3.
%% @end
%%--------------------------------------------------------------------
-spec write(atom(), atom(), [{atom(), sql_value()}]) -> any().
write(Db, Tbl, Data) ->
    gen_server:call(Db, {write, Tbl, Data}).

%%--------------------------------------------------------------------
%% @spec write_many(Tbl :: atom(), Data) -> any()
%%         Data = [[{Column :: atom(), Value :: sql_value()}]]
%% @doc
%%   Write all records in Data into table Tbl. Value must be of the 
%%   same type as determined from table_info/2.
%% @end
%%--------------------------------------------------------------------
-spec write_many(atom(), [[{atom(), sql_value()}]]) -> any().
write_many(Tbl, Data) ->
    write_many(?MODULE, Tbl, Data).

%%--------------------------------------------------------------------
%% @spec write_many(Db :: atom(), Tbl :: atom(), Data) -> term()
%%         Data = [[{Column :: atom(), Value :: sql_value()}]]
%% @doc
%%   Write all records in Data into table Tbl in database Db. Value 
%%   must be of the same type as determined from table_info/3.
%% @end
%%--------------------------------------------------------------------
-spec write_many(atom(), atom(), [[{atom(), sql_value()}]]) -> any().
write_many(Db, Tbl, Data) ->
    gen_server:call(Db, {write_many, Tbl, Data}).

%%--------------------------------------------------------------------
%% @spec update(Tbl :: atom(), Key :: atom(), Value, Data) -> Result
%%        Value = any()
%%        Data = [{Column :: atom(), Value :: sql_value()}]
%%        Result = {ok, ID} | Unknown
%%        Unknown = term()
%% @doc
%%    Updates rows into Tbl table such that the Value matches the
%%    value in Key with Data. Returns ID of the first updated
%%    record.
%% @end
%%--------------------------------------------------------------------
update(Tbl, Key, Value, Data) ->
  update(?MODULE, Tbl, Key, Value, Data).

%%--------------------------------------------------------------------
%% @spec update(Db :: atom(), Tbl :: atom(), Key :: atom(), Value, Data) -> Result
%%        Value = sql_value()
%%        Data = [{Column :: atom(), Value :: sql_value()}]
%%        Result = {ok, ID} | Unknown
%%        Unknown = term()
%% @doc
%%    Updates rows into Tbl table in Db database such that the Value
%%    matches the value in Key with Data. Returns ID of the first
%%    updated record.
%% @end
%%--------------------------------------------------------------------
update(Db, Tbl, Key, Value, Data) ->
  gen_server:call(Db, {update, Tbl, Key, Value, Data}).

%%--------------------------------------------------------------------
%% @spec read_all(Db :: atom(), Table :: atom()) -> any()
%% @doc
%%   Reads all rows from Table in Db.
%% @end
%%--------------------------------------------------------------------
-spec read_all(atom(), atom()) -> any().
read_all(Db, Tbl) ->
    gen_server:call(Db, {read, Tbl}).

%%--------------------------------------------------------------------
%% @spec read_all(Db :: atom(), Table :: atom(), Columns :: [atom()]) -> any()
%% @doc
%%   Reads Columns in all rows from Table in Db.
%% @end
%%--------------------------------------------------------------------
-spec read_all(atom(), atom(), [atom()]) -> any().
read_all(Db, Tbl, Columns) ->
    gen_server:call(Db, {read, Tbl, Columns}).

%%--------------------------------------------------------------------
%% @spec read(Tbl :: atom(), Key) -> [any()]
%%         Key = {Column :: atom(), Value :: sql_value()}
%% @doc
%%   Reads a row from Tbl table such that the Value matches the 
%%   value in Column. Value must have the same type as determined 
%%   from table_info/2.
%% @end
%%--------------------------------------------------------------------
-spec read(atom(), {atom(), any()}) -> any().
read(Tbl, Key) ->
    read(?MODULE, Tbl, Key).

%%--------------------------------------------------------------------
%% @spec read(Db :: atom(), Tbl :: atom(), Key) -> [any()]
%%         Key = {Column :: atom(), Value :: sql_value()}
%% @doc
%%   Reads a row from Tbl table in Db database such that the Value 
%%   matches the value in Column. ColValue must have the same type 
%%   as determined from table_info/3.
%% @end
%%--------------------------------------------------------------------
-spec read(atom(), atom(), {atom(), any()}) -> any().
read(Db, Tbl, {Column, Value}) ->
    gen_server:call(Db, {read, Tbl, Column, Value}).

%%--------------------------------------------------------------------
%% @spec read(Db, Tbl, Key, Columns) -> [any()]
%%        Db = atom()
%%        Tbl = atom()
%%        Key = {Column :: atom(), Value :: sql_value()}
%%        Columns = [atom()]
%% @doc
%%    Reads a row from Tbl table in Db database such that the Value
%%    matches the value in Column. Value must have the same type as 
%%    determined from table_info/3.
%% @end
%%--------------------------------------------------------------------
read(Db, Tbl, {Key, Value}, Columns) ->
  gen_server:call(Db, {read, Tbl, Key, Value, Columns}).

%%--------------------------------------------------------------------
%% @spec delete(Tbl :: atom(), Key) -> any()
%%        Key = {Column :: atom(), Value :: sql_value()}
%% @doc
%%   Delete a row from Tbl table in Db database such that the Value 
%%   matches the value in Column. 
%%   Value must have the same type as determined from table_info/3.
%% @end
%%--------------------------------------------------------------------
-spec delete(atom(), {atom(), any()}) -> any().
delete(Tbl, Key) ->
    delete(?MODULE, Tbl, Key).

%%--------------------------------------------------------------------
%% @spec delete(Db :: atom(), Tbl :: atom(), Key) -> any()
%%        Key = {Column :: atom(), Value :: sql_value()}
%% @doc
%%   Delete a row from Tbl table in Db database such that the Value 
%%   matches the value in Column. 
%%   Value must have the same type as determined from table_info/3.
%% @end
%%--------------------------------------------------------------------
-spec delete(atom(), atom(), {atom(), any()}) -> any().
delete(Db, Tbl, Key) ->
    gen_server:call(Db, {delete, Tbl, Key}).

%%--------------------------------------------------------------------
%% @spec drop_table(Tbl :: atom()) -> any()
%% @doc
%%   Drop the table Tbl.
%% @end
%%--------------------------------------------------------------------
-spec drop_table(atom()) -> any().
drop_table(Tbl) ->
    drop_table(?MODULE, Tbl).

%%--------------------------------------------------------------------
%% @spec drop_table(Db :: atom(), Tbl :: atom()) -> any()
%% @doc
%%   Drop the table Tbl from Db database.
%% @end
%%--------------------------------------------------------------------
-spec drop_table(atom(), atom()) -> any().
drop_table(Db, Tbl) ->
    gen_server:call(Db, {drop_table, Tbl}).


%%--------------------------------------------------------------------
%% @spec create_function(Db :: atom(), FunctionName :: atom(), Function :: function()) -> term()
%% @doc
%%   Creates function under name FunctionName.
%%
%% @end
%%--------------------------------------------------------------------
-spec create_function(atom(), atom(), function()) -> any().
create_function(Db, FunctionName, Function) ->
    gen_server:call(Db, {create_function, FunctionName, Function}).

%%--------------------------------------------------------------------
%% @spec begin_transaction(Db :: atom()) -> term()
%% @doc
%%   Begins a transaction in Db.
%% @end
%%--------------------------------------------------------------------
-spec begin_transaction(atom()) -> any().
begin_transaction(Db) ->
    SQL = "BEGIN;",
    sql_exec(Db, SQL).

%%--------------------------------------------------------------------
%% @spec commit_transaction(Db :: atom()) -> term()
%% @doc
%%   Commits the current transaction in Db.
%% @end
%%--------------------------------------------------------------------
-spec commit_transaction(atom()) -> any().
commit_transaction(Db) ->
    SQL = "COMMIT;",
    sql_exec(Db, SQL).

%%--------------------------------------------------------------------
%% @spec rollback_transaction(Db :: atom()) -> term()
%% @doc
%%   Rolls back the current transaction in Db.
%% @end
%%--------------------------------------------------------------------
-spec rollback_transaction(atom()) -> any().
rollback_transaction(Db) ->
    SQL = "ROLLBACK;",
    sql_exec(Db, SQL).

%%--------------------------------------------------------------------
%% @spec value_to_sql_unsafe(Value :: sql_value()) -> iolist()
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
%% @spec value_to_sql(Value :: sql_value()) -> iolist()
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
%% @spec init(Args) -> {ok, State} |
%%                         {ok, State, Timeout} |
%%                         ignore               |
%%                         {stop, Reason}
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
      {error, Error} ->
        Msg = io_lib:format("Error loading ~p: ~p", [?DRIVER_NAME, erl_ddll:format_error(Error)]),
        {stop, lists:flatten(Msg)}
    end.

%%--------------------------------------------------------------------
%% @spec handle_call(Request, From, State) -> {reply, Reply, State} |
%%                                      {reply, Reply, State, Timeout} |
%%                                      {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, Reply, State} |
%%                                      {stop, Reason, State}
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
    SQL = "select name from sqlite_master where type='table';",
    Data = do_sql_exec(SQL, State),
    TableList = proplists:get_value(rows, Data),
    TableNames = [erlang:list_to_atom(erlang:binary_to_list(Name)) || {Name} <- TableList],
    {reply, TableNames, State};
handle_call({table_info, Tbl}, _From, State) ->
    % make sure we only get table info.
    % SQL Injection warning
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
handle_call({create_function, FunctionName, Function}, _From, #state{port = Port} = State) ->
    % make sure we only get table info.
    % SQL Injection warning
    Reply = exec(Port, {create_function, FunctionName, Function}),
    {reply, Reply, State};
handle_call({sql_exec, SQL}, _From, State) ->
    do_handle_call_sql_exec(SQL, State);
handle_call({create_table, Tbl, Columns}, _From, State) ->
    SQL = sqlite3_lib:create_table_sql(Tbl, Columns),
    do_handle_call_sql_exec(SQL, State);
handle_call({create_table, Tbl, Columns, Constraints}, _From, State) ->
    SQL = sqlite3_lib:create_table_sql(Tbl, Columns, Constraints),
    do_handle_call_sql_exec(SQL, State);
handle_call({update, Tbl, Key, Value, Data}, _From, State) ->
    SQL = sqlite3_lib:update_sql(Tbl, Key, Value, Data),
    do_handle_call_sql_exec(SQL, State);
handle_call({write, Tbl, Data}, _From, State) ->
    % insert into t1 (data,num) values ('This is sample data',3);
    SQL = sqlite3_lib:write_sql(Tbl, Data),
    do_handle_call_sql_exec(SQL, State);
handle_call({write_many, Tbl, DataList}, _From, State) ->
    do_sql_exec("BEGIN;", State),
    [do_sql_exec(sqlite3_lib:write_sql(Tbl, Data), State) || Data <- DataList],
    do_handle_call_sql_exec("COMMIT;", State);
handle_call({read, Tbl}, _From, State) ->
    % select * from  Tbl where Key = Value;
    SQL = sqlite3_lib:read_sql(Tbl),
    do_handle_call_sql_exec(SQL, State);
handle_call({read, Tbl, Columns}, _From, State) ->
    SQL = sqlite3_lib:read_sql(Tbl, Columns),
    do_handle_call_sql_exec(SQL, State);
handle_call({read, Tbl, Key, Value}, _From, State) ->
    % select * from  Tbl where Key = Value;
    SQL = sqlite3_lib:read_sql(Tbl, Key, Value),
    do_handle_call_sql_exec(SQL, State);
handle_call({read, Tbl, Key, Value, Columns}, _From, State) ->
    SQL = sqlite3_lib:read_sql(Tbl, Key, Value, Columns),
    do_handle_call_sql_exec(SQL, State);
handle_call({delete, Tbl, {Key, Value}}, _From, State) ->
    % delete from Tbl where Key = Value;
    SQL = sqlite3_lib:delete_sql(Tbl, Key, Value),
    do_handle_call_sql_exec(SQL, State);
handle_call({drop_table, Tbl}, _From, State) ->
    SQL = sqlite3_lib:drop_table_sql(Tbl),
    do_handle_call_sql_exec(SQL, State);
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
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
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% @doc Handling all non call/cast messages
%% @end
%% @hidden
%%--------------------------------------------------------------------
-spec handle_info(any(), #state{}) -> {'noreply', #state{}}.
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @spec terminate(Reason, State) -> void()
%% @doc This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any necessary
%% cleaning up. When it returns, the gen_server terminates with Reason.
%% The return value is ignored.
%% @end
%% @hidden
%%--------------------------------------------------------------------
-spec terminate(atom(), tuple()) -> atom().
terminate(normal, #state{port = Port}) ->
    port_command(Port, term_to_binary({close, nop})),
    port_close(Port),
    ok;
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% Func: code_change(OldVsn, State, Extra) -> {ok, NewState}
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

create_port_cmd(DbFile) ->
    atom_to_list(?DRIVER_NAME) ++ " " ++ DbFile.

do_handle_call_sql_exec(SQL, State) ->
    Reply = do_sql_exec(SQL, State),
    {reply, Reply, State}.

do_sql_exec(SQL, #state{port = Port}) ->
    ?dbg("SQL: ~s~n", [SQL]),
    exec(Port, {sql_exec, SQL}).

exec(_Port, {create_function, _FunctionName, _Function}) ->
  error_logger:error_report([{application, sqlite3}, "NOT IMPL YET"]);
  %port_control(Port, ?SQL_CREATE_FUNCTION, list_to_binary(Cmd)),
  %wait_result(Port);
exec(Port, {sql_exec, Cmd}) ->
  port_control(Port, ?SQL_EXEC_COMMAND, Cmd),
  wait_result(Port).

wait_result(Port) ->
  receive
    %% Messages given at http://www.erlang.org/doc/reference_manual/ports.html
    {Port, Reply} ->
      % ?dbg("Reply: ~p~n", [Reply]),
      Reply;
    {error, Reason} ->
      error_logger:error_msg("sqlite3 driver error: ~s~n", [Reason]),
      % ?dbg("Error: ~p~n", [Reason]),
      {error, Reason};
    {'EXIT', Port, Reason} ->
      error_logger:error_msg("sqlite3 driver port closed with reason ~p~n", [Reason]),
      % ?dbg("Error: ~p~n", [Reason]),
      {error, Reason}
  end.

parse_table_info(Info) ->
    [_, Tail] = string:tokens(Info, "()"),
    Cols = string:tokens(Tail, ","),
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
build_constraints(["UNIQUE" | Tail]) -> [unique | build_constraints(Tail)];
build_constraints(["NOT", "NULL" | Tail]) -> [not_null | build_constraints(Tail)];
build_constraints(["DEFAULT", DefaultValue | Tail]) -> [{default, sqlite3_lib:sql_to_value(DefaultValue)} | build_constraints(Tail)].
% build_constraints(["CHECK", Check | Tail]) -> ...
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
%% @type sql_value() = number() | 'null' | iodata().
%% 
%% Values accepted in SQL statements include numbers, atom 'null',
%% and io:iolist().
%% @end
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% Tests
%%--------------------------------------------------------------------
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.
