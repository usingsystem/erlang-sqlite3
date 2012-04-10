-module(sqlite3).  

-export([open/1, open_link/1]).

%Sqlite3 API
-export([alter/2,
		 analyze/2,
		 attach/2,
		 create/2,
		 delete/2,
		 detach/2,
		 drop/2,
		 insert/2,
		 pragma/2,
		 replace/2,
		 select/2,
		 tables/1,
		 transac/2,
		 update/2,
		 vaccum/1]).

-behavior(gen_server).

%gen_server callbacks
-export([init/1,
        handle_call/3,
		priorities_call/3,
        handle_cast/2,
        handle_info/2,
        terminate/2,
        code_change/3]).

-define('DRIVER_NAME', 'sqlite3_drv').

-define(SQLITE_CMD, [{
	{alter, 1},
	{analyze, 2},
	{attach, 3},
	{create, 4},
	{delete, 5},
	{drop, 6},
	{insert, 7},
	{pragma, 8},
	{replace, 9},
	{select, 10},
	{update, 11},
	{vaccum, 12},
	{'begin', 100},
	{'commit', 101}
}]).

-record(state, {port, file}).

open(File) ->
	gen_server2:start(?MODULE, [File], []).

open_link(File) ->
	gen_server2:start_link(?MODULE, [File], []).

%% -----------------------------------------------------------------
%% ALTER TABLE [database-name.] table-name RENAME TO new-table-name
%% ALTER TABLE [database-name.] table-name ADD [COLUMN] column-def
%% ----------------------------------------------------------------- 
alter(DB, SQL) when is_list(SQL) ->
	gen_server2:call(DB, {alter, SQL}).

%% -----------------------------------------------------------------
%% ANALYZE database-name
%% ANALYZE [database-name.] table-or-index-name
%% -----------------------------------------------------------------
analyze(DB, SQL) when is_list(SQL) ->
	gen_server2:call(DB, {analyze, SQL}).

%% -----------------------------------------------------------------
%% ATTACH [DATABASE] expr AS database-name
%% -----------------------------------------------------------------
attach(DB, SQL) -> 
	gen_server2:call(DB, {attach, SQL}).

%% -----------------------------------------------------------------
%% http://sqlite.org/images/syntax/create-table-stmt.gif
%% CREATE TABLE [database-name.] table-name (column-def,)+

%% http://sqlite.org/images/syntax/create-index-stmt.gif
%% CREATE [UNIQUE] INDEX index-name ON table-name (indexed-column,)+

%% http://sqlite.org/images/syntax/create-trigger-stmt.gif

%% http://sqlite.org/images/syntax/create-view-stmt.gif
%% CREATE [TEMP|TEMPORARY] VIEW [database-name.] view-name AS select-stmt
%% -----------------------------------------------------------------
create(DB, SQL) ->
	gen_server2:call(DB, {create, SQL}).

%% -----------------------------------------------------------------
%% DELETE FROM table-name WHERE expr.
%% -----------------------------------------------------------------
delete(DB, SQL) ->
	gen_server2:call(DB, {delete, SQL}).

%% -----------------------------------------------------------------
%% DETACH DATABASE database-name
%% -----------------------------------------------------------------
detach(DB, SQL) ->
	gen_server2:call(DB, {detach, SQL}).

%% -----------------------------------------------------------------
%% DROP INDEX [IF EXISTS] [database-name.] index-name
%% DROP TABLE [IF EXISTS] [database-name.] table-name
%% DROP TRIGGER [IF EXISTS] [database-name.] trigger-name
%% DROP VIEW [IF EXISTS] [database-name.] view-name
%% -----------------------------------------------------------------
drop(DB, SQL) ->
	gen_server2:call(DB, {drop, SQL}).

%% -----------------------------------------------------------------
%% http://sqlite.org/images/syntax/insert-stmt.gif
%% INSERT INTO [database-name.] table-name [columns] VALUES values
%% -----------------------------------------------------------------
insert(DB, SQL) ->
	gen_server2:call(DB, {insert, SQL}).

%% -----------------------------------------------------------------
%% PRAGMA [database-name.] pragma-name [=pragma-value]
%% -----------------------------------------------------------------
pragma(DB, SQL) ->
	gen_server2:call(DB, {pragma, SQL}).

%% -----------------------------------------------------------------
%% REPLACE: alias for INSERT OR REPLACE
%% -----------------------------------------------------------------
replace(DB, SQL) ->
	gen_server2:call(DB, {replace, SQL}).

%% -----------------------------------------------------------------
%% http://sqlite.org/lang_select.html
%% -----------------------------------------------------------------
select(DB, SQL) ->
	gen_server2:call(DB, {select, SQL}).

%% -----------------------------------------------------------------
%% .tables
%% -----------------------------------------------------------------
tables(DB) ->
	gen_server2:call(DB, tables).

%% -----------------------------------------------------------------
%% BEGIN SQL COMMIT
%% -----------------------------------------------------------------
transac(DB, SQL) ->
	gen_server2:call(DB, {transac, SQL}).

%% -----------------------------------------------------------------
%% http://sqlite.org/lang_update.html
%% -----------------------------------------------------------------
update(DB, SQL) ->
	gen_server2:call(DB, {update, SQL}).

%% -----------------------------------------------------------------
%% http://sqlite.org/lang_vacuum.html
%% -----------------------------------------------------------------
vaccum(DB) ->
	gen_server2:call(DB, vacuum).

init([File]) ->
	[put({oper, Oper}, Id)  || {Oper, Id} <- ?SQLITE_CMD],
    PortCmd = atom_to_list(?DRIVER_NAME) ++ " " ++ File,
    case erl_ddll:load(priv_dir(), atom_to_list(?DRIVER_NAME)) of
	ok -> 
		Port = open_port({spawn, PortCmd}, [binary]),
		{ok, #state{port = Port, file = File}};
	{error, permanent} -> %% already loaded!
		Port = open_port({spawn, PortCmd}, [binary]),
		{ok, #state{port = Port, file = File}};      
	{error, Error} ->
		Msg = io_lib:format("Error loading ~p: ~s", 
			[?DRIVER_NAME, erl_ddll:format_error(Error)]),
		{stop, lists:flatten(Msg)}
    end.

%TODO: not supported
handle_call({transac, SQL}, _From, #state{port = Port} = State) ->
	ok = erlang:port_call(Port, get({oper, 'begin'}), "BEGIN"),
	ok = erlang:port_call(Port, get({oper, 'commit'}), "COMMIT"),
	{reply, ok, State};

handle_call(tables, _From, #state{port = Port} = State) ->
    SQL = "select name, sql from sqlite_master where type='table';",
	Reply = erlang:port_call(Port, get({oper, select}), SQL),
	{reply, Reply, State};

handle_call({Oper, SQL}, _From, #state{port = Port} = State) ->
	Reply = erlang:port_call(Port, get({oper, Oper}), SQL),
	{reply, Reply, State};

handle_call(Req, _From, State) ->
	{stop, {error, {badreq, Req}}, State}.

priorities_call({select, _}, _From, _State) ->
	10;
priorities_call(_, _From, _State) ->
	0.

handle_cast(Msg, State) ->
	{stop, {error, {badmsg, Msg}}, State}.

handle_info(Info, State) ->
	{stop, {error, {badinfo, Info}}, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
	
priv_dir() ->
    case code:priv_dir(sqlite3) of
        {error, bad_name} ->
            {?MODULE, _, FileName} = code:get_object_code(?MODULE),
            filename:join(filename:dirname(FileName), "../priv");
        Dir ->
            Dir
    end.

