%%%-------------------------------------------------------------------
%%% File    : sqlite3_test.erl
%%% Author  : Tee Teoh <tteoh@teemac.ott.cti.com>
%%% Description :
%%%
%%% Created : 10 Jun 2008 by Tee Teoh <tteoh@teemac.ott.cti.com>
%%%-------------------------------------------------------------------
-module(sqlite3_test).

%% ====================================================================
%% API
%% ====================================================================
%% --------------------------------------------------------------------
%% Function: 
%% Description:
%% --------------------------------------------------------------------
-include_lib("eunit/include/eunit.hrl").

drop_all_tables(Db) ->
	Tables = sqlite3:list_tables(Db),
	[sqlite3:drop_table(Db, Table) || Table <- Tables],
	Tables.

drop_table_if_exists(Db, Table) ->
	case lists:member(Table, sqlite3:list_tables(Db)) of
		true -> sqlite3:drop_table(Db, Table);
		false -> ok
	end.

rows(SqlExecReply) ->
	[{columns, _Columns}, {rows, Rows}] = SqlExecReply,
	Rows.

basic_functionality_test() ->
	Columns = ["id", "name", "age", "wage"],
	AllRows = [{1, <<"abby">>, 20, 2000}, {2, <<"marge">>, 30, 2000}],
	AbbyOnly = [{1, <<"abby">>, 20, 2000}],
	sqlite3:open(ct),
	drop_all_tables(ct),
    ?assertEqual(
        [], 
        sqlite3:list_tables(ct)),
	{ok, TableId} = sqlite3:create_table(ct, user, [{id, integer, [primary_key]}, {name, text}, {age, integer}, {wage, integer}]),
    ?assertEqual(
        [user], 
        sqlite3:list_tables(ct)),
    ?assertEqual(
        [{id, integer, [primary_key]}, {name, text}, {age, integer}, {wage, integer}], 
        sqlite3:table_info(ct, user)),
    ?assertEqual(
        {id, 1}, 
        sqlite3:write(ct, user, [{name, "abby"}, {age, 20}, {wage, 2000}])),
    ?assertEqual(
        {id, 2}, 
        sqlite3:write(ct, user, [{name, "marge"}, {age, 30}, {wage, 2000}])),
    ?assertEqual(
        [{columns, Columns}, {rows, AllRows}], 
        sqlite3:sql_exec(ct, "select * from user;")),
    ?assertEqual(
        [{columns, Columns}, {rows, AllRows}], 
        sqlite3:read_all(ct, user)),
    ?assertEqual(
        [{columns, ["name"]}, {rows, [{<<"abby">>}, {<<"marge">>}]}], 
        sqlite3:read_all(ct, user, [name])),
    ?assertEqual(
        [{columns, Columns}, {rows, AbbyOnly}], 
        sqlite3:read(ct, user, {name, "abby"})),
    ?assertEqual(
        [{columns, Columns}, {rows, AllRows}], 
        sqlite3:read(ct, user, {wage, 2000})),
    ?assertEqual(
        {ok, TableId}, 
        sqlite3:delete(ct, user, {name, "marge"})),
    ?assertEqual(
        [{columns, Columns}, {rows, AbbyOnly}], 
        sqlite3:sql_exec(ct, "select * from user;")),
    ?assertEqual(
        {ok, TableId}, 
        sqlite3:drop_table(ct, user)),
	sqlite3:close(ct).

select_many_records_test() ->
	sqlite3:open(ct),
	drop_table_if_exists(ct, many_records),
    sqlite3:create_table(ct, many_records, [{id, integer}, {name, text}]),
    sqlite3:write_many(ct, many_records, [[{id, X}, {name, "bar"}] || X <- lists:seq(1, 1024)]),
%%     sqlite3:begin_transaction(ct),
%% 	[sqlite3:write(ct, many_records, [{id, X}, {name, "bar"}]) || X <- lists:seq(1, 1024)], %% takes very long :(
%%     sqlite3:commit_transaction(ct),
	Columns = ["id", "name"],
    ?assertEqual(
        [{columns, Columns}, {rows, [{1, <<"bar">>}]}], 
	    sqlite3:read(ct, many_records, {id, 1})),
    ?assertEqual(
        10, 
	    length(rows(sqlite3:sql_exec(ct, "select * from many_records limit 10;")))),
    ?assertEqual(
        100, 
	    length(rows(sqlite3:sql_exec(ct, "select * from many_records limit 100;")))),
    ?assertEqual(
        1000, 
	    length(rows(sqlite3:sql_exec(ct, "select * from many_records limit 1000;")))),
    ?assertEqual(
        1024, 
	    length(rows(sqlite3:sql_exec(ct, "select * from many_records;")))),
	sqlite3:close(ct).

nonexistent_table_info_test() ->
	sqlite3:open(ct),
	?assertEqual(table_does_not_exist, sqlite3:table_info(ct, nonexistent)),
	sqlite3:close(ct).

% create, read, update, delete
%%====================================================================
%% Internal functions
%%====================================================================
