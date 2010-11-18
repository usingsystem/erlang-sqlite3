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

-define(FuncTest(Name), {??Name, fun Name/0}).

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

all_test_() ->
    {setup,
     fun open_db/0,
     fun close_db/1,
     [?FuncTest(basic_functionality),
      ?FuncTest(blob),
      ?FuncTest(escaping),
      ?FuncTest(select_many_records),
      ?FuncTest(nonexistent_table_info),
      ?FuncTest(large_number)]}.

open_db() ->
    sqlite3:open(ct).

close_db({ok, _Pid}) ->
    sqlite3:close(ct);
close_db(_) ->
    ok.

basic_functionality() ->
    Columns = ["id", "name", "age", "wage"],
    AllRows = [{1, <<"abby">>, 20, 2000}, {2, <<"marge">>, 30, 2000}],
    AbbyOnly = [{1, <<"abby">>, 20, 2000}],
    TableInfo = [{id, integer, [primary_key]}, {name, text}, {age, integer}, {wage, integer}],
    drop_all_tables(ct),
    ?assertEqual(
        [], 
        sqlite3:list_tables(ct)),
    {ok, TableId} = sqlite3:create_table(ct, user, TableInfo),
    ?assertEqual(
        [user], 
        sqlite3:list_tables(ct)),
    ?assertEqual(
        TableInfo, 
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
        sqlite3:drop_table(ct, user)).

blob() ->
    drop_table_if_exists(ct, blobs),
    sqlite3:create_table(ct, blobs, [{blob_col, blob}]),
    sqlite3:write(ct, blobs, [{blob_col, {blob, <<0,255,1,2>>}}]),
    ?assertEqual(
        [{columns, ["blob_col"]}, {rows, [{<<0,255,1,2>>}]}], 
        sqlite3:read_all(ct, blobs)).

escaping() ->
    drop_table_if_exists(ct, escaping),
    sqlite3:create_table(ct, escaping, [{str, text}]),
    Strings = ["a'", "b\"c", "d''e", "f\"\""],
    Input = [[{str, String}] || String <- Strings],
    ExpectedRows = [{list_to_binary(String)} || String <- Strings],
    sqlite3:write_many(ct, escaping, Input),
    ?assertEqual(
        [{columns, ["str"]}, {rows, ExpectedRows}], 
        sqlite3:read_all(ct, escaping)).

select_many_records() ->
    drop_table_if_exists(ct, many_records),
    sqlite3:create_table(ct, many_records, [{id, integer}, {name, text}]),
    sqlite3:write_many(ct, many_records, [[{id, X}, {name, "bar"}] || X <- lists:seq(1, 1024)]),
%%     sqlite3:begin_transaction(ct),
%%     [sqlite3:write(ct, many_records, [{id, X}, {name, "bar"}]) || X <- lists:seq(1, 1024)], %% takes very long :(
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
        length(rows(sqlite3:sql_exec(ct, "select * from many_records;")))).

nonexistent_table_info() ->
    ?assertEqual(table_does_not_exist, sqlite3:table_info(ct, nonexistent)).

large_number() ->
    N1 = 4294967295,
    N2 = (N1 + 1) div 2,
    Query1 = io_lib:format("select ~p, ~p", [N1, N2]),
    ?assertEqual([{N1, N2}], rows(sqlite3:sql_exec(ct, Query1))).

% create, read, update, delete
%%====================================================================
%% Internal functions
%%====================================================================
