%%%-------------------------------------------------------------------
%%% File    : sqlite3_test.erl
%%% Author  : Tee Teoh <tteoh@teemac.ott.cti.com>
%%% Description : 
%%%
%%% Created : 10 Jun 2008 by Tee Teoh <tteoh@teemac.ott.cti.com>
%%%-------------------------------------------------------------------
-module(sqlite3_test).

%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% Function: 
%% Description:
%%--------------------------------------------------------------------
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

create_table_test() ->
    file:delete("ct.db"),
    sqlite3:open(ct),
    sqlite3:create_table(ct, user, [{id, integer, [primary_key]}, {name, text}, {age, integer}, {wage, integer}]),
    [user] = sqlite3:list_tables(ct),
    [{id, integer, [primary_key]}, {name, text}, {age, integer}, {wage, integer}] = sqlite3:table_info(ct, user),
    {id, Id1} = sqlite3:write(ct, user, [{name, "abby"}, {age, 20}, {wage, 2000}]),
    Id1 = 1,
    {id, Id2} = sqlite3:write(ct, user, [{name, "marge"}, {age, 30}, {wage, 2000}]),
    Id2 = 2,
    [{columns, Columns}, {rows, Rows1}] = sqlite3:sql_exec(ct, "select * from user;"),
	Columns = ["id", "name", "age", "wage"],
	Rows1 = [{1, <<"abby">>, 20, 2000}, {2, <<"marge">>, 30, 2000}],
    [{columns, Columns}, {rows, Rows2}] = sqlite3:read(ct, user, {name, "abby"}),
	Rows2 = [{1, <<"abby">>, 20, 2000}],
    [{columns, Columns}, {rows, Rows1}] = sqlite3:read(ct, user, {wage, 2000}),
    sqlite3:delete(ct, user, {name, "abby"}),
    sqlite3:drop_table(ct, user),
%sqlite3:delete_db(ct)
    sqlite3:close(ct).

-endif.

% create, read, update, delete
%%====================================================================
%% Internal functions
%%====================================================================
