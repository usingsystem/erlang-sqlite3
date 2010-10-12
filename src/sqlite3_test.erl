%%%-------------------------------------------------------------------
%%% File    : sqlite3_test.erl
%%% Author  : Tee Teoh <tteoh@teemac.ott.cti.com>
%%% Description :
%%%
%%% Created : 10 Jun 2008 by Tee Teoh <tteoh@teemac.ott.cti.com>
%%%-------------------------------------------------------------------
-module(sqlite3_test).

-include_lib("eunit/include/eunit.hrl").

create_table_test() ->
    file:delete("ct.db"),
    sqlite3:open(ct),
    sqlite3:create_table(ct, user, [{name, text}, {age, integer},
                                    {wage, integer}]),
    [user] = sqlite3:list_tables(ct),
    [{name, primary_key}, {age, integer}, {wage, integer}] =
        sqlite3:table_info(ct, user),
    sqlite3:write(ct, user, [{name, "abby"}, {age, 20}, {wage, 2000}]),
    sqlite3:write(ct, user, [{name, "marge"}, {age, 30}, {wage, 3000}]),
    sqlite3:sql_exec(ct, "select * from user;"),
    sqlite3:read(ct, user, {name, "abby"}),
    sqlite3:delete(ct, user, {name, "abby"}),
    sqlite3:drop_table(ct, user),
    sqlite3:close(ct).
