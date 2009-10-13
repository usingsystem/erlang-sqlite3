#!/usr/bin/env escript
%%! -smp enable -pa ebin -name testsqlite3

-record(user, {name, age, wage}).

test() ->
    sqlite3:open(ct),
    sqlite3:create_table(ct, user, [{name, text}, {age, integer}, {wage, integer}]),
    [user] = sqlite3:list_tables(ct),
    [{name, text}, {age, integer}, {wage, integer}] = sqlite3:table_info(ct, user),
    sqlite3:write(ct, user, [{name, "abby"}, {age, 20}, {wage, 2000}]),
    sqlite3:write(ct, user, [{name, "marge"}, {age, 30}, {wage, 3000}]),
    sqlite3:sql_exec(ct, "select * from user;"),
    sqlite3:read(ct, user, {name, "abby"}),
    sqlite3:delete(ct, user, {name, "abby"}),
    sqlite3:drop_table(ct, user),
%sqlite3:delete_db(ct)
    sqlite3:close(ct).

main(_) ->
  try test() of
    _ -> ok
  catch
    Class:Error ->
      io:format("~p:~p:~p~n", [Class, Error, erlang:get_stacktrace()])
  end.