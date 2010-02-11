#!/usr/bin/env escript
%%! -smp enable -pa ebin -sname testsqlite3

-record(user, {name, age, wage}).

test() ->
    file:delete("store.db"),
    sqlite3:open(ct),
    sqlite3:create_table(ct, user, [{id, integer}, {name, text}, {age, integer}, {wage, integer}]),
    [user] = sqlite3:list_tables(ct),
    [{id, primary_key}, {name, text}, {age, integer}, {wage, integer}] = sqlite3:table_info(ct, user),
    {id, Id1} = sqlite3:write(ct, user, [{name, "abby"}, {age, 20}, {wage, 2000}]),
    Id1 = 1,
    {id, Id2} = sqlite3:write(ct, user, [{name, "marge"}, {age, 30}, {wage, 3000}]),
    Id2 = 2,
    [{columns, [id, name, age, wage]}, {rows, [{1, <<"abby">>, 20, 2000}, {2, <<"marge">>, 30, 3000}]}] = sqlite3:sql_exec(ct, "select * from user;"),
    sqlite3:read(ct, user, {name, "abby"}),
    sqlite3:delete(ct, user, {name, "abby"}),
    sqlite3:drop_table(ct, user),
%sqlite3:delete_db(ct)
    sqlite3:close(ct),
    io:format("Tests passed~n").

main(_) ->
  try test() of
    _ -> ok
  catch
    Class:Error ->
      io:format("~p:~p:~p~n", [Class, Error, erlang:get_stacktrace()])
  end.
