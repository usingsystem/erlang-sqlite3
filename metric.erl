#!/usr/bin/env escript
%%! -smp enable -pa ebin -sname testsqlite3

%-record(metric, {node, timestamp, metric, value}).

main([N]) ->
    %file:delete("metric.db"),
    sqlite3:open(metric),
    %sqlite3:create_table(metric, metric, [{id, integer, [primary_key]}, {node, text}, {timestamp, integer}, {metric, integer}, {value, real}]),
    Tables = sqlite3:list_tables(metric),
	io:format("tables: ~p~n", [Tables]),
	[io:format("~p table: ~p~n", [Tab, sqlite3:table_info(metric, Tab)]) || Tab <- Tables],

	erlang:statistics(runtime),
	lists:foreach(fun(_) -> 
		Records = [ [{node, "node" ++ integer_to_list(I)}, 
				     {timestamp, I}, 
				     {metric, "metric" ++ integer_to_list(I rem 50)},
				     {value, I}] ||  I <- lists:seq(1, 3000) ],
		sqlite3:write_many(metric, metric, Records)
	end, lists:seq(1, list_to_integer(N))),
	io:format("time: ~p~n", [erlang:statistics(runtime)]);

main(_) ->
	io:format("Usage: metric.erl N(1000)~n").
