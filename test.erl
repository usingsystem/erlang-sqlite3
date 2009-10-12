#!/usr/bin/env escript
%%! -smp enable -pa ebin
main(_) ->
  sqlite3_store:start_link().

