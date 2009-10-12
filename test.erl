#!/usr/bin/env escript
%%! -smp enable -pa ebin -name testsqlite3
main(_) ->
  ct:run("src").

