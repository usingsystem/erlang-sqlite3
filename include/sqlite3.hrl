-ifdef(DEBUG).
-include_lib("eunit/include/eunit.hrl"). %% for debugging macros
-define(dbg(Message), ?debugMsg(Message)).
-define(dbg(Format, Data), ?debugFmt(Format, Data)).
-define(dbgVal(Expr), ?debugVal(Expr)).
-define(dbgTime(Text, Expr), ?debugTime(Text, Expr)).
-else.
-define(dbg(_Message), ok).
-define(dbg(_Format, _Data), ok).
-define(dbgVal(Expr), Expr).
-define(dbgTime(_Text, Expr), Expr).
%% -ifdef(TEST).
%% -include_lib("eunit/include/eunit.hrl"). %% for debugging macros
%% -else.
%% -define(debugMsg(_Message), ok).
%% -define(debugFmt(_Format, _Data), ok).
%% -define(debugVal(Expr), Expr).
%% -define(debugTime(_Text, Expr), Expr).
%% -endif.
-endif.

-define(NULL_ATOM, null).
-type(sql_value() :: number() | ?NULL_ATOM | iodata() | {blob, binary()}).
