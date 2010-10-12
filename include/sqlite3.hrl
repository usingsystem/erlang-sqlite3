-define(NULL_ATOM, null).
-type(sql_value() :: number() | ?NULL_ATOM | iodata()).

%%--------------------------------------------------------------------
%% @type sql_value() :: number() | 'null' | iodata().
%% 
%% Values accepted in SQL statements include numbers, atom 'null',
%% and iodata().
%% @end
%%--------------------------------------------------------------------
