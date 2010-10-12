%%%-------------------------------------------------------------------
%%% File    : sqlite3_lib.erl
%%% @author Tee Teoh
%%% @copyright 21 Jun 2008 by Tee Teoh 
%%% @version 1.0.0
%%% @doc Library module for sqlite3
%%% @end
%%%-------------------------------------------------------------------
-module(sqlite3_lib).
-include("sqlite3.hrl").

%% API
-export([col_type_to_atom/1]).
-export([value_to_sql/1, value_to_sql_unsafe/1, escape/1]).
-export([write_value_sql/1, write_col_sql/1]).
-export([create_table_sql/2, write_sql/2, read_sql/3, delete_sql/3, drop_table/1]). 
-export([update_sql/4, update_set_sql/1, replace/1]).
-export([read_sql/4, read_cols_sql/1]).

%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% @spec col_type_to_string(Type :: atom()) -> string()
%% @doc Maps sqlite3 column type.
%% @end
%%--------------------------------------------------------------------
-spec col_type_to_string(atom()) -> string().
col_type_to_string(integer) ->
    "INTEGER";
col_type_to_string(text) ->
    "TEXT";
col_type_to_string(double) ->
    "REAL";
col_type_to_string(real) ->
    "REAL".

%%--------------------------------------------------------------------
%% @spec col_type_to_atom(Type :: string()) -> atom()
%% @doc Maps sqlite3 column type.
%% @end
%%--------------------------------------------------------------------
-spec col_type_to_atom(string()) -> atom().
col_type_to_atom("INTEGER") ->
    integer;
col_type_to_atom("TEXT") ->
    text;
col_type_to_atom("REAL") ->
    double.

%%--------------------------------------------------------------------
%% @spec value_to_sql_unsafe(Value :: sql_value()) -> iodata()
%% @doc 
%%    Converts an Erlang term to an SQL string.
%%    Currently supports integers, floats, 'null' atom, and iodata 
%%    (binaries and iolists) which are treated as SQL strings.
%%
%%    Note that it opens opportunity for injection if an iolist includes 
%%    single quotes! Replace all single quotes (') with '' manually, or
%%    use value_to_sql/1 if you are not sure if your strings contain
%%    single quotes (e.g. can be entered by users).
%% @end
%%--------------------------------------------------------------------
-spec value_to_sql_unsafe(sql_value()) -> iodata().
value_to_sql_unsafe(X) ->
	if
		is_integer(X)   -> integer_to_list(X);
		is_float(X)     -> float_to_list(X);
		X == ?NULL_ATOM -> "NULL";
		true            -> [$', X, $'] %% assumes no $' inside strings!
	end.

%%--------------------------------------------------------------------
%% @spec value_to_sql(Value :: sql_value()) -> iodata()
%% @doc 
%%    Converts an Erlang term to an SQL string.
%%    Currently supports integers, floats, 'null' atom, and iodata 
%%    (binaries and iolists) which are treated as SQL strings.
%%
%%    All single quotes (') will be replaced with ''.
%% @end
%%--------------------------------------------------------------------
-spec value_to_sql(sql_value()) -> iolist().
value_to_sql(X) ->
	if
		is_integer(X)   -> integer_to_list(X);
		is_float(X)     -> float_to_list(X);
		X == ?NULL_ATOM -> "NULL";
		true            -> [$', escape(X), $']
	end.

%%--------------------------------------------------------------------
%% @spec write_value_sql(Value :: [term()]) -> string()
%% @doc 
%%    Creates the values portion of the sql stmt.
%% @end
%%--------------------------------------------------------------------
-spec write_value_sql(any()) -> string().
write_value_sql(Values) ->
    map_intersperse(fun value_to_sql/1, Values, ",").

%%--------------------------------------------------------------------
%% @spec write_col_sql([atom()]) -> string()
%% @doc Creates the column/data stmt for SQL.
%% @end
%%--------------------------------------------------------------------
-spec write_col_sql([atom()]) -> string().
write_col_sql(Cols) ->
    map_intersperse(fun atom_to_list/1, Cols, ",").

%%--------------------------------------------------------------------
%% @spec replace(String) -> string()
%%        String = string()
%% @doc Returns copy of String for which \" replaced with '
%% @end
%%--------------------------------------------------------------------
-spec replace(string()) -> string().
replace([]) -> [];
replace([$" | T]) ->
  [$' | replace(T)];
replace([H | T]) ->
  [H | replace(T)].

%%--------------------------------------------------------------------
%% @spec escape(IoData :: iodata()) -> iodata()
%% 
%% @doc Returns copy of IoData with all ' replaced by ''
%% @end
%%--------------------------------------------------------------------
-spec escape(iodata()) -> iodata().
escape(IoData) -> re:replace(IoData, "'", "''", [global]).

%%--------------------------------------------------------------------
%% @spec update_set_sql([{Col, Value}]) -> string()
%%       Col = atom()
%%       Value = number() | atom() | string()
%% @doc 
%%    Creates update set stmt.
%%    Currently supports integer, double/float and strings.
%%    For strings \" replaced with '.
%% @end
%%--------------------------------------------------------------------
-spec update_set_sql(any()) -> string().
update_set_sql(Data) ->
  ColValueToSqlFun =
	fun({Col, Value}) ->
		[atom_to_list(Col), " = ", value_to_sql(Value)]
    end,
  map_intersperse(ColValueToSqlFun, Data, ", ").

%%--------------------------------------------------------------------
%% @spec read_cols_sql(Columns::[atom()]) -> string()
%% @doc
%%    Creates list of columns for select stmt.
%% @end
%%--------------------------------------------------------------------
-spec read_cols_sql([atom()]) -> string().
read_cols_sql(Columns) ->
  map_intersperse(fun atom_to_list/1, Columns, ", ").

%%--------------------------------------------------------------------
%% @spec create_table_sql(Tbl, [{ColName, Type}]) -> string()
%%       Tbl = atom()
%%       ColName = atom()
%%       Type = string()
%% @doc Generates a table create stmt in SQL.
%%      First column listed is considered the primary key.
%% @end
%%--------------------------------------------------------------------
-spec create_table_sql(atom(), [{atom(), string()}]) -> string().
create_table_sql(Tbl, [{ColName, Type} | Tl]) ->
    CT = ["CREATE TABLE ", atom_to_list(Tbl), " "],
    Start = ["(", atom_to_list(ColName), " ", col_type_to_string(Type), " PRIMARY KEY, "],
    End = [map_intersperse(
	         fun({Name0, Type0}) ->
	             [atom_to_list(Name0), " ", col_type_to_string(Type0)]
	         end, Tl, ", "), ");"],
    [CT, Start, End].

%%--------------------------------------------------------------------
%% @spec update_sql(Tbl, Key, Value, Data) -> string()
%%        Tbl = atom()
%%        Key = atom()
%%        Value = atom()
%%        Data = [{ColName :: atom(), Value :: string() | integer() | float()}]
%% @doc 
%%    Using Key as the column name and Data as list of column names 
%%    and values pairs it creates the proper update SQL stmt for the 
%%    record with matching Value.
%% @end
%%--------------------------------------------------------------------
-spec update_sql(atom(), atom(), atom(), [{atom(), sql_value()}]) -> string().
update_sql(Tbl, Key, Value, Data) ->
    ["UPDATE ", atom_to_list(Tbl), " SET ", update_set_sql(Data), 
	 " WHERE ", atom_to_list(Key), " = ", value_to_sql(Value), ";"].

%%--------------------------------------------------------------------
%% @spec write_sql(Tbl, Data) -> string()
%%       Tbl = atom()
%%       Data = [{ColName :: atom(), Values :: string() | integer() | float()}]
%% @doc Taking Data as list of column names and values pairs it creates the
%%      proper insertion SQL stmt.
%% @end
%%--------------------------------------------------------------------
-spec write_sql(atom(), [{atom(), sql_value()}]) -> string().
write_sql(Tbl, Data) ->
    {Cols, Values} = lists:unzip(Data),
    ["INSERT INTO ", atom_to_list(Tbl), " (", sqlite3_lib:write_col_sql(Cols), 
	 ") values (", sqlite3_lib:write_value_sql(Values), ");"].

%%--------------------------------------------------------------------
%% @spec read_sql(Tbl, Key, Value) -> string()
%%       Tbl = atom()
%%       Key = atom()
%%       Value = string() | integer() | float()
%% @doc Using Key as the column name searches for the record with
%%      matching Value.
%% @end
%%--------------------------------------------------------------------
-spec read_sql(atom(), atom(), sql_value()) -> string().
read_sql(Tbl, Key, Value) ->
    ["SELECT * FROM ", atom_to_list(Tbl), " WHERE ", atom_to_list(Key), 
	 " = ", value_to_sql(Value), ";"].

%%--------------------------------------------------------------------
%% @spec read_sql(Tbl, Key, Value, Columns) -> string()
%%        Tbl = atom()
%%        Key = atom()
%%        Value = string() | integer() | float()
%%        Columns = [atom()]
%% @doc
%%    Using Key as the column name searhces for the record with
%%    matching Value and returns only specified columns Columns.
%% @end
%%--------------------------------------------------------------------
-spec read_sql(atom(), atom(), sql_value(), [atom()]) -> string().
read_sql(Tbl, Key, Value, Columns) ->
    ["SELECT ", sqlite3_lib:read_cols_sql(Columns), " FROM ",
	 atom_to_list(Tbl), " WHERE ", atom_to_list(Key), " = ", 
	 value_to_sql(Value), ";"].

%%--------------------------------------------------------------------
%% @spec delete_sql(Tbl, Key, Value) -> string()
%%       Tbl = atom()
%%       Key = atom()
%%       Value = string() | integer() | float()
%% @doc Using Key as the column name searches for the record with
%%      matching Value then deletes that record.
%% @end
%%--------------------------------------------------------------------
-spec delete_sql(atom(), atom(), sql_value()) -> string().
delete_sql(Tbl, Key, Value) ->
    ["DELETE FROM ", atom_to_list(Tbl), " WHERE ", atom_to_list(Key), 
	 " = ", value_to_sql(Value), ";"].

%%--------------------------------------------------------------------
%% @spec drop_table(Tbl) -> string()
%%       Tbl = atom()
%% @doc Drop the table Tbl from the database
%% @end
%%--------------------------------------------------------------------
-spec drop_table(atom()) -> string().
drop_table(Tbl) ->
    ["DROP TABLE ", atom_to_list(Tbl), ";"].

%%====================================================================
%% Internal functions
%%====================================================================

%% @doc Works like string:join for iolists

-spec map_intersperse(fun((X) -> Z), [X], Y) -> [Z | Y].
map_intersperse(_Fun, [], _Sep) -> [];
map_intersperse(Fun, [Elem], _Sep) -> [Fun(Elem)];
map_intersperse(Fun, [Head | Tail], Sep) -> [Fun(Head), Sep | map_intersperse(Fun, Tail, Sep)].
