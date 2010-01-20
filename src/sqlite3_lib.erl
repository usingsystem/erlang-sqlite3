%%%-------------------------------------------------------------------
%%% File    : sqlite3_lib.erl
%%% @author Tee Teoh
%%% @copyright 21 Jun 2008 by Tee Teoh 
%%% @version 1.0.0
%%% @doc Library module for sqlite3
%%% @end
%%%-------------------------------------------------------------------
-module(sqlite3_lib).

%% API
-export([col_type/1]).
-export([write_value_sql/1, write_col_sql/1]).
-export([create_table_sql/2, write_sql/2, read_sql/3, delete_sql/3, drop_table/1]). 
-export ([update_sql/4, update_set_sql/1, replace/1]).
-export ([read_sql/4, read_cols_sql/1]).

%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% @spec col_type(Type :: term()) -> term()
%% @doc Maps sqlite3 column type.
%% @end
%%--------------------------------------------------------------------
-spec(col_type/1::(atom() | string()) -> atom() | string()).
col_type(integer) ->
    "INTEGER";     
col_type("INTEGER") ->
    integer;
col_type(text) ->
    "TEXT";
col_type("TEXT") ->
    text;
col_type(double) ->
    "DOUBLE";
col_type("DOUBLE") ->
    double;
col_type(date) ->
    "DATE";
col_type("DATE") ->
    date.

%%--------------------------------------------------------------------
%% @spec write_value_sql(Value :: [term()]) -> string()
%% @doc 
%%    Creates the values portion of the sql stmt.
%%    Currently only support integer, double/float and strings.
%% @end
%%--------------------------------------------------------------------
-spec(write_value_sql/1::(any()) -> string()).
write_value_sql(Values) ->
    StrValues = lists:map(fun(X) when is_integer(X) ->
				  integer_to_list(X);
			     (X) when is_float(X) ->
				  float_to_list(X);
			     (X) ->
				  io_lib:format("'~s'", [X])
			  end, Values),
    string:join(StrValues, ",").

%%--------------------------------------------------------------------
%% @spec write_col_sql([atom()]) -> string()
%% @doc Creates the column/data stmt for SQL.
%% @end
%%--------------------------------------------------------------------
-spec(write_col_sql/1::([atom()]) -> string()).
write_col_sql(Cols) ->
    StrCols = lists:map(fun(X) ->
				atom_to_list(X)
			end, Cols),
    string:join(StrCols, ",").

%%--------------------------------------------------------------------
%% @spec replace (String) -> string ()
%%        String = string ()
%% @doc Returns copy of String for which \" replaced with '
%% @end
%%--------------------------------------------------------------------
-spec (replace/1::(string ()) -> string ()).
replace ([34 | T]) ->
  replace (T, ["'"]);
replace ([H | T]) ->
  replace (T, [binary_to_list (<<H>>)]).
replace ([], L) ->
  lists:flatten (lists:reverse (L));
replace ([34 | T], L) ->
  replace (T, ["'" | L]);
replace ([H | T], L) ->
  replace (T, [binary_to_list (<<H>>) | L]).

%%--------------------------------------------------------------------
%% @spec update_set_sql ([{Col, Value}]) -> string ()
%%       Col = atom ()
%%       Value = integer () | float () | string ()
%% @doc 
%%    Creates update set stmt.
%%    Currently only supports integer, double/float and strings.
%%    For strings \" replaced with '.
%% @end
%%--------------------------------------------------------------------
-spec (update_set_sql/1::(any ()) -> string ()).
update_set_sql (Data) ->
  Set = lists:map (fun 
      ({Col, Value}) when is_integer (Value) -> 
        string:join (
          [atom_to_list (Col), integer_to_list (Value)], " = ");
      ({Col, Value}) when is_float (Value) -> 
        string:join (
          [atom_to_list (Col), integer_to_list (Value)], " = ");
      ({Col, Value}) -> 
        string:join (
          [atom_to_list (Col), 
            io_lib:format ("\"~s\"", [replace (Value)])], " = ") 
    end,
    Data),
  string:join (Set, ", ").

%%--------------------------------------------------------------------
%% @spec read_cols_sql (Columns::[atom ()]) -> string ()
%% @doc
%%    Creates list of columns for select stmt.
%% @end
%%--------------------------------------------------------------------
-spec (read_cols_sql/1::([atom ()]) -> string ()).
read_cols_sql (Columns) ->
  string:join (
    lists:map (fun (A) -> atom_to_list (A) end, Columns), ", ").

%%--------------------------------------------------------------------
%% @spec create_table_sql(Tbl, [{ColName, Type}]) -> string()
%%       Tbl = atom()
%%       ColName = atom()
%%       Type = string()
%% @doc Generates a table create stmt in SQL.
%% @end
%%--------------------------------------------------------------------
-spec(create_table_sql/2::(atom(), [{atom(), string()}]) -> string()).
create_table_sql(Tbl, [{ColName, Type} | Tl]) ->
    CT = io_lib:format("CREATE TABLE ~p ", [Tbl]),
    Start = io_lib:format("(~p ~s PRIMARY KEY, ", [ColName, sqlite3_lib:col_type(Type)]),
    End = string:join(
	    lists:map(fun({Name0, Type0}) ->
			      io_lib:format("~p ~s", [Name0, sqlite3_lib:col_type(Type0)])
		      end, Tl), ", ") ++ ");",
    lists:flatten(CT ++ Start ++ End).

%%--------------------------------------------------------------------
%% @spec update_sql (Tbl, Key, Value, Data) -> string ()
%%        Tbl = atom ()
%%        Key = atom ()
%%        Value = atom ()
%%        Data = [{ColName :: atom (), Value :: string () | integer () | float ()}]
%% @doc 
%%    Using Key as the column name and Data as list of column names 
%%    and values pairs it creates the proper update SQL stmt for the 
%%    record with matching Value.
%% @end
%%--------------------------------------------------------------------
-type(sql_value() :: string() | integer() | float()).
-spec (update_sql/4::(atom (), atom (), atom (), [{atom (), sql_value ()}]) -> string ()).
update_sql (Tbl, Key, Value, Data) ->
    lists:flatten (
      io_lib:format ("UPDATE ~p SET ~s WHERE ~p = ~p;", 
        [Tbl,
         sqlite3_lib:update_set_sql (Data),
         Key,
         Value
       ])).

%%--------------------------------------------------------------------
%% @spec write_sql(Tbl, Data) -> string()
%%       Tbl = atom()
%%       Data = [{ColName :: atom(), Values :: string() | integer() | float()}]
%% @doc Taking Data as list of column names and values pairs it creates the
%%      proper insertion SQL stmt.
%% @end
%%--------------------------------------------------------------------
-spec(write_sql/2::(atom(), [{atom(), sql_value()}]) -> string()).
write_sql(Tbl, Data) ->
    {Cols, Values} = lists:unzip(Data),
    lists:flatten(
      io_lib:format("INSERT INTO ~p (~s) values (~s);", 
		    [Tbl, 
		     sqlite3_lib:write_col_sql(Cols), 
		     sqlite3_lib:write_value_sql(Values)])).

%%--------------------------------------------------------------------
%% @spec read_sql(Tbl, Key, Value) -> string()
%%       Tbl = atom()
%%       Key = atom()
%%       Value = string() | integer() | float()
%% @doc Using Key as the column name searches for the record with
%%      matching Value.
%% @end
%%--------------------------------------------------------------------
-spec(read_sql/3::(atom(), atom(), sql_value()) -> string()).
read_sql(Tbl, Key, Value) ->
    lists:flatten(
      io_lib:format("SELECT * FROM ~p WHERE ~p = ~p;", [Tbl, Key, Value])).

%%--------------------------------------------------------------------
%% @spec read_sql (Tbl, Key, Value, Columns) -> string ()
%%        Tbl = atom ()
%%        Key = atom ()
%%        Value = string () | integer () | float ()
%%        Columns = [atom ()]
%% @doc
%%    Using Key as the column name searhces for the record with
%%    matching Value and returns only specified columns Columns.
%% @end
%%--------------------------------------------------------------------
-spec (read_sql/4::(atom (), atom (), sql_value (), [atom ()]) -> string ()).
read_sql (Tbl, Key, Value, Columns) ->
  lists:flatten (
    io_lib:format ("SELECT ~p FROM ~p WHERE ~p = ~p;",
      [sqlite3_lib:read_cols_sql (Columns),
       Tbl,
       Key,
       Value
     ])).

%%--------------------------------------------------------------------
%% @spec delete_sql(Tbl, Key, Value) -> string()
%%       Tbl = atom()
%%       Key = atom()
%%       Value = string() | integer() | float()
%% @doc Using Key as the column name searches for the record with
%%      matching Value then deletes that record.
%% @end
%%--------------------------------------------------------------------
-spec(delete_sql/3::(atom(), atom(), sql_value()) -> string()).
delete_sql(Tbl, Key, Value) ->
    lists:flatten(
      io_lib:format("DELETE FROM ~p WHERE ~p = ~p;", [Tbl, Key, Value])).

%%--------------------------------------------------------------------
%% @spec drop_table(Tbl) -> string()
%%       Tbl = atom()
%% @doc Drop the table Tbl from the database
%% @end
%%--------------------------------------------------------------------
-spec(drop_table/1::(atom()) -> string()).
drop_table(Tbl) ->
    lists:flatten(
      io_lib:format("DROP TABLE ~p;", [Tbl])).

%%====================================================================
%% Internal functions
%%====================================================================
