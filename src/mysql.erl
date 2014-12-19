%% mysql - Interface to the Erlang MySQL library
%%
%% This module implements the DB API interface (see dbapi.hrl and
%% dbapi_specs.hrl).
%%
-module(mysql).

-include("dbapi.hrl").
-include("dbapi_specs.hrl").
-include("mysql_internal.hrl").

%% App API
-export([start/0,
         get_cfg/1]).

%% DB API
-export([connect/1,
         execute/2,
         describe/1,
         describe/2,
         rows/1,
         first/1,
         next/1,
         last/1,
         prev/1,
         close/1,
         prepare/2,
         free/1]).

%% Extra
-export([ping/1]).

%% ===================================================================
%% App start
%% ===================================================================

start() ->
    application:start(mysql).

%% ===================================================================
%% Config
%% ===================================================================

%% TODO: Hard coded values for now - implement app config
get_cfg(max_allowed_packet) -> 100 * 1024 * 1024;
get_cfg(default_character_set) -> 0. %% TODO try 0

%% ===================================================================
%% DB API
%% ===================================================================

connect(_Options) ->
    mysql_lib:connect([]).

execute(Db, Stmt) ->
    dbapi_result(mysql_lib:query(Db, iolist_to_binary(Stmt))).

describe(#resultset{}=RS) ->
    describe_resultset(RS);
describe(#ok_packet{}=OK) ->
    describe_ok(OK);
describe(#err_packet{}=Err) ->
    describe_err(Err).

describe(#resultset{rows=Rows}, rowcount) ->
    length(Rows);
describe(#ok_packet{affected_rows=Rows}, rowcount) ->
    Rows;
describe(#resultset{columns=Cols}, column_names) ->
    [C#coldef.name || C <- Cols];
describe(Term, Name) ->
    %% TODO: continue to optimize here - this is a catchall
    proplists:get_value(Name, describe(Term)).

rows(#resultset{rows=Rows}) -> Rows;
rows(#ok_packet{}) -> [].

next(#resultset{rows=[Row|Rest]}=RS) ->
    {Row, RS#resultset{rows=Rest}};
next(#resultset{rows=[]}) -> eof;
next(#ok_packet{}) -> eof.

first(RS) -> next(RS).

last(_) -> error(not_implemented).

prev(_) -> error(not_implemented).

prepare(Db, Stmt) ->
    dbapi_result(mysql_lib:prepare_statement(Db, iolist_to_binary(Stmt))).

close(Db) ->
    mysql_lib:close(Db).

free(Stmt) ->
    mysql_lib:close_statement(Stmt).

%% ===================================================================
%% MySQL -> DB API
%% ===================================================================

dbapi_result(#ok_packet{}=OK) ->
    {ok, OK};
dbapi_result(#resultset{}=RS) ->
    {ok, RS};
dbapi_result(#err_packet{sqlstate=SqlState}=Err) ->
    {error, {SqlState, Err}};
dbapi_result(#prepared_stmt{}=Stmt) ->
    {ok, Stmt}.

%% ===================================================================
%% Descriptions
%% ===================================================================

describe_resultset(#resultset{columns=Cols, rows=Rows}) ->
    [{rowcount, length(Rows)},
     {columns, [describe_col(Col) || Col <- Cols]}].

describe_col(
  #coldef{
     catalog=Catalog,
     schema=Schema,
     table=Table,
     org_table=OrgTable,
     name=Name,
     org_name=OrgName,
     character_set=CharSet,
     column_length=Len,
     type=Type,
     flags=Flags,
     decimals=Decimals,
     default_values=Default}) ->
    [{catalog, Catalog},
     {schema, Schema},
     {table, Table},
     {org_table, OrgTable},
     {name, Name},
     {org_name, OrgName},
     {character_set, CharSet},
     {column_length, Len},
     {type, dbapi_type(Type)},
     {flags, Flags},
     {decimals, Decimals},
     {default, Default}].

dbapi_type(16#00) -> decimal;
dbapi_type(16#01) -> tiny;
dbapi_type(16#02) -> short;
dbapi_type(16#03) -> long;
dbapi_type(16#04) -> float;
dbapi_type(16#05) -> double;
dbapi_type(16#06) -> null;
dbapi_type(16#07) -> timestamp;
dbapi_type(16#08) -> longlong;
dbapi_type(16#09) -> int24;
dbapi_type(16#0a) -> date;
dbapi_type(16#0b) -> time;
dbapi_type(16#0c) -> datetime;
dbapi_type(16#0d) -> year;
dbapi_type(16#0e) -> newdate;
dbapi_type(16#0f) -> varchar;
dbapi_type(16#10) -> bit;
dbapi_type(16#11) -> timestamp2;
dbapi_type(16#12) -> datetime2;
dbapi_type(16#13) -> time2;
dbapi_type(16#f6) -> newdecimal;
dbapi_type(16#f7) -> enum;
dbapi_type(16#f8) -> set;
dbapi_type(16#f9) -> tiny_blob;
dbapi_type(16#fa) -> medium_blob;
dbapi_type(16#fb) -> long_blob;
dbapi_type(16#fc) -> blob;
dbapi_type(16#fd) -> var_string;
dbapi_type(16#fe) -> string;
dbapi_type(16#ff) -> geometry.

describe_ok(
  #ok_packet{
     affected_rows=AffectedRows,
     status_flags=Status,
     warnings=Warnings,
     info=Info}) ->
    [{rowcount, AffectedRows},
     {status, Status},
     {warnings, Warnings},
     {info, Info}].

describe_err(#err_packet{sqlstate=SqlState, code=Code, msg=Msg}) ->
    [{sqlstate, SqlState},
     {native, Code},
     {msg, Msg}].

%% ===================================================================
%% Extra
%% ===================================================================

ping(Db) ->
    #ok_packet{} = mysql_lib:ping(Db),
    ok.
