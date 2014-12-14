%% mysql - Interface to the Erlang MySQL library
%%
%% This module implements the DB API interface (see dbapi.hrl and
%% dbapi_specs.hrl).
%%
-module(mysql).

-include_lib("emysql/include/emysql.hrl").

-include("dbapi.hrl").
-include("dbapi_specs.hrl").

%% App API
-export([start/0,
         get_cfg/1]).

%% DB API
-export([connect/1,
         execute/2,
         close/1,
         commit/1,
         rollback/1]).

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
get_cfg(default_character_set) -> 8. %% TODO try 0

%% ===================================================================
%% DB API
%% ===================================================================

connect(_Options) ->
    mysql_lib:connect([]).

execute(Db, Stmt) ->
    execute(Db, Stmt, []).

execute(_Db, _Stmt, _Options) ->
    yyy.

close(Db) ->
    mysql_lib:close(Db).

commit(_Db) -> ok.

rollback(_Db) -> {error, not_implemented}.

%% ===================================================================
%% Extra
%% ===================================================================

ping(Db) -> mysql_lib:ping(Db).
