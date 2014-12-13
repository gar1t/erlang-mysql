-module(mysql).

-include_lib("emysql/include/emysql.hrl").

-include("dbapi.hrl").

%% App API
-export([start/0,
         get_cfg/1]).

%% DB API
-include("dbapi_specs.hrl").
-export([connect/1,
         execute/2,
         close/1,
         commit/1,
         rollback/1]).

%% ===================================================================
%% App start
%% ===================================================================

start() ->
    ensure_app_started(crypto),
    ensure_app_started(emysql),
    ensure_app_started(mysql).

ensure_app_started(App) ->
    case application:start(App) of
        ok -> ok;
        {error, {alread_started, App}} -> ok;
        Other -> Other
    end. 

%% ===================================================================
%% Config
%% ===================================================================

%% TODO: Hard coded values for now - implement app config
get_cfg(max_allowed_packet) -> 100 * 1024 * 1024;
get_cfg(default_character_set) -> 8. %% TODO try 0

%% ===================================================================
%% Connect
%% ===================================================================

connect(Options) ->
    Ref = make_pool_id(),
    AddPoolOptions = add_pool_options(Options),
    AddPoolResult = emysql:add_pool(Ref, AddPoolOptions),
    connect_result(AddPoolResult, Ref).

make_pool_id() ->
    list_to_atom("mysql_db_" ++ integer_to_list(rand_id())).

rand_id() -> erlang:phash2(os:timestamp()).

add_pool_options(Options) -> Options.

connect_result(ok, Ref) ->
    {ok, Ref};
connect_result({failed_to_connect_to_database, Reason}, _Ref) ->
    {error, Reason}.

%% ===================================================================
%% Execute
%% ===================================================================

execute(Db, Stmt) ->
    Result = emysql:execute(Db, Stmt),
    execute_result(Result).

execute_result(#error_packet{}=Err) -> {error, error_reason(Err)};
execute_result(Result) -> Result.

%% ===================================================================
%% Close
%% ===================================================================

close(Db) ->
    emysql:remove_pool(Db).

%% ===================================================================
%% Commit
%% ===================================================================

commit(_Db) -> ok.

%% ===================================================================
%% Rollback
%% ===================================================================

rollback(_Db) -> {error, not_implemented}.

%% ===================================================================
%% Shared / util
%% ===================================================================

error_reason(#error_packet{code=Code, status=Status, msg=Msg}) ->
    #dberr{
       sqlstate=sqlstate_from_status(Status),
       native=Code,
       msg=ensure_binary(Msg)}.

sqlstate_from_status(Status) -> binary_to_list(Status).

ensure_binary(B) when is_binary(B) -> B;
ensure_binary(L) when is_list(L) -> list_to_binary(L).
