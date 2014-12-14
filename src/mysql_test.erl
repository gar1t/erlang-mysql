-module(mysql_test).

-export([run_all/0]).

-include("dbapi.hrl").

-define(TESTS,
        [fun ping_server/0]).

%% ===================================================================
%% Run tests
%% ===================================================================

run_all() ->
    lists:foreach(fun run_test/1, ?TESTS).

run_test(Test) -> Test().

%% ===================================================================
%% Tests
%% ===================================================================

ping_server() ->
    io:format("ping_server: "),

    {ok, Db} = mysql:connect([]),
    ok = mysql:ping(Db),
    ok = mysql:close(Db),

    io:format("OK~n").
