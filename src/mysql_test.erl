-module(mysql_test).

-export([run_all/0]).

-include("dbapi.hrl").

%% ===================================================================
%% Run tests
%% ===================================================================

run_all() ->
    Tests =
        [fun simple_select/0,
         fun syntax_error/0],
    lists:foreach(fun run_test/1, Tests).

run_test(Test) ->
    handle_result(catch(Test())).

handle_result(ok) -> ok;
handle_result({'EXIT', Err}) -> print_error(Err); 
handle_result(Err) -> print_error(Err).

print_error(Err) ->
    io:format("ERROR~n"),
    io:format(" ~p~n", [Err]).

%% ===================================================================
%% Tests
%% ===================================================================

simple_select() ->
    io:format("simple_select: "),

    {ok, Db} = mysql:connect([{database, "test"}]),

    xxx = mysql:execute(Db, "create table if not exists t (i int) "
                            "engine = memory"),

    io:format("OK~n").

syntax_error() ->
    io:format("syntax_error: "),
    {ok, Db} = mysql:connect([]),

    {error, #dberr{sqlstate="42000",
                   native=1064,
                   msg= <<"You have an error in "
                          "your SQL syntax;", _/binary>>}} =
        mysql:execute(Db, "show database"),

    ok = mysql:close(Db),
    io:format("OK~n").
