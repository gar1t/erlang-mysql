-module(mysql_test).

-export([run_all/0]).

-include("dbapi.hrl").

-define(TESTS,
        [fun ping_server/0,
         fun insert_select/0]).

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

insert_select() ->
    io:format("insert_select: "),

    {ok, Db} = mysql:connect([]),

    %% Create a table we can insert into
    {ok, _} = mysql:execute(Db, "use test"),
    {ok, _} = mysql:execute(Db, "drop table if exists __t"),
    {ok, _} = mysql:execute(Db, "create table __t (i int)"),

    %% Insert returns a result that tells us what happened
    {ok, I1} = mysql:execute(Db, "insert into __t values (1)"),
    1 = mysql:describe(I1, rowcount),
    0 = mysql:describe(I1, warnings),
    2 = mysql:describe(I1, status),
    <<>> = mysql:describe(I1, info),

    %% Insert some more values
    {ok, _} = mysql:execute(Db, "insert into __t values (2)"),
    {ok, _} = mysql:execute(Db, "insert into __t values (3)"),

    %% Select returns a resultset that contains the rows
    {ok, RS} = mysql:execute(Db, "select * from __t order by i"),
    [{<<"1">>},
     {<<"2">>},
     {<<"3">>}] = mysql:rows(RS),

    %% Note at this time the driver does *not* convert values to their
    %% native Erlang type - these are returned by the MySQL as strings
    %% and are preserved as such.

    %% We can get information about the result set.
    [<<"i">>] = mysql:describe(RS, column_names),

    ok = mysql:close(Db),

    io:format("OK~n").
