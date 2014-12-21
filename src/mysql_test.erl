-module(mysql_test).

-export([run_all/0]).

-include("dbapi.hrl").

-define(TESTS,
        [fun ping_server/0,
         fun insert_select/0,
         fun null_bitmap/0]).

%% ===================================================================
%% Run tests
%% ===================================================================

run_all() ->
    lists:foreach(fun run_test/1, ?TESTS).

run_test(Test) -> Test().

%% ===================================================================
%% Ping server
%% ===================================================================

ping_server() ->
    io:format("ping_server: "),

    {ok, Db} = mysql:connect([]),
    ok = mysql:ping(Db),
    ok = mysql:close(Db),

    io:format("OK~n").

%% ===================================================================
%% Insert select
%% ===================================================================

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

%% ===================================================================
%% Null bitmap
%%
%% See http://dev.mysql.com/doc/internals/en/null-bitmap.html
%% ===================================================================

null_bitmap() ->
    io:format("null_bitmap: "),

    NB = fun(Offset, Vals) -> mysql_protocol:null_bitmap(Offset, Vals) end,

    %% For some inexplicable reason, the prepared statement binary protocol
    %% uses a so called NULL bitmap to represent null field values. This is
    %% used in addition to a NULL value type - the two need to be in sync. The
    %% argument is that "If many NULL values are sent, it is more efficient
    %% than the old way". Apparently the handful of bits saved in these edge
    %% cases is worth the CPU cycles needed to encode and decode them. Oh,
    %% except for the use of the NULL type _byte_ - wait, no, that's _two_
    %% bytes as the value type includes an extra byte (yes, eight more precious
    %% bits) to flag an unsigned value -- both of which are _required in all
    %% cases, even when a value is NULL_.
    %%
    %% If this wasn't the actual behavior that we have to accomodate, it'd be a
    %% sick, twisted, hilarious troll.
    %%
    %% So... these tests illustrate what's required of us. Getting our big boy
    %% pants on...

    %% Each null value gets a bit in its respective field location:

    <<2#00000001>> = NB(0, [null]),
    <<2#00000011>> = NB(0, [null, null]),
    <<2#00000101>> = NB(0, [null, 0, null]),
    <<2#00000101>> = NB(0, [null, 0, null]),
    <<2#00011010>> = NB(0, [0, null, 0, null, null]),
    <<2#10000000>> = NB(0, [0, 0, 0, 0, 0, 0, 0, null]),

    %% We add another little byte as needed to accommodate more fields.

    <<2#00000000, 2#00000001>> = NB(0, [0, 0, 0, 0, 0, 0, 0, 0,
                                        null]),
    <<2#00000001, 2#00000001>> = NB(0, [null, 0, 0, 0, 0, 0, 0, 0,
                                        null]),
    <<2#11100000, 2#00000111>> = NB(0, [0, 0, 0, 0, 0, null, null, null,
                                        null, null, null]),
    <<2#01010101, 2#01010101>> = NB(0, [null, 0, null, 0, null, 0, null, 0,
                                        null, 0, null, 0, null, 0, null, 0]),

    %% Offset shifts the bits to the left

    <<2#00000010>> = NB(1, [null]),
    <<2#00000110>> = NB(1, [null, null]),
    <<2#00001010>> = NB(1, [null, 0, null]),
    <<2#00001010>> = NB(1, [null, 0, null]),
    <<2#00110100>> = NB(1, [0, null, 0, null, null]),
    <<2#00000000, 2#00000001>> = NB(1, [0, 0, 0, 0, 0, 0, 0, null]),

    io:format("ok~n").
