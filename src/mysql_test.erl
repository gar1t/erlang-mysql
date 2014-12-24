-module(mysql_test).

-export([run_all/0]).

-include("dbapi.hrl").

-define(TESTS,
        [fun ping_server/0,
         fun insert_select/0,
         fun null_bitmap/0,
         fun prepared_statements/0]).

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
    {ok, _} = mysql:execute(Db, "create table __t (i int, s varchar(100))"),

    %% Insert a row into our table
    {ok, R1} = mysql:execute(Db, "insert into __t values (1, 'Dog')"),

    %% The result of an insert gives us some information
    [{rowcount, 1},
     {status, 2},
     {warnings, 0},
     {info, <<>>}] = mysql:describe(R1),

    %% We can get specific attributes directly using describe/2
    1 = mysql:describe(R1, rowcount),
    0 = mysql:describe(R1, warnings),
    2 = mysql:describe(R1, status),
    <<>> = mysql:describe(R1, info),

    %% Insert some more values
    {ok, _} = mysql:execute(Db, "insert into __t values (2, 'Cat')"),
    {ok, _} = mysql:execute(Db, "insert into __t values (3, 'Lemur')"),

    %% Select returns a resultset that contains the rows
    {ok, R2} = mysql:execute(Db, "select * from __t order by i"),
    [{<<"1">>, <<"Dog">>},
     {<<"2">>, <<"Cat">>},
     {<<"3">>, <<"Lemur">>}] = mysql:rows(R2),

    %% Note at this time the driver does *not* convert values to their
    %% native Erlang type - these are returned by the MySQL as strings
    %% and are preserved as such.

    %% Like the insert result, we can get info about a select result
    [{rowcount, 3}, {columns, Cols}] = mysql:describe(R2),

    %% The columns attribute contains information about each of the
    %% collumns returned in the resulset
    2 = length(Cols),
    [long, var_string] = map_attr(type, Cols),
    [<<"i">>, <<"s">>] = map_attr(name, Cols),

    %% There are shortcuts for getting some of the attributes directly
    3 = mysql:describe(R2, rowcount),
    [long, var_string] = mysql:describe(R2, column_types),
    [<<"i">>, <<"s">>] = mysql:describe(R2, column_names),

    ok = mysql:close(Db),

    io:format("OK~n").

map_attr(Name, List) ->
    [proplists:get_value(Name, Item) || Item <- List].

%% ===================================================================
%% Null bitmap
%%
%% See http://dev.mysql.com/doc/internals/en/null-bitmap.html
%% ===================================================================

null_bitmap() ->
    io:format("null_bitmap: "),

    Enc = fun mysql_protocol:encode_null_bitmap/2,

    %% Each null value gets a bit in its respective field location:

    <<2#00000001>> = Enc(0, [null]),
    <<2#00000011>> = Enc(0, [null, null]),
    <<2#00000101>> = Enc(0, [null, 0, null]),
    <<2#00000101>> = Enc(0, [null, 0, null]),
    <<2#00011010>> = Enc(0, [0, null, 0, null, null]),
    <<2#10000000>> = Enc(0, [0, 0, 0, 0, 0, 0, 0, null]),

    %% We add another little byte as needed to accommodate more fields.

    <<2#00000000, 2#00000001>> = Enc(0, [0, 0, 0, 0, 0, 0, 0, 0,
                                         null]),
    <<2#00000001, 2#00000001>> = Enc(0, [null, 0, 0, 0, 0, 0, 0, 0,
                                         null]),
    <<2#11100000, 2#00000111>> = Enc(0, [0, 0, 0, 0, 0, null, null, null,
                                         null, null, null]),
    <<2#01010101, 2#01010101>> = Enc(0, [null, 0, null, 0, null, 0, null, 0,
                                         null, 0, null, 0, null, 0, null, 0]),

    %% Offset shifts the bits to the left

    <<2#00000010>> = Enc(1, [null]),
    <<2#00000110>> = Enc(1, [null, null]),
    <<2#00001010>> = Enc(1, [null, 0, null]),
    <<2#00001010>> = Enc(1, [null, 0, null]),
    <<2#00110100>> = Enc(1, [0, null, 0, null, null]),
    <<2#00000000, 2#00000001>> = Enc(1, [0, 0, 0, 0, 0, 0, 0, null]),

    %% When reading a bitmap provided by the MySQL server, we use
    %% is_field_null to test for a null bit for a field (0 based).

    Null = fun mysql_protocol:is_field_null/3,
    Bitmap = <<2#01010101, 2#00000011>>,

    true  = Null(0, 0, Bitmap),
    false = Null(1, 0, Bitmap),
    true  = Null(2, 0, Bitmap),
    false = Null(3, 0, Bitmap),
    true  = Null(4, 0, Bitmap),
    false = Null(5, 0, Bitmap),
    true  = Null(6, 0, Bitmap),
    false = Null(7, 0, Bitmap),
    true  = Null(8, 0, Bitmap),
    true  = Null(9, 0, Bitmap),
    false = Null(10, 0, Bitmap),
    false = Null(11, 0, Bitmap),

    io:format("OK~n").

%% ===================================================================
%% Prepare statement
%% ===================================================================

prepared_statements() ->
    io:format("prepared_statements: "),

    {ok, Db} = mysql:connect([]),

    %% Create a table we can insert into
    {ok, _} = mysql:execute(Db, "use test"),
    {ok, _} = mysql:execute(Db, "drop table if exists __t"),
    CreateTableSQL =
        "create table __t ("
        " i int,"
        " f double,"
        " s varchar(100),"
        " t timestamp,"
        " b blob)",
    {ok, _} = mysql:execute(Db, CreateTableSQL),

    %% Create a prepared statement that inserts a row
    {ok, Insert} = mysql:prepare(Db, "insert into __t values (?, ?, ?, ?, ?)"),

    %% Execute the statement by passing along native parameter values
    Now = calendar:local_time(),
    P1 = [1, 1.12345, "Hello", Now, <<1,2,3,4>>],
    P2 = [2, null, "Goodbye", Now, null],

    {ok, _} = mysql:execute(Db, Insert, P1),
    {ok, _} = mysql:execute(Db, Insert, P2),

    %% Create a select statement
    {ok, Select} = mysql:prepare(Db, "select * from __t where i > ?"),

    %% Use select in the same way
    {ok, R1} = mysql:execute(Db, Select, [0]),
    [{1, 1.12345, <<"Hello">>, Now, <<1,2,3,4>>},
     {2, null, <<"Goodbye">>, Now, null}] = mysql:rows(R1),

    ok = mysql:close(Db),

    io:format("OK~n").
