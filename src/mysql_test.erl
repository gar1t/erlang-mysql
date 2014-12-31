-module(mysql_test).

-export([run_all/0, run_all/1, run_all_cli/0]).

-compile(export_all). % To run individual tests

-include("dbapi.hrl").

-define(DEFAULT_HOST, "localhost").
-define(DEFAULT_PORT, 3306).
-define(DEFAULT_USER, "").
-define(DEFAULT_PASSWORD, "").
-define(DEFAULT_DATABASE, "test").

-define(TESTS,
        [fun ping_server/1,
         fun init_db/1,
         fun insert_select/1,
         fun null_bitmap/1,
         fun prepared_statements/1,
         fun error_interface/1,
         fun signed_ints/1]).

%% ===================================================================
%% Run tests
%% ===================================================================

run_all() ->
    run_all(
      maybe_env_opts(
        [{"MYSQL_TEST_HOST", host},
         {"MYSQL_TEST_PORT", port},
         {"MYSQL_TEST_USER", user},
         {"MYSQL_TEST_PASSWORD", password},
         {"MYSQL_TEST_DATABASE", database}])).

run_all(Opts) ->
    lists:foreach(fun(Test) -> Test(Opts) end, ?TESTS).

run_all_cli() ->
    try
        run_all()
    catch
        _:Err -> print_error_and_exit(Err)
    end.

print_error_and_exit(Err) ->
    io:format(standard_error, "ERROR ~p~n", [Err]),
    io:format(standard_error, "~p~n", [erlang:get_stacktrace()]),
    erlang:halt(1).

%% ===================================================================
%% Helpers
%% ===================================================================

maybe_env_opts(Names) ->
    maybe_env_opts(Names, []).

maybe_env_opts([{Name, Opt}|Rest], Acc) ->
    maybe_env_opts(Rest, maybe_env_opt(Name, Opt, Acc));
maybe_env_opts([], Acc) -> Acc.

maybe_env_opt(Name, Opt, Opts) ->
    case os:getenv(Name) of
        false -> Opts;
        Value -> [{Opt, maybe_int_opt(Opt, Value)}|Opts]
    end.

maybe_int_opt(port, Value) -> list_to_integer(Value);
maybe_int_opt(_, Value) -> Value.

connect_opts(Opts) ->
    Opt = fun(Name, Default) -> mysql_util:option(Name, Opts, Default) end,
    [{host,     Opt(host, ?DEFAULT_HOST)},
     {port,     Opt(port, ?DEFAULT_PORT)},
     {user,     Opt(user, ?DEFAULT_USER)},
     {password, Opt(password, ?DEFAULT_PASSWORD)},
     {database, Opt(database, ?DEFAULT_DATABASE)}].

%% ===================================================================
%% Ping server
%% ===================================================================

ping_server(Opts) ->
    io:format("ping_server: "),

    {ok, Db} = mysql:connect(connect_opts(Opts)),
    ok = mysql:ping(Db),
    ok = mysql:close(Db),

    io:format("OK~n").

%% ===================================================================
%% Init db
%% ===================================================================

init_db(Opts) ->
    io:format("init_db: "),

    ConnectOpts = connect_opts(Opts),
    TestDb = mysql_util:option(database, ConnectOpts, undefined),
    true = TestDb /= null,
    true = TestDb /= undefined,

    {ok, Db} = mysql:connect(ConnectOpts),

    {ok, RS} = mysql:execute(Db, "select database()"),
    [{TestDbBin}] = mysql:rows(RS),
    TestDbBin = iolist_to_binary(TestDb),

    ok = mysql:close(Db),

    io:format("OK~n").

%% ===================================================================
%% Insert select
%% ===================================================================

insert_select(Opts) ->
    io:format("insert_select: "),

    {ok, Db} = mysql:connect(connect_opts(Opts)),

    %% Create a table we can insert into
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

    %% As a shortcut, we can use `select/2` to return the rows directly from a
    %% select statement.

    {ok, [{<<"2">>, <<"Cat">>},
          {<<"1">>, <<"Dog">>},
          {<<"3">>, <<"Lemur">>}]} =
        mysql:select(Db, "select * from __t order by s"),

    ok = mysql:close(Db),

    io:format("OK~n").

map_attr(Name, List) ->
    [proplists:get_value(Name, Item) || Item <- List].

%% ===================================================================
%% Null bitmap
%%
%% See http://dev.mysql.com/doc/internals/en/null-bitmap.html
%% ===================================================================

null_bitmap(_Opts) ->
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

prepared_statements(Opts) ->
    io:format("prepared_statements: "),

    {ok, Db} = mysql:connect(Opts),

    %% Create a table we can insert into
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

    %% As with `select/2` we can use `select/3` with a statement and params.

    {ok, [{2, null, <<"Goodbye">>, Now, null}]} = 
        mysql:select(Db, Select, [1]),

    ok = mysql:close(Db),

    io:format("OK~n").

%% ===================================================================
%% Error interface
%% ===================================================================

error_interface(Opts) ->
    io:format("error_interface: "),

    {ok, Db} = mysql:connect(Opts),

    %% All errors are designated by the standard two-tuple:

    {error, Reason} = mysql:execute(Db, "bad syntax"),

    %% An error reason is always a two tuple of a code (sqlstate) and an opaque
    %% term. Our statement contains a syntax error, so we get the 42000 code:

    {"42000", _Opaque} = Reason,

    %% We can use describe to get more information:

    [{sqlstate,"42000"},
     {native_code,1064},
     {msg, <<"You have an error in your SQL syntax", _/binary>>}]
        = mysql:describe(Reason),

    ok = mysql:close(Db),

    io:format("OK~n").

%% ===================================================================
%% Error interface
%% ===================================================================

signed_ints(Opts) ->
    io:format("signed_ints: "),

    {ok, Db} = mysql:connect(Opts),

    %% Create a table with both signed ans unsigned ints.

    {ok, _} = mysql:execute(Db, "drop table if exists __t"),
    {ok, _} = mysql:execute(Db, "create table __t ("
                                " i int signed, "
                                " u int unsigned)"),
    
    %% Use the text query interface to insert values along various ranges.

    Insert =
        fun(I, U) ->
                SQL = io_lib:format(
                        "insert into __t values (~b, ~b)",
                        [I, U]),
                mysql:execute(Db, SQL)
        end,

    {ok, _} = Insert(0, 0),                   % Zeros
    {ok, _} = Insert(-2147483648, 0),         % Min values
    {ok, _} = Insert(2147483647, 4294967295), % Max values

    %% Using the text query interface, we get what we expect.

    {ok, [{<<"0">>,           <<"0">>},
          {<<"-2147483648">>, <<"0">>},
          {<<"2147483647">>,  <<"4294967295">>}]} =
        mysql:select(Db, "select * from __t"),

    %% Using a prepared statement (binary protocol) to select:

    {ok, SelectStmt} = mysql:prepare(Db, "select * from __t"),

    %% We get the same results decoded properly as signed and unsigned ints.

    {ok, [{0,           0},
          {-2147483648, 0},
          {2147483647,  4294967295}]} =
        mysql:select(Db, SelectStmt, []),

    %% Let's write using the binary protocol.

    {ok, InsertStmt} = mysql:prepare(Db, "insert into __t values (?, ?)"),
    {ok, _} = mysql:execute(Db, "delete from __t"),
    {ok, _} = mysql:execute(Db, InsertStmt, [0, 0]),
    {ok, _} = mysql:execute(Db, InsertStmt, [-2147483648, 0]),
    {ok, _} = mysql:execute(Db, InsertStmt, [2147483647, 4294967295]),

    %% Reading using the text protocol we see our expected values as text.

    {ok, [{<<"0">>,           <<"0">>},
          {<<"-2147483648">>, <<"0">>},
          {<<"2147483647">>,  <<"4294967295">>}]} =
        mysql:select(Db, "select * from __t"),

    %% As well as using the binary protocol via the prepared statement.

    {ok, [{0,           0},
          {-2147483648, 0},
          {2147483647,  4294967295}]} =
        mysql:select(Db, SelectStmt, []),

    ok = mysql:close(Db),

    io:format("OK~n").
