%% mysql_lib - MySQL specific interface
%%
%% This library uses mysql_net, mysql_protocol, and mysql_auth to provide
%% functions that a higher level DB interface can use to interface with a MySQL
%% server.
%%
%% The interface should reflect the MySQL client/server protocol interface and
%% not any higher level Erlang DB spec (e.g. dbapi).
%%
-module(mysql_lib).

-export([connect/1,
         close/1,
         simple_command/3,
         query/2,
         ping/1]).

-include("mysql_internal.hrl").

-define(DEFAULT_HOST, "localhost").
-define(DEFAULT_PORT, 3306).
-define(DEFAULT_USER, <<>>).
-define(DEFAULT_PWD,  <<>>).
-define(DEFAULT_CONNECT_TIMEOUT, 15000).

%% ===================================================================
%% Connect
%% ===================================================================

connect(Options) ->
    Host = mysql_util:option(host, Options, ?DEFAULT_HOST),
    Port = mysql_util:option(port, Options, ?DEFAULT_PORT),
    User = mysql_util:option(user, Options, ?DEFAULT_USER),
    Pwd = mysql_util:option(password, Options, ?DEFAULT_PWD),
    Timeout = mysql_util:option(timeout, Options, ?DEFAULT_CONNECT_TIMEOUT),
    ConnectResult = mysql_net:connect(Host, Port, Timeout),
    try_authenticate(ConnectResult, User, Pwd).

try_authenticate({ok, Sock}, User, Pwd) ->
    handle_authenticate(mysql_auth:authenticate(Sock, User, Pwd), Sock);
try_authenticate({error, Err}, _User, _Pwd) ->
    {error, Err}.

handle_authenticate(#ok_packet{}, Sock) ->
    {ok, #mysql{sock=Sock}};
handle_authenticate(#err_packet{}=Err, Sock) ->
    try_close(Sock),
    {error, Err}.

%% ===================================================================
%% Close
%% ===================================================================

close(#mysql{sock=Sock}) ->
    send_packet(Sock, 0, com_packet(quit, [])),
    try_close(Sock).

try_close(Sock) ->
    case mysql_net:close(Sock) of
        ok -> ok;
        {error, closed} -> ok;
        {error, Err} -> {error, Err}
    end.

%% ===================================================================
%% Simple command
%% ===================================================================

simple_command(#mysql{sock=Sock}, Command, Args) ->
    send_packet(Sock, 0, com_packet(Command, Args)),
    {_Seq, Result} = recv_decoded_packet(Sock),
    Result.

com_packet(ping, []) -> mysql_protocol:com_ping();
com_packet(quit, []) -> mysql_protocol:com_quit();
com_packet(query, [Query]) -> mysql_protocol:com_query(Query).

send_packet(Sock, Seq, Packet) ->
    handle_send_packet(mysql_protocol:send_packet(Sock, Seq, Packet)).

handle_send_packet(ok) -> ok;
handle_send_packet({error, Err}) ->
    error({send_packet, Err}).

recv_decoded_packet(Sock) ->
    decode_packet(recv_packet(Sock)).

recv_packet(Sock) ->
    handle_recv_packet(mysql_protocol:recv_packet(Sock)).

handle_recv_packet({ok, {Seq, Packet}}) ->
    {Seq, Packet};
handle_recv_packet({error, Err}) ->
    error({recv_packet, Err}).

decode_packet(Packet) ->
    mysql_protocol:decode_packet(Packet).

%% ===================================================================
%% Query
%% ===================================================================

query(#mysql{sock=Sock}, Query) ->
    send_packet(Sock, 0, com_packet(query, [Query])),
    recv_query_result(Sock).

recv_query_result(Sock) ->
    Packet = recv_decoded_packet(Sock),
    handle_query_result_first_packet(Packet, Sock).

handle_query_result_first_packet({1, #ok_packet{}=OK}, _Sock) ->
    OK;
handle_query_result_first_packet({1, #resultset_packet{}=RS}, Sock) ->
    recv_query_result_columns(Sock, RS);
handle_query_result_first_packet({1, #err_packet{}=Err}, _Sock) ->
    Err.

recv_query_result_columns(Sock, RS) ->
    Packet = recv_decoded_packet(Sock),
    handle_query_result_column(Packet, Sock, RS).

handle_query_result_column({_Seq, #raw_packet{data=Data}}, Sock, RS) ->
    ColDef = mysql_protocol:decode_column_definition(Data),
    Packet = recv_decoded_packet(Sock),
    handle_query_result_column(Packet, Sock, add_column(ColDef, RS));
handle_query_result_column({_Seq, #eof_packet{}}, Sock, RS) ->
    recv_query_result_rows(Sock, finalize_columns(RS)).

add_column(Col, #resultset_packet{columns=Cols}=RS) ->
    RS#resultset_packet{columns=[Col|Cols]}.

finalize_columns(#resultset_packet{columns=Cols}=RS) ->
    RS#resultset_packet{columns=lists:reverse(Cols)}.

recv_query_result_rows(Sock, RS) ->
    Packet = recv_decoded_packet(Sock),
    handle_query_result_row(Packet, Sock, RS).

handle_query_result_row({_Seq, #raw_packet{data=Data}}, Sock, RS) ->
    Row = mysql_protocol:decode_resultset_row(Data),
    Packet = recv_decoded_packet(Sock),
    handle_query_result_row(Packet, Sock, add_row(Row, RS));
handle_query_result_row({_Seq, #eof_packet{}}, _Sock, RS) ->
    finalize_rows(RS).

add_row(Row, #resultset_packet{rows=Rows}=RS) ->
    RS#resultset_packet{rows=[list_to_tuple(Row)|Rows]}.

finalize_rows(#resultset_packet{rows=Rows}=RS) ->
    RS#resultset_packet{rows=lists:reverse(Rows)}.

%% ===================================================================
%% Command wrappers
%% ===================================================================

ping(Db) ->
    simple_command(Db, ping, []).

