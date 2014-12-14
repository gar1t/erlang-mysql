%% mysql_lib - MySQL specific interface
%%
%% This library uses mysql_net, mysql_protocol, and mysql_auth to provide
%% functions that a higher level DB interface can use to interface with a MySQL
%% server.
%%
-module(mysql_lib).

-export([connect/1,
         simple_command/3,
         ping/1,
         close/1]).

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
    handle_connect(mysql_net:connect(Host, Port, Timeout), User, Pwd).

handle_connect({ok, Sock}, User, Pwd) ->
    handle_authenticate(mysql_auth:authenticate(Sock, User, Pwd), Sock);
handle_connect({error, Err}, _User, _Pwd) ->
    {error, Err}.

handle_authenticate(ok, Sock) ->
    {ok, #mysql{sock=Sock}};
handle_authenticate({error, Err}, Sock) ->
    try_close(Sock),
    {error, Err}.

%% ===================================================================
%% Simple command
%% ===================================================================

simple_command(#mysql{sock=Sock}, Command, Args) ->
    send_packet(Sock, 0, com_packet(Command, Args)),
    {_Seq, Packet} = recv_packet(Sock),
    decode_packet(Packet).

com_packet(ping, []) -> mysql_protocol:com_ping();
com_packet(quit, []) -> mysql_protocol:com_quit().

send_packet(Sock, Seq, Packet) ->
    handle_send_packet(mysql_protocol:send_packet(Sock, Seq, Packet)).

handle_send_packet(ok) -> ok;
handle_send_packet({error, Err}) ->
    error(send_packet, Err).

recv_packet(Sock) ->
    handle_recv_packet(mysql_protocol:recv_packet(Sock)).

handle_recv_packet({ok, {Seq, Packet}}) ->
    {Seq, Packet};
handle_recv_packet({error, Err}) ->
    error({recv_packet, Err}).

decode_packet(Packet) ->
    mysql_protocol:decode_packet(Packet).

%% ===================================================================
%% Command wrappers
%% ===================================================================

ping(Db) ->
    handle_ping_command(simple_command(Db, ping, [])).

handle_ping_command(#ok_packet{}) -> ok;
handle_ping_command(#err_packet{}=Err) -> {error, Err}.

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
