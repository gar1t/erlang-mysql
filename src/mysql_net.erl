%% mysql_net - Network interface to a MySQL server
%%
%% Wraps underlying client socket providing a TCP like interface to a MySQL
%% server.
%%
-module(mysql_net).

-export([connect/3,
         send/2,
         recv/2,
         close/1]).

connect(Host, Port, Timeout) ->
    Options = [binary, {packet, raw}, {active, false}],
    gen_tcp:connect(Host, Port, Options, Timeout).

send(Sock, Data) ->
    gen_tcp:send(Sock, Data).

recv(Sock, Len) ->
    gen_tcp:recv(Sock, Len).

close(Sock) ->
    gen_tcp:close(Sock).
