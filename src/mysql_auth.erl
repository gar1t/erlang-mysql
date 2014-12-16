%% mysql_auth - Implements MySQL auth support
%%
%% This library uses mysql_protocol to interface with the MySQL server over
%% a server socket.
%%
-module(mysql_auth).

-export([authenticate/3]).

-include("mysql_internal.hrl").

-record(state, {sock, user, pwd, auth_result}).

%% ===================================================================
%% Authenticate
%% ===================================================================

authenticate(Sock, User, Password) ->
    Phases = 
        [fun handshake_phase/1,
         fun auth_phase/1],
    State = #state{sock=Sock, user=User, pwd=Password},
    apply_authenticate_phases(Phases, State).

apply_authenticate_phases([Phase|Rest], State) ->
    apply_authenticate_phases(Rest, Phase(State));
apply_authenticate_phases([], State) ->
    authenticate_result(State).

handshake_phase(#state{sock=Sock, user=User, pwd=Pwd}=State) ->
    {Seq, Handshake} = recv_handshake(Sock),
    Response = handshake_response(Handshake, User, Pwd),
    send_packet(Sock, Seq + 1, Response),
    State.

recv_handshake(Sock) ->
    {Seq, Data} = recv_packet(Sock),
    {Seq, mysql_protocol:decode_handshake(Data)}.

handshake_response(Handshake, User, Password) ->
    mysql_protocol:encode_handshake_response(Handshake, User, Password).

auth_phase(#state{sock=Sock}=S) ->
    ResponseAck = recv_response_ack(Sock),
    S#state{auth_result=ResponseAck}.

recv_response_ack(Sock) ->
    Packet = recv_packet(Sock),
    {_Seq, Ack} = mysql_protocol:decode_packet(Packet),
    Ack.

authenticate_result(#state{auth_result=Result}) -> Result.

%% ===================================================================
%% Protocol helpers
%% ===================================================================

recv_packet(Sock) ->
    handle_recv_packet(mysql_protocol:recv_packet(Sock)).

handle_recv_packet({ok, Packet}) -> Packet;
handle_recv_packet({error, Err}) ->
    error(recv_packet, Err).

send_packet(Sock, Seq, Packet) ->
    handle_send_packet(mysql_protocol:send_packet(Sock, Seq, Packet)).

handle_send_packet(ok) -> ok;
handle_send_packet({error, Err}) ->
    error(send_packet, Err).
