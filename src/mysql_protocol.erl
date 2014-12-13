-module(mysql_protocol).

-export([test/0, test2/0]).

-record(handshake,
        {seq,
         protocol_version,
         server_version,
         connection_id,
         auth_plugin_data_1,
         capabilities_low,
         character_set,
         status_flags,
         capabilities_high,
         auth_plugin_data_len,
         auth_plugin_data_2,
         auth_plugin_name}).

-record(hs_response_state,
        {handshake,
         user,
         password}).

-record(ok_packet,
        {seq,
         affected_rows,
         last_insert_id,
         status_flags,
         warnings,
         info}).

-record(err_packet,
        {seq,
         code,
         sqlstate,
         msg}).

%% Generic response headers

-define(OK_HEADER,  16#00).
-define(ERR_HEADER, 16#ff).

%% Capability flags

-define(CLIENT_PROTOCOL_41,       16#00000200).
-define(CLIENT_SECURE_CONNECTION, 16#00008000).

%% ===================================================================
%% Our tests
%% ===================================================================

test() ->
    User = <<>>,
    Pwd = <<>>,
    Sock = connection(),
    Handshake = handshake(recv_packet(Sock)),
    HandshakeResponse = handshake_response(Handshake, User, Pwd),

    send_packet(Sock, HandshakeResponse),
    Last = decode_packet(recv_packet(Sock)),
    
    gen_tcp:close(Sock),

    io:format("Handshake: ~p~n", [Handshake]),
    io:format("HandshakeResponse:~n~s~n",
              [mysql_debug:hexdump(packet_data(HandshakeResponse))]),
    io:format("Last Packet: ~p~n", [Last]).

connection() ->
    Host = "localhost",
    Port = 3306,
    Options = [binary, {packet, raw}, {active, false}],
    Timeout = 1000,
    {ok, Sock} = gen_tcp:connect(Host, Port, Options, Timeout),
    Sock.

%% ===================================================================
%% Tests with emysql
%% ===================================================================

test2() ->
    Connection = connection(),
    Result = emysql_auth:handshake(Connection, "", ""),
    gen_tcp:close(Connection),
    Result.

%% ===================================================================
%% Helpers
%% ===================================================================

recv_packet(Sock) ->
    {ok, <<Len:24/little, Seq:8>>} = gen_tcp:recv(Sock, 4),
    {ok, Packet} = gen_tcp:recv(Sock, Len),
    {Seq, Packet}.

send_packet(Sock, {Seq, Data}) ->
    Len = iolist_size(Data),
    ok = gen_tcp:send(Sock, [<<Len:24/little, Seq:8>>, Data]).

asciiz(Data) ->
    [Str, Rest] = binary:split(Data, <<0>>),
    {Str, Rest}.

strip_null(Str) ->
    [Stripped, <<>>] = binary:split(Str, <<0>>),
    Stripped.

maybe_strip_null(Str) ->
    case binary:split(Str, <<0>>) of
        [Stripped, <<>>] -> Stripped;
        [Str] -> Str
    end.

apply_decoders([Decoder|Rest], Data0, Acc0) ->
    {Data, Acc} = Decoder(Data0, Acc0),
    apply_decoders(Rest, Data, Acc);
apply_decoders([], _Data, Acc) -> Acc.

apply_encoders([Encoder|Rest], State0, DataAcc0) ->
    {State, DataAcc} = Encoder(State0, DataAcc0),
    apply_encoders(Rest, State, DataAcc);
apply_encoders([], _State, DataAcc) ->
    lists:reverse(DataAcc).

packet_data({_Seq, Data}) -> Data.

next_seq(#handshake{seq=Seq}) -> Seq + 1.

decode_integer(<<I:8/integer, Rest/binary>>) when I < 250 ->
    {I, Rest}.

%% ===================================================================
%% Handshake
%% ===================================================================

handshake({Seq, Data}) ->
    Decoders = 
        [fun hs_protocol_version/2,
         fun hs_server_version/2,
         fun hs_connection_id/2,
         fun hs_auth_plugin_data_1/2,
         fun hs_capabilities_low/2,
         fun hs_character_set/2,
         fun hs_status_flags/2,
         fun hs_capabilities_high/2,
         fun hs_auth_plugin_data_len/2,
         fun hs_auth_plugin_data_2/2,
         fun hs_auth_plugin_name/2],
    apply_decoders(Decoders, Data, init_handshake(Seq)).

init_handshake(Seq) ->
    #handshake{seq=Seq}.

hs_protocol_version(<<Ver:8, Rest/binary>>, HS) ->
    {Rest, HS#handshake{protocol_version=Ver}}.

hs_server_version(Data, HS) ->
    {Ver, Rest} = asciiz(Data),
    {Rest, HS#handshake{server_version=Ver}}.

hs_connection_id(<<Id:32/little, Rest/binary>>, HS) ->
    {Rest, HS#handshake{connection_id=Id}}.

hs_auth_plugin_data_1(<<Data:8/binary, _:1/binary, Rest/binary>>, HS) ->
    {Rest, HS#handshake{auth_plugin_data_1=Data}}.

hs_capabilities_low(<<Low:16/little>>, HS) ->
    {<<>>, HS#handshake{capabilities_low=Low}};
hs_capabilities_low(<<Low:16/little, Rest/binary>>, HS) ->
    {Rest, HS#handshake{capabilities_low=Low}}.

hs_character_set(<<CharacterSet:8, Rest/binary>>, HS) ->
    {Rest, HS#handshake{character_set=CharacterSet}}.

hs_status_flags(<<Flags:16/little, Rest/binary>>, HS) ->
    {Rest, HS#handshake{status_flags=Flags}}.

hs_capabilities_high(<<High:16/little, Rest/binary>>, HS) ->
    {Rest, HS#handshake{capabilities_high=High}}.

hs_auth_plugin_data_len(<<Len:8, _:10/binary, Rest/binary>>, HS) ->
    {Rest, HS#handshake{auth_plugin_data_len=Len}}.

hs_auth_plugin_data_2(Data, #handshake{auth_plugin_data_len=Len}=HS) ->
    PartLen = max(13, Len - 8),
    <<PluginData:PartLen/binary, Rest/binary>> = Data,
    {Rest, HS#handshake{auth_plugin_data_2=strip_null(PluginData)}}.

hs_auth_plugin_name(Data, HS) ->
    %% Plugin name may not be null terminated - see
    %% http://bugs.mysql.com/bug.php?id=59453
    Name = maybe_strip_null(Data),
    {<<>>, HS#handshake{auth_plugin_name=Name}}.

%% ===================================================================
%% Handshake response
%% ===================================================================

handshake_response(HS, User, Pwd) ->
    Encoders =
        [fun hsr_capabilities/2,
         fun hsr_max_packet_size/2,
         fun hsr_character_set/2,
         fun hsr_reserved/2,
         fun hsr_username/2,
         fun hsr_auth_response/2],
    State = init_hs_response_state(HS, User, Pwd),
    Data = apply_encoders(Encoders, State, []),
    {next_seq(HS), Data}.

init_hs_response_state(HS, User, Pwd) ->
    #hs_response_state{
       handshake=HS,
       user=User,
       password=Pwd}.

hsr_capabilities(State, Data) ->
    Capabilities =
        ?CLIENT_PROTOCOL_41 bor
        ?CLIENT_SECURE_CONNECTION,
    {State, [<<Capabilities:32/little>>|Data]}.

hsr_max_packet_size(State, Data) ->
    Size = mysql:get_cfg(max_allowed_packet),
    {State, [<<Size:32/little>>|Data]}.

hsr_character_set(State, Data) ->
    CharacterSet = mysql:get_cfg(default_character_set),
    {State, [<<CharacterSet:8>>|Data]}.

hsr_reserved(State, Data) ->
    {State, [<<0:23/integer-unit:8>>|Data]}.

hsr_username(#hs_response_state{user=User}=State, Data) ->
    {State, [<<User/binary, 0>>|Data]}.

hsr_auth_response(State, Data) ->
    AuthResp = auth_response(State),
    AuthRespLen = size(AuthResp),
    {State, [<<AuthRespLen:8, AuthResp/binary>>|Data]}.

auth_response(#hs_response_state{handshake=HS, password=Pwd}) ->
    Salt = handshake_salt(HS),
    secure_password(Salt, Pwd).

handshake_salt(
  #handshake{
     auth_plugin_name= <<"mysql_native_password">>,
     auth_plugin_data_1=SaltLow,
     auth_plugin_data_2=SaltHigh}) ->
    <<SaltLow/binary, SaltHigh/binary>>.

secure_password(_, <<>>) -> <<>>;
secure_password(Salt, Pwd) ->
    PwdHash = sha1(Pwd),
    PwdHashHash = sha1(PwdHash),
    SaltedPwdHash = sha1(<<Salt/binary, PwdHashHash/binary>>),
    sha1_xor(PwdHash, SaltedPwdHash).

sha1(Data) -> crypto:hash(sha, Data).

sha1_xor(H1, H2) ->
    <<I1:160/integer>> = H1,
    <<I2:160/integer>> = H2,
    Xored = I1 bxor I2,
    <<Xored:160/integer>>.

%% ===================================================================
%% Generic response packet decoding
%% ===================================================================

decode_packet({Seq, <<?OK_HEADER:8, Data/binary>>}) ->
    ok_packet(Seq, Data);
decode_packet({Seq, <<?ERR_HEADER:8, Data/binary>>}) ->
    error_packet(Seq, Data).

%% ===================================================================
%% OK packet
%% ===================================================================

ok_packet(Seq, Data) ->
    Decoders =
        [fun ok_affected_rows/2,
         fun ok_last_insert_id/2,
         fun ok_status_and_rest/2],
    apply_decoders(Decoders, Data, #ok_packet{seq=Seq}).

ok_affected_rows(Data, OK) ->
    {Rows, Rest} = decode_integer(Data),
    {Rest, OK#ok_packet{affected_rows=Rows}}.

ok_last_insert_id(Data, OK) ->
    {Id, Rest} = decode_integer(Data),
    {Rest, OK#ok_packet{last_insert_id=Id}}.

ok_status_and_rest(<<Status:16/little,
                     Warnings:16/little,
                     Info/binary>>, OK) ->
    {<<>>,
     OK#ok_packet{
       status_flags=Status,
       warnings=Warnings,
       info=Info}}.

%% ===================================================================
%% Error packet
%% ===================================================================

error_packet(Seq, <<Code:16/little,
                    _:1/binary,
                    SqlState:5/binary,
                    Msg/binary>>) ->
    #err_packet{
       seq=Seq,
       code=Code,
       sqlstate=SqlState,
       msg=Msg}.
