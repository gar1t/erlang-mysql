-module(mysql_protocol).

-export([test/0, test2/0, test3/0]).

-record(handshake,
        {protocol_version,
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



%% ===================================================================
%% Our tests
%% ===================================================================

test() ->
    User = "",
    Pwd = "",
    Sock = connection(),
    %Handshake = handshake(recv_packet(Sock)),
    Handshake = handshake2(recv_packet(Sock)),
    _Response = handshake_response(Handshake, User, Pwd),
    
    gen_tcp:close(Sock),
    Handshake.

test3() ->
    Sock = connection(),
    Handshake = handshake(recv_packet(Sock)),
    gen_tcp:close(Sock),
    Handshake.    

connection() ->
    Host = "localhost",
    Port = 3306,
    Options = [binary, {packet, raw}, {active, false}],
    Timeout = 1000,
    {ok, Sock} = gen_tcp:connect(Host, Port, Options, Timeout),
    Sock.

recv_packet(Sock) ->
    {ok, <<Len:24/little, _SeqNum:8>>} = gen_tcp:recv(Sock, 4),
    {ok, Packet} = gen_tcp:recv(Sock, Len),
    Packet.

asciiz(Data) ->
    [Str, Rest] = binary:split(Data, <<0>>),
    {Str, Rest}.

strip_null(Str) ->
    {Stripped, <<>>} = asciiz(Str),
    Stripped.

%% ===================================================================
%% Handshake
%% ===================================================================

handshake(Packet) ->
    protocol_version(Packet, #handshake{}).

protocol_version(<<Ver:8/little, Rest/binary>>, HS) ->
    server_version(Rest, HS#handshake{protocol_version=Ver}).

server_version(Data, HS) ->
    {Ver, Rest} = asciiz(Data),
    connection_id(Rest, HS#handshake{server_version=Ver}).

connection_id(<<Id:32/little, Rest/binary>>, HS) ->
    auth_plugin_data_1(Rest, HS#handshake{connection_id=Id}).

auth_plugin_data_1(<<Data:8/binary, _Unused:1/binary, Rest/binary>>, HS) ->
    capabilities_low(Rest, HS#handshake{auth_plugin_data_1=Data}).

capabilities_low(<<Low:16/little>>, HS) ->
    HS#handshake{capabilities_low=Low};
capabilities_low(<<Low:16/little, Rest/binary>>, HS) ->
    character_set(Rest, HS#handshake{capabilities_low=Low}).

character_set(<<CharacterSet:8/little, Rest/binary>>, HS) ->
    status_flags(Rest, HS#handshake{character_set=CharacterSet}).

status_flags(<<Flags:16/little, Rest/binary>>, HS) ->
    capabilities_high(Rest, HS#handshake{status_flags=Flags}).

capabilities_high(<<High:16/little, Rest/binary>>, HS) ->
    auth_plugin_data_len(Rest, HS#handshake{capabilities_high=High}).

auth_plugin_data_len(<<Len:8/little, _Unused:10/binary, Rest/binary>>, HS) ->
    auth_plugin_data_2(Rest, HS#handshake{auth_plugin_data_len=Len}).

auth_plugin_data_2(Data, #handshake{auth_plugin_data_len=Len}=HS) ->
    PartLen = max(13, Len - 8),
    <<PluginData:PartLen/binary, Rest/binary>> = Data,
    auth_plugin_name(
      Rest, HS#handshake{auth_plugin_data_2=strip_null(PluginData)}).

auth_plugin_name(Data, HS) ->
    HS#handshake{auth_plugin_name=strip_null(Data)}.

%% ===================================================================
%% Handshake 2
%% ===================================================================

handshake2(Data) ->
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
    apply_decoders(Decoders, Data, #handshake{}).

apply_decoders([Decoder|Rest], Data0, Acc0) ->
    {Data, Acc} = Decoder(Data0, Acc0),
    apply_decoders(Rest, Data, Acc);
apply_decoders([], _Data, Acc) -> Acc.

hs_protocol_version(<<Ver:8/little, Rest/binary>>, HS) ->
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

hs_character_set(<<CharacterSet:8/little, Rest/binary>>, HS) ->
    {Rest, HS#handshake{character_set=CharacterSet}}.

hs_status_flags(<<Flags:16/little, Rest/binary>>, HS) ->
    {Rest, HS#handshake{status_flags=Flags}}.

hs_capabilities_high(<<High:16/little, Rest/binary>>, HS) ->
    {Rest, HS#handshake{capabilities_high=High}}.

hs_auth_plugin_data_len(<<Len:8/little, _:10/binary, Rest/binary>>, HS) ->
    {Rest, HS#handshake{auth_plugin_data_len=Len}}.

hs_auth_plugin_data_2(Data, #handshake{auth_plugin_data_len=Len}=HS) ->
    PartLen = max(13, Len - 8),
    <<PluginData:PartLen/binary, Rest/binary>> = Data,
    {Rest, HS#handshake{auth_plugin_data_2=strip_null(PluginData)}}.

hs_auth_plugin_name(Data, HS) ->
    {<<>>, HS#handshake{auth_plugin_name=strip_null(Data)}}.

%% ===================================================================
%% Handshake response
%% ===================================================================

handshake_response(_HS, _User, _Pwd) ->
    xxx.

%% ===================================================================
%% Tests with emysql
%% ===================================================================

test2() ->
    Connection = connection(),
    Result = emysql_auth:handshake(Connection, "", ""),
    gen_tcp:close(Connection),
    Result.
