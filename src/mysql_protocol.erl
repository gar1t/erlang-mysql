%% mysql_protocol - Support for the MySQL client/server protocol
%%
%% This module attempts to implement the protocol defined here:
%%
%% http://dev.mysql.com/doc/internals/en/client-server-protocol.html
%%
-module(mysql_protocol).

%% Network API
-export([send_packet/3,
         recv_packet/1]).

%% Packet encoding/decoding
-export([decode_handshake/1,
         decode_packet/1,
         decode_column_definition/1,
         decode_stmt_prepare_resp_packet/1,
         decode_resultset_row/1,
         encode_handshake_response/3]).

%% Command packets
-export([com_quit/0,
         com_init/1,
         com_query/1,
         com_ping/0,
         com_stmt_prepare/1,
         com_stmt_execute/2,
         com_stmt_close/1]).

-include("mysql_internal.hrl").

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

-record(hs_response_state,
        {handshake,
         user,
         password}).

-record(stmt_exec_state,
        {stmt_id,
         params,
         values}).

%% Capability flags

-define(CLIENT_PROTOCOL_41,       16#00000200).
-define(CLIENT_SECURE_CONNECTION, 16#00008000).

%% Generic response headers

-define(OK_HEADER,   16#00).
-define(ERR_HEADER,  16#ff).
-define(EOF_HEADER,  16#fe).
-define(NULL_HEADER, 16#fb).

%% Commands

-define(COM_QUIT,         16#01).
-define(COM_INIT,         16#02).
-define(COM_QUERY,        16#03).
-define(COM_PING,         16#0e).
-define(COM_STMT_PREPARE, 16#16).
-define(COM_STMT_EXECUTE, 16#17).
-define(COM_STMT_CLOSE,   16#19).

%% ===================================================================
%% Network
%% ===================================================================

send_packet(Sock, Seq, Data) ->
    Len = iolist_size(Data),
    mysql_net:send(Sock, [<<Len:24/little, Seq:8>>, Data]).

recv_packet(Sock) ->
    handle_packet_header_recv(mysql_net:recv(Sock, 4), Sock).

handle_packet_header_recv({ok, <<Len:24/little, Seq:8>>}, Sock) ->
    handle_packet_payload_recv(mysql_net:recv(Sock, Len), Seq);
handle_packet_header_recv({error, Err}, _Sock) ->
    {error, Err}.

handle_packet_payload_recv({ok, Packet}, Seq) ->
    {ok, {Seq, Packet}}.

%% ===================================================================
%% Handshake decoder
%% ===================================================================

decode_handshake(Data) ->
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
%% Handshake response encoder
%% ===================================================================

encode_handshake_response(HS, User, Pwd) ->
    Encoders =
        [fun hsr_capabilities/2,
         fun hsr_max_packet_size/2,
         fun hsr_character_set/2,
         fun hsr_reserved/2,
         fun hsr_username/2,
         fun hsr_auth_response/2],
    State = init_hs_response_state(HS, User, Pwd),
    apply_encoders(Encoders, State, []).

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
%% Generic packet decoder
%% ===================================================================

decode_packet({Seq, <<?OK_HEADER:8, Data/binary>>}) ->
    {Seq, ok_packet(Data)};
decode_packet({Seq, <<?ERR_HEADER:8, Data/binary>>}) ->
    {Seq, error_packet(Data)};
decode_packet({Seq, <<?EOF_HEADER:8, Data/binary>>}) ->
    {Seq, eof_packet(Data)};
decode_packet({1, Data}) ->
    {1, init_resultset(Data)};
decode_packet({Seq, Data}) ->
    {Seq, raw_packet(Data)}.

%% ===================================================================
%% OK packet
%% ===================================================================

ok_packet(Data) ->
    Decoders =
        [fun ok_affected_rows/2,
         fun ok_last_insert_id/2,
         fun ok_status_and_rest/2],
    apply_decoders(Decoders, Data, #ok_packet{}).

ok_affected_rows(Data, OK) ->
    {Rows, Rest} = decode_integer(Data),
    {Rest, OK#ok_packet{affected_rows=Rows}}.

ok_last_insert_id(Data, OK) ->
    {Id, Rest} = decode_integer(Data),
    {Rest, OK#ok_packet{last_insert_id=Id}}.

ok_status_and_rest(<<Status:16/little,
                     Warnings:16/little,
                     Info/binary>>, OK) ->
    {<<>>, OK#ok_packet{
             status_flags=Status,
             warnings=Warnings,
             info=Info}}.

%% ===================================================================
%% Error packet
%% ===================================================================

error_packet(<<Code:16/little,
               _:1/binary,
               SqlState:5/binary,
               Msg/binary>>) ->
    #err_packet{code=Code, sqlstate=SqlState, msg=Msg}.

%% ===================================================================
%% EOF Packet
%% ===================================================================

eof_packet(<<Warnings:16/little, Status:16/little>>) ->
    #eof_packet{warnings=Warnings, status_flags=Status}.

%% ===================================================================
%% Resulset packet
%% ===================================================================

init_resultset(Data) ->
    {ColCount, <<>>} = decode_integer(Data),
    #resultset{column_count=ColCount}.

%% ===================================================================
%% Unknown packet
%% ===================================================================

raw_packet(Data) ->
    #raw_packet{data=Data}.

%% ===================================================================
%% Column definition decoder
%% ===================================================================

decode_column_definition(Data) ->
    Decoders =
        [fun col_catalog/2,
         fun col_schema/2,
         fun col_table/2,
         fun col_org_table/2,
         fun col_name/2,
         fun col_org_name/2,
         fun col_fixed_length_fields/2,
         fun col_default_values/2],
    apply_decoders(Decoders, Data, #coldef{}).

col_catalog(Data, C) ->
    {Catalog, Rest} = decode_string(Data),
    {Rest, C#coldef{catalog=Catalog}}.

col_schema(Data, C) ->
    {Schema, Rest} = decode_string(Data),
    {Rest, C#coldef{schema=Schema}}.

col_table(Data, C) ->
    {Table, Rest} = decode_string(Data),
    {Rest, C#coldef{table=Table}}.

col_org_table(Data, C) ->
    {Table, Rest} = decode_string(Data),
    {Rest, C#coldef{org_table=Table}}.

col_name(Data, C) ->
    {Name, Rest} = decode_string(Data),
    {Rest, C#coldef{name=Name}}.

col_org_name(Data, C) ->
    {Name, Rest} = decode_string(Data),
    {Rest, C#coldef{org_name=Name}}.

col_fixed_length_fields(
  <<16#0c,
    CharacterSet:16/little,
    ColLength:32/little,
    Type:8,
    Flags:16/little,
    Decimals:8,
    0, 0,
    Rest/binary>>, C) ->
    {Rest, C#coldef{
             character_set=CharacterSet,
             column_length=ColLength,
             type=Type,
             flags=Flags,
             decimals=Decimals}}.

col_default_values(<<>>, C) -> {<<>>, C};
col_default_values(Data, C) ->
    {Len, ValuesPlusRest} = decode_integer(Data),
    {Rest, Values} = split_at_len(ValuesPlusRest, Len),
    {Rest, C#coldef{default_values=Values}}.

%% ===================================================================
%% Row decoder
%% ===================================================================

decode_resultset_row(<<?NULL_HEADER:8>>) -> null;
decode_resultset_row(Data) ->
    acc_decoded_strings(Data, []).

acc_decoded_strings(<<>>, Acc) ->
    lists:reverse(Acc);
acc_decoded_strings(Data, Acc) ->
    {Str, Rest} = decode_string(Data),
    acc_decoded_strings(Rest, [Str|Acc]).

%% ===================================================================
%% Statement response packet decoder
%%
%% MySQL cleverly reuses the same packet header for OK and
%% STMT_PREPARE_OK, even though they're completely different
%% structures. This variant of decode packet should be used for stmt
%% prepare responses.
%% ===================================================================

decode_stmt_prepare_resp_packet({Seq, <<?OK_HEADER:8, Data/binary>>}) ->
    {Seq, init_prepared_stmt(Data)};
decode_stmt_prepare_resp_packet({Seq, <<?ERR_HEADER:8, Data/binary>>}) ->
    {Seq, error_packet(Data)};
decode_stmt_prepare_resp_packet({Seq, Data}) ->
    {Seq, raw_packet(Data)}.

init_prepared_stmt(
  <<StmtId:32/little,
    ColCount:16/little,
    ParamCount:16/little,
    _:8,
    WarningCount:16/little>>) ->
    #prepared_stmt{
       stmt_id=StmtId,
       column_count=ColCount,
       param_count=ParamCount,
       warning_count=WarningCount}.

%% ===================================================================
%% Simple command packets
%% ===================================================================

com_ping() -> <<?COM_PING:8>>.

com_quit() -> <<?COM_QUIT:8>>.

com_init(Db) -> <<?COM_INIT:8, Db/binary>>.

com_query(Query) -> <<?COM_QUERY:8, Query/binary>>.

com_stmt_prepare(Query) -> <<?COM_STMT_PREPARE:8, Query/binary>>.

com_stmt_close(StmtId) -> <<?COM_STMT_CLOSE:8, StmtId:32/little>>.

%% ===================================================================
%% Stmt execute packet
%% ===================================================================

com_stmt_execute(Stmt, Values) ->
    Encoders =
        [fun stmt_exec_head/2,
         fun stmt_exec_null_bitmap/2,
         fun stmt_exec_new_params_bound_flag/2,
         fun stmt_exec_params/2],
    State = init_stmt_exec_state(Stmt, Values),
    apply_encoders(Encoders, State, []).

init_stmt_exec_state(#prepared_stmt{stmt_id=StmtId, params=Params}, Values) ->
    #stmt_exec_state{
       stmt_id=StmtId,
       params=Params,
       values=Values}.

stmt_exec_head(#stmt_exec_state{stmt_id=StmtId}=State, Data) ->
    Head = <<?COM_STMT_EXECUTE:8, StmtId:32/little, 0:8, 1:32/little>>,
    {State, [Head|Data]}.

%% stmt_exec_null_bitmap(#stmt_exec_state{params=Params}=State, Data) ->
%%     N = (length(Params) + 7) div 8,
%%     {State, [<<0:N/integer-unit:8>>|Data]}.
stmt_exec_null_bitmap(State, Data) ->
    {State, [<<4>>|Data]}.

stmt_exec_new_params_bound_flag(State, Data) ->
    {State, [<<1>>|Data]}.

stmt_exec_params(#stmt_exec_state{values=Values}=Stmt, Data) ->
    {EncodedTypes, EncodedVals} = encode_params(Values),
    {Stmt, [EncodedVals, EncodedTypes|Data]}.

encode_params(Values) ->
    encode_params(Values, [], []).

encode_params([Value|Rest], EncTypes, EncVals) ->
    {EncType, EncVal} = encode_param(Value),
    encode_params(Rest, [EncType|EncTypes], [EncVal|EncVals]);
encode_params([], EncTypes, EncVals) ->
    {lists:reverse(EncTypes), lists:reverse(EncVals)}.

encode_param(I) when is_integer(I) ->
    {<<16#03, 0>>, <<I:32/little>>};
encode_param(null) ->
    {<<16#06, 0>>, <<>>}.

%% ===================================================================
%% Helpers
%% ===================================================================

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

decode_integer(<<I:8/integer,        Rest/binary>>) when I < 251 -> {I, Rest};
decode_integer(<<16#fc, I:16/little, Rest/binary>>) -> {I, Rest};
decode_integer(<<16#fd, I:24/little, Rest/binary>>) -> {I, Rest};
decode_integer(<<16#fe, I:32/little, Rest/binary>>) -> {I, Rest}.

decode_string(<<16#fb, Rest/binary>>) ->
    {null, Rest};
decode_string(Data) ->
    {Len, StrPlusRest} = decode_integer(Data),
    split_at_len(StrPlusRest, Len).

split_at_len(Data, Len) ->
    P1 = binary:part(Data, 0, Len),
    P2 = binary:part(Data, Len, size(Data) - Len),
    {P1, P2}.
