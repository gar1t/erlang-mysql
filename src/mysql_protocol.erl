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
         decode_text_resultset_row/1,
         decode_binary_resultset_row/2,
         encode_handshake_response/3,
         encode_null_bitmap/2,
         is_field_null/3]).

%% Command packets
-export([com_quit/0,
         com_init_db/1,
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

%% Protocol version

-define(PROTOCOL_VER, 10).

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
-define(COM_INIT_DB,      16#02).
-define(COM_QUERY,        16#03).
-define(COM_PING,         16#0e).
-define(COM_STMT_PREPARE, 16#16).
-define(COM_STMT_EXECUTE, 16#17).
-define(COM_STMT_CLOSE,   16#19).

%% Column types

-define(TYPE_DECIMAL,     16#00).
-define(TYPE_TINY,        16#01).
-define(TYPE_SHORT,       16#02).
-define(TYPE_LONG,        16#03).
-define(TYPE_FLOAT,       16#04).
-define(TYPE_DOUBLE,      16#05).
-define(TYPE_NULL,        16#06).
-define(TYPE_TIMESTAMP,   16#07).
-define(TYPE_LONGLONG,    16#08).
-define(TYPE_INT24,       16#09).
-define(TYPE_DATE,        16#0a).
-define(TYPE_TIME,        16#0b).
-define(TYPE_DATETIME,    16#0c).
-define(TYPE_YEAR,        16#0d).
-define(TYPE_NEWDATE,     16#0e).
-define(TYPE_VARCHAR,     16#0f).
-define(TYPE_BIT,         16#10).
-define(TYPE_TIMESTAMP2,  16#11).
-define(TYPE_DATETIME2,   16#12).
-define(TYPE_TIME2,       16#13).
-define(TYPE_NEWDECIMAL,  16#f6).
-define(TYPE_ENUM,        16#f7).
-define(TYPE_SET,         16#f8).
-define(TYPE_TINY_BLOB,   16#f9).
-define(TYPE_MEDIUM_BLOB, 16#fa).
-define(TYPE_LONG_BLOB,   16#fb).
-define(TYPE_BLOB,        16#fc).
-define(TYPE_VAR_STRING,  16#fd).
-define(TYPE_STRING,      16#fe).
-define(TYPE_GEOMETRY,    16#ff).

%% Default config

-define(DEFAULT_MAX_PACKET_SIZE, 104857600). % 100M
-define(DEFAULT_CHARACTER_SET, 0).

%% ===================================================================
%% Network
%% ===================================================================

send_packet(Sock, Seq, Data) ->
    Len = iolist_size(Data),
    mysql_net:send(Sock, [<<Len:24/little, Seq>>, Data]).

recv_packet(Sock) ->
    handle_packet_header_recv(mysql_net:recv(Sock, 4), Sock).

handle_packet_header_recv({ok, <<Len:24/little, Seq>>}, Sock) ->
    handle_packet_payload_recv(mysql_net:recv(Sock, Len), Seq);
handle_packet_header_recv({error, Err}, _Sock) ->
    {error, Err}.

handle_packet_payload_recv({ok, Packet}, Seq) ->
    {ok, {Seq, Packet}}.

%% ===================================================================
%% General decoding / encoding support
%% ===================================================================

apply_decoders([Decoder|Rest], Data0, Acc0) ->
    {Data, Acc} = Decoder(Data0, Acc0),
    apply_decoders(Rest, Data, Acc);
apply_decoders([], _Data, Acc) -> Acc.

apply_encoders([Encoder|Rest], State0, DataAcc0) ->
    {State, DataAcc} = Encoder(State0, DataAcc0),
    apply_encoders(Rest, State, DataAcc);
apply_encoders([], _State, DataAcc) ->
    lists:reverse(DataAcc).

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

hs_protocol_version(<<?PROTOCOL_VER, Rest/binary>>, HS) ->
    {Rest, HS#handshake{protocol_version=?PROTOCOL_VER}};
hs_protocol_version(<<Ver, _/binary>>, _HS) ->
    error({unsupported_protocol_version, Ver}).

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

hs_character_set(<<CharacterSet, Rest/binary>>, HS) ->
    {Rest, HS#handshake{character_set=CharacterSet}}.

hs_status_flags(<<Flags:16/little, Rest/binary>>, HS) ->
    {Rest, HS#handshake{status_flags=Flags}}.

hs_capabilities_high(<<High:16/little, Rest/binary>>, HS) ->
    {Rest, HS#handshake{capabilities_high=High}}.

hs_auth_plugin_data_len(<<Len, _:10/binary, Rest/binary>>, HS) ->
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
    Size = ?DEFAULT_MAX_PACKET_SIZE,
    {State, [<<Size:32/little>>|Data]}.

hsr_character_set(State, Data) ->
    CharacterSet = ?DEFAULT_CHARACTER_SET,
    {State, [<<CharacterSet>>|Data]}.

hsr_reserved(State, Data) ->
    {State, [<<0:23/integer-unit:8>>|Data]}.

hsr_username(#hs_response_state{user=User}=State, Data) ->
    {State, [<<User/binary, 0>>|Data]}.

hsr_auth_response(State, Data) ->
    AuthResp = auth_response(State),
    AuthRespLen = size(AuthResp),
    {State, [<<AuthRespLen, AuthResp/binary>>|Data]}.

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
%%
%% TODO:
%%
%% This implementation shows that it's impossible to implement this
%% functionality as MySQL doesn't use unique headers for each packet
%% type. The last two cases illustrate this, which are hacks to
%% leak raw packet data for context-sensitive decoding. We need to
%% provide all of the various decode_xxx_packet variants to reflect
%% the true complexity of the protocol.
%% ===================================================================

decode_packet({Seq, <<?OK_HEADER, Data/binary>>}) ->
    {Seq, ok_packet(Data)};
decode_packet({Seq, <<?ERR_HEADER, Data/binary>>}) ->
    {Seq, error_packet(Data)};
decode_packet({Seq, <<?EOF_HEADER, Data/binary>>}) ->
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
    #err_packet{code=Code, sqlstate=binary_to_list(SqlState), msg=Msg}.

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
    Type,
    Flags:16/little,
    Decimals,
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
%% Text row decoder
%% ===================================================================

decode_text_resultset_row(Data) ->
    list_to_tuple(acc_decoded_strings(Data, [])).

acc_decoded_strings(<<>>, Acc) ->
    lists:reverse(Acc);
acc_decoded_strings(Data, Acc) ->
    {Str, Rest} = decode_string(Data),
    acc_decoded_strings(Rest, [Str|Acc]).

%% ===================================================================
%% Binary row decoder
%% ===================================================================

decode_binary_resultset_row(Cols, Data) ->
    {Bitmap, Rest} = decode_null_bitmap(Cols, Data),
    decode_binary_values(Cols, null_test_fun(Bitmap, 2), Rest).

decode_null_bitmap(Cols, Data) ->
    BitmapLen = (length(Cols) + 7 + 2) div 8,
    <<Bitmap:BitmapLen/binary, Rest/binary>> = Data,
    {Bitmap, Rest}.

null_test_fun(Bitmap, Offset) ->
    fun(Field) -> is_field_null(Field, Offset, Bitmap) end.

decode_binary_values(Cols, IsNull, Data) ->
    list_to_tuple(acc_decoded_values(Cols, 0, IsNull, Data, [])).

acc_decoded_values([Col|RestCols], ColPos, IsNull, Data, Values) ->
    {Value, RestData} = decode_binary_value(IsNull(ColPos), Col, Data),
    acc_decoded_values(
      RestCols, ColPos + 1, IsNull, RestData, [Value|Values]);
acc_decoded_values([], _ColPos, _IsNull, <<>>, Values) ->
    lists:reverse(Values).

decode_binary_value(_Null=false, #coldef{type=Type, flags=Flags}, Data) ->
    decode_binary_value({Type, signed(Flags)}, Data);
decode_binary_value(_Null=true, _Col, Data) ->
    {null, Data}.

signed(ColFlags) ->
    case ColFlags band 16#20 of
        16#20 -> unsigned;
        0     -> signed
    end.

%% ===================================================================
%% Statement prepare response packet decoder
%% ===================================================================

decode_stmt_prepare_resp_packet({Seq, <<?OK_HEADER, Data/binary>>}) ->
    {Seq, init_prepared_stmt(Data)};
decode_stmt_prepare_resp_packet({Seq, <<?ERR_HEADER, Data/binary>>}) ->
    {Seq, error_packet(Data)};
decode_stmt_prepare_resp_packet({Seq, Data}) ->
    {Seq, raw_packet(Data)}.

init_prepared_stmt(
  <<StmtId:32/little,
    ColCount:16/little,
    ParamCount:16/little,
    _Reserved,
    WarningCount:16/little>>) ->
    #prepared_stmt{
       stmt_id=StmtId,
       column_count=ColCount,
       param_count=ParamCount,
       warning_count=WarningCount}.

%% ===================================================================
%% Simple command packets
%% ===================================================================

com_ping() -> <<?COM_PING>>.

com_quit() -> <<?COM_QUIT>>.

com_init_db(Db) -> <<?COM_INIT_DB, Db/binary>>.

com_query(Query) -> <<?COM_QUERY, Query/binary>>.

com_stmt_prepare(Query) -> <<?COM_STMT_PREPARE, Query/binary>>.

com_stmt_close(StmtId) -> <<?COM_STMT_CLOSE, StmtId:32/little>>.

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
    Head = <<?COM_STMT_EXECUTE, StmtId:32/little, 0, 1:32/little>>,
    {State, [Head|Data]}.

stmt_exec_null_bitmap(#stmt_exec_state{values=Values}=State, Data) ->
    {State, [encode_null_bitmap(0, Values)|Data]}.

stmt_exec_new_params_bound_flag(State, Data) ->
    {State, [<<1>>|Data]}.

stmt_exec_params(#stmt_exec_state{values=Values}=Stmt, Data) ->
    {EncodedTypes, EncodedVals} = encode_params(Values),
    {Stmt, [EncodedVals, EncodedTypes|Data]}.

%% ===================================================================
%% Parameter encoding
%% ===================================================================

encode_params(Values) ->
    encode_params(Values, [], []).

encode_params([Value|Rest], EncTypes, EncVals) ->
    {EncType, EncVal} = encode_param(Value),
    encode_params(Rest, [EncType|EncTypes], [EncVal|EncVals]);
encode_params([], EncTypes, EncVals) ->
    {lists:reverse(EncTypes), lists:reverse(EncVals)}.

encode_param(I) when is_integer(I) ->
    encode_integer_param(I);
encode_param(Str) when is_binary(Str) orelse is_list(Str) ->
    encode_string_param(Str);
encode_param(F) when is_float(F) ->
    encode_float_param(F);
encode_param(null) ->
    encode_null_param();
encode_param(Timestamp) when is_tuple(Timestamp) ->
    encode_timestamp_param(Timestamp).

-define(inrange(I, MinIncl, MaxExcl), I >= MinIncl andalso I < MaxExcl).

-define(tiny(I),      ?inrange(I, -16#80,               16#80)).
-define(utiny(I),     ?inrange(I, 0,                    16#100)).
-define(short(I),     ?inrange(I, -16#8000,             16#8000)).
-define(ushort(I),    ?inrange(I, 0,                    16#10000)).
-define(int24(I),     ?inrange(I, -16#800000,           16#800000)).
-define(uint24(I),    ?inrange(I, 0,                    16#1000000)).
-define(long(I),      ?inrange(I, -16#80000000,         16#80000000)).
-define(ulong(I),     ?inrange(I, 0,                    16#100000000)).
-define(longlong(I),  ?inrange(I, -16#8000000000000000, 16#8000000000000000)).
-define(ulonglong(I), ?inrange(I, 0,                    16#10000000000000000)).

encode_integer_param(I) ->
    if
        ?tiny(I)      -> {<<?TYPE_TINY,     0>>,     <<I>>};
        ?utiny(I)     -> {<<?TYPE_TINY,     16#80>>, <<I>>};
        ?short(I)     -> {<<?TYPE_SHORT,    0>>,     <<I:16/little>>};
        ?ushort(I)    -> {<<?TYPE_SHORT,    16#80>>, <<I:16/little>>};
        ?int24(I)     -> {<<?TYPE_INT24,    0>>,     <<I:24/little>>};
        ?uint24(I)    -> {<<?TYPE_INT24,    16#80>>, <<I:24/little>>};
        ?long(I)      -> {<<?TYPE_LONG,     0>>,     <<I:32/little>>};
        ?ulong(I)     -> {<<?TYPE_LONG,     16#80>>, <<I:32/little>>};
        ?longlong(I)  -> {<<?TYPE_LONGLONG, 0>>,     <<I:64/little>>};
        ?ulonglong(I) -> {<<?TYPE_LONGLONG, 16#80>>, <<I:64/little>>}
    end.

encode_string_param(S) ->
    {<<?TYPE_VAR_STRING, 0>>, encode_string(S)}.

encode_null_param() -> {<<16#06, 0>>, <<>>}.

encode_float_param(F) ->
    {<<?TYPE_DOUBLE, 0>>, <<F:64/little-float>>}.

encode_timestamp_param(TS) ->
    {<<?TYPE_TIMESTAMP, 0>>, encode_timestamp(TS)}.

%% ===================================================================
%% Null terminated strings
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

%% ===================================================================
%% Length encoded integer decoder
%% ===================================================================

decode_integer(<<I/integer, Rest/binary>>) when I < 251 -> {I, Rest};
decode_integer(<<16#fc, I:16/little, Rest/binary>>) -> {I, Rest};
decode_integer(<<16#fd, I:24/little, Rest/binary>>) -> {I, Rest};
decode_integer(<<16#fe, I:64/little, Rest/binary>>) -> {I, Rest}.

%% ===================================================================
%% Fixed length integer decoders
%% ===================================================================

decode_integer(Len, Data) ->
    <<I:Len/little-signed, Rest/binary>> = Data,
    {I, Rest}.

decode_unsigned_integer(Len, Data) ->
    <<I:Len/little, Rest/binary>> = Data,
    {I, Rest}.

%% ===================================================================
%% Length encoded integer encoder
%% ===================================================================

encode_integer(I) when I < 251 -> <<I>>;
encode_integer(I) when I =< 16#ffff -> <<16#fc, I:16/little>>;
encode_integer(I) when I =< 16#ffffff -> <<16#fd, I:24/little>>;
encode_integer(I) when I =< 16#ffffffffffffffff -> <<16#fe, I:64/little>>;
encode_integer(I) -> error({integer_too_big, I}).

%% ===================================================================
%% Fixed length float decoder
%% ===================================================================

decode_float(Len, Data) ->
    <<F:Len/little-signed-float, Rest/binary>> = Data,
    {F, Rest}.

%% ===================================================================
%% Length encoded string decoder
%% ===================================================================

decode_string(<<?NULL_HEADER, Rest/binary>>) ->
    {null, Rest};
decode_string(Data) ->
    {Len, StrPlusRest} = decode_integer(Data),
    split_at_len(StrPlusRest, Len).

split_at_len(Data, Len) ->
    P1 = binary:part(Data, 0, Len),
    P2 = binary:part(Data, Len, size(Data) - Len),
    {P1, P2}.

%% ===================================================================
%% Length encoded string encoder
%% ===================================================================

encode_string(Str) ->
    StrBin = iolist_to_binary(Str),
    Len = encode_integer(size(StrBin)),
    <<Len/binary, StrBin/binary>>.

%% ===================================================================
%% Timestamp decoder
%%
%% TODO: We're discarding the microsecond component here to
%% stay within the calendar module's datetime type. This is a problem
%% obviously. See README for notes on this.
%% ===================================================================

decode_timestamp(<<0, Data/binary>>) ->
    {{{0, 0, 0}, {0, 0, 0}}, Data};
decode_timestamp(<<4, Data/binary>>) ->
    <<Year:16/little, Month, Day, Rest/binary>> = Data,
    {{{Year, Month, Day}, {0, 0, 0}}, Rest};
decode_timestamp(<<7, Data/binary>>) ->
    <<Year:16/little, Month, Day,
      Hour, Minute, Second, Rest/binary>> = Data,
    {{{Year, Month, Day}, {Hour, Minute, Second}}, Rest};
decode_timestamp(<<11, Data/binary>>) ->
    <<Year:16/little, Month, Day,
      Hour, Minute, Second, _Micro:32/little,
      Rest/binary>> = Data,
    {{{Year, Month, Day}, {Hour, Minute, Second}}, Rest}.

%% ===================================================================
%% Timestamp encoder
%% ===================================================================

encode_timestamp({_, _, Micro}=TS) ->
    {{Year, Mon, Day}, {Hour, Min, Sec}} = calendar:now_to_datetime(TS),
    encode_timestamp({{Year, Mon, Day}, {Hour, Min, Sec, Micro}});
encode_timestamp({{Year, Mon, Day}, {Hour, Min, Sec}}) ->
    <<7, Year:16/little, Mon, Day, Hour, Min, Sec>>;
encode_timestamp({{Year, Mon, Day}, {Hour, Min, Sec, Micro}}) ->
    <<11, Year:16/little, Mon, Day, Hour, Min, Sec, Micro:32/little>>.

%% ===================================================================
%% Time decoder
%% ===================================================================

decode_time(Data) ->
    <<Len, Rest/binary>> = Data,
    decode_time(Len, Rest).

decode_time(0, Data) ->
    {{0, 0, 0, 0, 0, 0}, Data};
decode_time(8, Data) ->
    <<IsNegative:1, Days:32/little, Hours,
      Minutes, Seconds, Rest/binary>> = Data,
    {{IsNegative, Days, Hours, Minutes, Seconds, 0}, Rest};
decode_time(12, Data) ->
    <<IsNegative:1, Days:32/little, Hours,
      Minutes, Seconds, Micro:32/little, Rest/binary>> = Data,
    {{IsNegative, Days, Hours, Minutes, Seconds, Micro}, Rest}.

%% ===================================================================
%% NULL bitmap encoder
%% ===================================================================

encode_null_bitmap(Offset, Values) ->
    encode_null_bitmap(Values, Offset, 0, 0).

encode_null_bitmap([Val|Rest], Offset, Field, Bitmap) ->
    encode_null_bitmap(
      Rest, Offset, Field + 1,
      apply_bit(Val, Field, Offset, Bitmap));
encode_null_bitmap([], Offset, FieldCount, Bitmap) ->
    bitmap_bytes(Offset, FieldCount, Bitmap).

apply_bit(null, Field, Offset, Bitmap) ->
    Bitmap bor null_bitmap_bit(Field, Offset);
apply_bit(_, _, _, Bitmap) ->
    Bitmap.

null_bitmap_bit(Field, Offset) ->
    BytePos = (Field + Offset) div 8,
    BitPos = (Field + Offset) rem 8,
    (1 bsl BitPos) bsl (BytePos * 8).

bitmap_bytes(Offset, FieldCount, Bitmap) ->
    NumBytes = (FieldCount + 7 + Offset) div 8,
    <<Bitmap:NumBytes/little-unit:8>>.

%% ===================================================================
%% NULL bitmap decode support
%% ===================================================================

is_field_null(Field, Offset, Bitmap) ->
    BytePos = (Field + Offset) div 8,
    Byte = binary:at(Bitmap, BytePos),
    BitPos = (Field + Offset) rem 8,
    Byte band (1 bsl BitPos) /= 0.

%% ===================================================================
%% Binary value decoder
%% ===================================================================

decode_binary_value(Type, Data) ->
    case Type of
        {?TYPE_DECIMAL, _}         -> decode_string(Data);
        {?TYPE_TINY, signed}       -> decode_integer(8, Data);
        {?TYPE_TINY, unsigned}     -> decode_unsigned_integer(8, Data);
        {?TYPE_SHORT, signed}      -> decode_integer(16, Data);
        {?TYPE_SHORT, unsigned}    -> decode_unsigned_integer(16, Data);
        {?TYPE_LONG, signed}       -> decode_integer(32, Data);
        {?TYPE_LONG, unsigned}     -> decode_unsigned_integer(32, Data);
        {?TYPE_FLOAT, _}           -> decode_float(32, Data);
        {?TYPE_DOUBLE, _}          -> decode_float(64, Data);
        {?TYPE_TIMESTAMP, _}       -> decode_timestamp(Data);
        {?TYPE_LONGLONG, signed}   -> decode_integer(16, Data);
        {?TYPE_LONGLONG, unsigned} -> decode_unsigned_integer(16, Data);
        {?TYPE_INT24, signed}      -> decode_integer(32, Data);
        {?TYPE_INT24, unsigned}    -> decode_unsigned_integer(32, Data);
        {?TYPE_DATE, _}            -> decode_timestamp(Data);
        {?TYPE_TIME, _}            -> decode_time(Data);
        {?TYPE_DATETIME, _}        -> decode_timestamp(Data);
        {?TYPE_YEAR, _}            -> decode_integer(16, Data);
        {?TYPE_VARCHAR, _}         -> decode_string(Data);
        {?TYPE_BIT, _}             -> decode_string(Data);
        {?TYPE_NEWDECIMAL, _}      -> decode_string(Data);
        {?TYPE_ENUM, _}            -> decode_string(Data);
        {?TYPE_SET, _}             -> decode_string(Data);
        {?TYPE_TINY_BLOB, _}       -> decode_string(Data);
        {?TYPE_MEDIUM_BLOB, _}     -> decode_string(Data);
        {?TYPE_LONG_BLOB, _}       -> decode_string(Data);
        {?TYPE_BLOB, _}            -> decode_string(Data);
        {?TYPE_VAR_STRING, _}      -> decode_string(Data);
        {?TYPE_STRING, _}          -> decode_string(Data);
        {?TYPE_GEOMETRY, _}        -> decode_string(Data)
    end.
