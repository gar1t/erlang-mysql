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
         query/2,
         prepare_statement/2,
         execute_statement/3,
         close_statement/2,
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
    ConnectResp = mysql_net:connect(Host, Port, Timeout),
    try_authenticate(ConnectResp, User, Pwd).

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
    send_packet(Sock, 0, mysql_protocol:com_quit()),
    try_close(Sock).

try_close(Sock) ->
    case mysql_net:close(Sock) of
        ok -> ok;
        {error, closed} -> ok;
        {error, Err} -> {error, Err}
    end.

%% ===================================================================
%% Command support
%% ===================================================================

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
%% Muti-part recv support
%% ===================================================================

apply_recv_parts([Recv|Rest], Sock, Acc) ->
    apply_recv_parts(Rest, Sock, Recv(Sock, Acc));
apply_recv_parts([], _, Acc) -> Acc.

%% ===================================================================
%% Query
%% ===================================================================

query(#mysql{sock=Sock}, Query) ->
    send_packet(Sock, 0, mysql_protocol:com_query(Query)),
    recv_query_resp(Sock).

recv_query_resp(Sock) ->
    Packet = recv_decoded_packet(Sock),
    handle_query_resp_first_packet(Packet, Sock).

handle_query_resp_first_packet({1, #ok_packet{}=OK}, _Sock) ->
    OK;
handle_query_resp_first_packet({1, #resultset{}=RS}, Sock) ->
    recv_query_resp_resultset(Sock, RS);
handle_query_resp_first_packet({1, #err_packet{}=Err}, _Sock) ->
    Err.

recv_query_resp_resultset(Sock, RS) ->
    Parts =
        [fun recv_query_resp_columns/2,
         fun recv_query_resp_rows/2],
    apply_recv_parts(Parts, Sock, RS).

%% ===================================================================
%% Receive column definitions into resultset
%% ===================================================================

recv_query_resp_columns(Sock, RS) ->
    Packet = recv_decoded_packet(Sock),
    handle_query_resp_column(Packet, Sock, RS).

handle_query_resp_column({_Seq, #raw_packet{data=Data}}, Sock, RS) ->
    ColDef = mysql_protocol:decode_column_definition(Data),
    Packet = recv_decoded_packet(Sock),
    handle_query_resp_column(Packet, Sock, add_rs_column(ColDef, RS));
handle_query_resp_column({_Seq, #eof_packet{}}, _Sock, RS) ->
    finalize_rs_columns(RS).

add_rs_column(Col, #resultset{columns=Cols}=RS) ->
    RS#resultset{columns=[Col|Cols]}.

finalize_rs_columns(#resultset{columns=Cols}=RS) ->
    RS#resultset{columns=lists:reverse(Cols)}.

%% ===================================================================
%% Receive text resultset rows
%% ===================================================================

recv_query_resp_rows(Sock, RS) ->
    Packet = recv_decoded_packet(Sock),
    handle_query_resp_row(Packet, Sock, RS).

handle_query_resp_row({_Seq, #raw_packet{data=Data}}, Sock, RS) ->
    Row = mysql_protocol:decode_text_resultset_row(Data),
    Packet = recv_decoded_packet(Sock),
    handle_query_resp_row(Packet, Sock, add_row(Row, RS));
handle_query_resp_row({_Seq, #eof_packet{}}, _Sock, RS) ->
    finalize_rows(RS).

add_row(Row, #resultset{rows=Rows}=RS) ->
    RS#resultset{rows=[Row|Rows]}.

finalize_rows(#resultset{rows=Rows}=RS) ->
    RS#resultset{rows=lists:reverse(Rows)}.

%% ===================================================================
%% Prepare statement
%% ===================================================================

prepare_statement(#mysql{sock=Sock}, Query) ->
    send_packet(Sock, 0, mysql_protocol:com_stmt_prepare(Query)),
    recv_stmt_prepare_resp(Sock).

recv_stmt_prepare_resp(Sock) ->
    Packet = recv_decoded_stmt_prepare_resp_packet(Sock),
    handle_stmt_prepare_resp_first_packet(Packet, Sock).

recv_decoded_stmt_prepare_resp_packet(Sock) ->
    mysql_protocol:decode_stmt_prepare_resp_packet(recv_packet(Sock)).

handle_stmt_prepare_resp_first_packet({1, #prepared_stmt{}=Stmt}, Sock) ->
    recv_prepared_stmt_params(Sock, Stmt);
handle_stmt_prepare_resp_first_packet({1, #err_packet{}=Err}, _Sock) ->
    Err.

recv_prepared_stmt_params(Sock, #prepared_stmt{param_count=0}=Stmt) ->
    recv_prepared_stmt_cols(Sock, finalize_stmt_params(Stmt));
recv_prepared_stmt_params(Sock, Stmt) ->
    Packet = recv_decoded_packet(Sock),
    handle_prepared_stmt_param(Packet, Sock, Stmt).

handle_prepared_stmt_param({_Seq, #raw_packet{data=Data}}, Sock, Stmt) ->
    ColDef = mysql_protocol:decode_column_definition(Data),
    Packet = recv_decoded_packet(Sock),
    handle_prepared_stmt_param(Packet, Sock, add_stmt_param(ColDef, Stmt));
handle_prepared_stmt_param({_Seq, #eof_packet{}}, Sock, Stmt) ->
    recv_prepared_stmt_cols(Sock, finalize_stmt_params(Stmt)).

add_stmt_param(Param, #prepared_stmt{params=Params}=Stmt) ->
    Stmt#prepared_stmt{params=[Param|Params]}.

finalize_stmt_params(#prepared_stmt{params=Params}=Stmt) ->
    Stmt#prepared_stmt{params=lists:reverse(Params)}.

recv_prepared_stmt_cols(_Sock, #prepared_stmt{column_count=0}=Stmt) ->
    Stmt;
recv_prepared_stmt_cols(Sock, Stmt) ->
    Packet = recv_decoded_packet(Sock),
    handle_prepared_stmt_col(Packet, Sock, Stmt).

handle_prepared_stmt_col({_Seq, #raw_packet{data=Data}}, Sock, Stmt) ->
    ColDef = mysql_protocol:decode_column_definition(Data),
    Packet = recv_decoded_packet(Sock),
    handle_prepared_stmt_col(Packet, Sock, add_stmt_col(ColDef, Stmt));
handle_prepared_stmt_col({_Seq, #eof_packet{}}, _Sock, Stmt) ->
    finalize_stmt_cols(Stmt).

add_stmt_col(Col, #prepared_stmt{columns=Cols}=Stmt) ->
    Stmt#prepared_stmt{columns=[Col|Cols]}.

finalize_stmt_cols(#prepared_stmt{columns=Cols}=Stmt) ->
    Stmt#prepared_stmt{columns=lists:reverse(Cols)}.

%% ===================================================================
%% Execute statement
%% ===================================================================

execute_statement(#mysql{sock=Sock}, Stmt, Values) ->
    %% TODO: We'd need to set a flag for the execute command to enable
    %% cursor/row-base retrieval.
    send_packet(Sock, 0, mysql_protocol:com_stmt_execute(Stmt, Values)),
    recv_execute_resp(Sock).

recv_execute_resp(Sock) ->
    Packet = recv_decoded_packet(Sock),
    handle_execute_resp_first_packet(Packet, Sock).

handle_execute_resp_first_packet({1, #ok_packet{}=OK}, _Sock) ->
    OK;
handle_execute_resp_first_packet({1, #resultset{}=RS}, Sock) ->
    recv_execute_resp_resultset(Sock, RS);
handle_execute_resp_first_packet({1, #err_packet{}=Err}, _Sock) ->
    Err.

recv_execute_resp_resultset(Sock, RS) ->
    %% TODO: If we're using cursors, we must skip the rows part.
    Parts =
        [fun recv_query_resp_columns/2,
         fun recv_execute_resp_rows/2],
    apply_recv_parts(Parts, Sock, RS).

%% ===================================================================
%% Receive binary resulset rows
%% ===================================================================

recv_execute_resp_rows(Sock, RS) ->
    %% MySQL uses the same header for the binary resultset row as
    %% they do for OK, so there's no way to reuse generic decoding
    %% here. We need to work with the raw packets. This needs to
    %% land in mysql_protocol.
    %%
    %% TODO: replace with mysql_protocol:decode_xxx_packet, which
    %% would apply the correct decoding for the xxx context (xxx
    %% in this case would be something like binary_resulset_row).
    %%
    Packet = recv_packet(Sock),
    handle_execute_resp_row(Packet, Sock, RS).

handle_execute_resp_row({_Seq, <<16#fe, _:16, _:16>>}, _Sock, RS) ->
    % eof header
    finalize_rows(RS);
handle_execute_resp_row({_Seq, <<0, Data/binary>>}, Sock,
                        #resultset{columns=Cols}=RS) ->
    Row = mysql_protocol:decode_binary_resultset_row(Cols, Data),
    Packet = recv_packet(Sock),
    handle_execute_resp_row(Packet, Sock, add_row(Row, RS)).

%% ===================================================================
%% Close statement
%% ===================================================================

close_statement(#mysql{sock=Sock}, #prepared_stmt{stmt_id=Id}) ->
    send_packet(Sock, 0, mysql_protocol:com_stmt_close(Id)).

%% ===================================================================
%% Command wrappers
%% ===================================================================

ping(#mysql{sock=Sock}) ->
    send_packet(Sock, 0, mysql_protocol:com_ping()),
    {1, Resp} = decode_packet(recv_packet(Sock)),
    Resp.
