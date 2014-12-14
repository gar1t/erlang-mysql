-module(mysql_util).

-export([option/3, dberr/1]).

-include("dbapi.hrl").
-include("mysql_internal.hrl").

option(Name, Options, Default) ->
    case lists:keyfind(Name, 1, Options) of
        {_, Value} -> Value;
        false -> Default
    end.

dberr(#err_packet{code=Code, sqlstate=SqlState, msg=Msg}) ->
    #dberr{sqlstate=SqlState, native=Code, msg=Msg}.
