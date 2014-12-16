-module(mysql_util).

-export([option/3]).

option(Name, Options, Default) ->
    case lists:keyfind(Name, 1, Options) of
        {_, Value} -> Value;
        false -> Default
    end.
