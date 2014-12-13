-module(mysql_debug).

-export([hexdump/1]).

hexdump(Data) ->
    Tmp = write_tmp_file(Data),
    spawn_file_cleanup(Tmp),
    os:cmd("hexdump -C " ++ Tmp).

write_tmp_file(Data) ->
    Tmp = mktemp(),
    ok = file:write_file(Tmp, Data),
    Tmp.

mktemp() ->
    strip_lf(os:cmd("mktemp")).    

strip_lf(S) -> lists:sublist(S, length(S) - 1).

spawn_file_cleanup(File) ->
    spawn(fun() -> delay_delete(File) end).

delay_delete(File) ->
    receive
    after 1000 -> file:delete(File)
    end.
