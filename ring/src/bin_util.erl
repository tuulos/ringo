-module(bin_util).
-export([member64/2, to_list64/1]).

member64(Bin, Val) when is_binary(Bin), is_integer(Val) ->
        member64(Bin, <<Val:64>>);

member64(Bin, Val) when is_binary(Bin), is_binary(Val) ->
        member64_1(Bin, Val).

member64_1(<<>>, _) -> false;
member64_1(<<X:8/binary, _/binary>>, Val) when X == Val -> true;
member64_1(<<_:8/binary, R/binary>>, Val) -> member64_1(R, Val).

to_list64(B) -> to_list64(B, []).
to_list64(<<X:64,R/binary>>, Res) -> to_list64(R, [X|Res]);
to_list64(<<>>, Res) -> Res.

