-module(bin_util).
-export([member32/2, to_list32/1]).

member32(Bin, Val) when is_binary(Bin), is_integer(Val) ->
        find32(Bin, <<Val:32>>);

member32(Bin, Val) when is_binary(Bin), is_binary(Val) ->
        member32_1(Bin, Val).

member32_1(<<>>, _) -> false;
member32_1(<<X:4/binary, _/binary>>, Val) when X == Val -> true;
member32_1(<<_:4/binary, R/binary>>, Val) -> find32_1(R, Val).

to_list32(B) -> to_list32(B, []).
to_list32(<<X:32,R/binary>>, Res) -> to_list32(R, [X|Res]);
to_list32(<<>>, Res) -> Res.

