-module(bin_util).
-export([member64/2, to_list64/1, encode_kvsegment/1, decode_kvsegment/1, bits/1, pad/1]).
-export([find_kv/2]).

member64(Bin, Val) when is_binary(Bin), is_integer(Val) ->
        member64(Bin, <<Val:64>>);

member64(Bin, Val) when is_binary(Bin), is_binary(Val) ->
        member64_1(Bin, Val).

member64_1(<<>>, _) -> false;
member64_1(<<X:8/binary, _/binary>>, Val) when X == Val -> true;
member64_1(<<_:8/binary, R/binary>>, Val) -> member64_1(R, Val).

to_list64(B) -> to_list64(B, []).
to_list64(<<X:8/binary, R/binary>>, Res) -> to_list64(R, [X|Res]);
to_list64(<<>>, Res) -> Res.

encode_kvsegment([]) -> <<>>;
encode_kvsegment(L) ->
        B = bits(lists:max([V || {_, V} <- L])),
        D = << <<K:32, V:B>> || {K, V} <- L >>, 
        pad(<<(length(L)):32, B:5, D/bits>>).

decode_kvsegment(<<>>) -> [];
decode_kvsegment(Seg) ->
        <<N:32, B:5, X/bits>> = Seg,
        S = N * (32 + B),
        <<D:S/bits, _/bits>> = X,
        [{K, V} || <<K:32, V:B>> <= D].

find_kv(_, <<>>) -> none;
find_kv(Key, Seg) ->
        <<N:32, B:5, X/bits>> = Seg,
        S = N * (32 + B),
        <<D:S/bits, _/bits>> = X,
        %io:fwrite("Dim ~b~n", [Key]),
        choose(D, Key, middle(D, 32 + B), 32 + B).

middle(<<>>, _) -> none;
middle(Seg, ItemSize) ->
        N = bit_size(Seg) div ItemSize,
        P = (N div 2) * ItemSize,
        <<_:P/bits, Middle:32, _/bits>> = Seg,
        %io:fwrite("Midle ~b ~b~n", [P,  Middle]),
        {P, Middle}.

choose(<<K:32, Value/bits>> = Seg, Key, _, ItemSize)
        when bit_size(Seg) == ItemSize, K == Key ->
        %io:fwrite("Last~n", []),
        S = ItemSize - 32,
        <<V:S>> = Value,
        {Key, V};

choose(Seg, _, _, ItemSize) when bit_size(Seg) == ItemSize -> 
        %io:fwrite("Last - not found~n", []),
        none;

choose(Seg, Key, {P, Middle}, ItemSize) when Key > Middle ->
        %io:fwrite("Key larger: ~b > ~b~n", [Key, Middle]),
        PP = P + ItemSize,
        <<_:PP, NSeg/bits>> = Seg,
        choose(NSeg, Key, middle(NSeg, ItemSize), ItemSize);

choose(Seg, Key, {P, Middle}, ItemSize) when Key < Middle ->
        %io:fwrite("Key smaller: ~b < ~b~n", [Key, Middle]),
        %PP = P + ItemSize,
        <<NSeg:P/bits, _/bits>> = Seg,
        choose(NSeg, Key, middle(NSeg, ItemSize), ItemSize);

choose(Seg, Key, {P, _}, ItemSize) ->
        %io:fwrite("Match: ~b~n", [Key]),
        <<_:P/bits, Item:ItemSize/bits, _/bits>> = Seg,
        S = ItemSize - 32,
        <<Key:32, Value:S>> = Item,
        {Key, Value};

choose(<<>>, _, _, _) -> none.

pad(X) when is_binary(X) -> X;
pad(X) ->
        P = 8 - (bit_size(X) rem 8),
        <<X/bits, 0:P>>.

bits(X) -> bits(X, 0).
bits(0, N) -> N;
bits(X, N) -> bits(X bsr 1, N + 1).
