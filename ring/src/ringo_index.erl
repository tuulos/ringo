-module(ringo_index).
-export([build_index/1, new_dex/0, add_item/3, serialize/1, find_key/2]).

% overwrite flag: index only the newest instance of this key

build_index(DBName) ->
        ringo_reader:fold(
        fun(Key, _, _Flags, _, _, Dex, Pos) ->
                add_item(Dex, Key, Pos)
        end, new_dex(), DBName, true).

new_dex() -> gb_trees:empty().

add_item(Dex, Key, Pos) ->
        <<Hash:32, _/binary>> = erlang:md5(Key),
        gb_trees:enter(Hash, add_pos(gb_trees:lookup(Hash, Dex), Pos), Dex).

add_pos(none, Pos) ->
        {Pos, <<Pos:32>>};

add_pos({value, {PrevPos, Lst}}, Pos) ->
        {Pos, <<Lst/bits, (elias_encode(Pos - PrevPos))/bits>>}.

serialize(Dex) ->
        L = gb_trees:to_list(Dex),
        {Single, Multi} = lists:partition(fun
                ({_, {_, <<_:32>>}}) -> true;
                (_) -> false
        end, L),
        SingleSeg = bin_util:encode_kvsegment(
                [{K, V} || {K, {_, <<V:32>>}} <- Single]),
        {KeysOffs, _} = lists:mapfoldl(fun({K, {_, V}}, Offs) ->
                VA = <<V/bits, (elias_encode(1))/bits>>,
                {{{K, Offs}, VA}, Offs + bit_size(VA)}
        end, 0, Multi),
        {MKeys, MOffs} = lists:unzip(KeysOffs),
        MultiSeg = bin_util:encode_kvsegment(MKeys),
        Offsets = bin_util:pad(list_to_bitstring(MOffs)),
        [<<(size(SingleSeg)):32, (size(MultiSeg)):32, (size(Offsets)):32>>,
                SingleSeg, MultiSeg, Offsets].

find_key(Key, SDex) ->
        <<SingleSegSize:32, MultiSegSize:32, OffsetSize:32,
                SingleSeg:SingleSegSize/binary,
                MultiSeg:MultiSegSize/binary,
                Offsets:OffsetSize/binary>> = SDex,
        
        R = bin_util:find_kv(Key, SingleSeg),
        if R == none ->
                R2 = bin_util:find_kv(Key, MultiSeg),
                if R2 == none -> none;
                true ->
                        {K, Offs} = R2,
                        <<_:Offs/bits, V/bits>> = Offsets,
                        {K, V}
                end;
        true ->
                R
        end.
        
elias_encode(0) -> throw(zero_elias);
elias_encode(X) -> <<X:(2 * bin_util:bits(X) - 1)>>.

elias_decode(B) -> elias_decode(B, 1).
elias_decode(B, N) ->
        case B of
                <<1:N, _/bits>> ->
                        M = N - 1,
                        <<_:M, X:N, R/bits>> = B,
                        {X, R};
                _ -> elias_decode(B, N + 1)
        end.




