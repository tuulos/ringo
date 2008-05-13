-module(ringo_index).
-export([build_index/3, fetch_entry/4, new_dex/0, add_item/3, serialize/1]).
-export([deserialize/1, dexhash/1, find_key/2, find_key/3, decode_poslist/1]).

-include("ringo_store.hrl").

% overwrite flag: index only the newest instance of this key

build_index(DBName, StartPos, Num) ->
        ringo_reader:fold(fun
                (_, _, _, _, _, {N, _, _} = A, _) when N == Num ->
                        throw({eof, A});
                (Key, _, _Flags, _, Entry, {N, Dex, _}, Pos) ->
                        {N + 1, add_item(Dex, Key, Pos), Pos + size(Entry)}
        end, {0, new_dex(), 0}, DBName, true, StartPos).

fetch_entry(DB, Home, Key, Offset) ->
        case ringo_reader:read_entry(DB, Offset) of
                % key matches -- a valid external entry
                {Time, _, Flags, Key, V, _} when ?FLAG_UP(Flags, ?EXT_FLAG) ->
                        case ringo_reader:read_external(Home, V) of
                                {ok, Value} -> {Time, Key, Value};
                                _ -> invalid_entry
                        end;
                % key matches -- a valid internal entry
                {Time, _, _, Key, Value, _} -> {Time, Key, Value};
                % entry corrupted -- should not happen 
                invalid_entry -> invalid_entry;
                % hash collision -- ignore this entry
                _ -> ignore
        end.

new_dex() -> gb_trees:empty().

dexhash(Key) ->
        <<Hash:32, _/binary>> = erlang:md5(Key), Hash.

add_item(Dex, Key, Pos) ->
        Hash = dexhash(Key),
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
                % We mark end of the offset list with an elias-encoded value 1.
                % This is safe, since real offsets are always at least 40 bytes,
                % because of entry headers etc.
                VA = <<V/bits, (elias_encode(1))/bits>>,
                {{{K, Offs}, VA}, Offs + bit_size(VA)}
        end, 0, Multi),
        {MKeys, MOffs} = lists:unzip(KeysOffs),
        MultiSeg = bin_util:encode_kvsegment(MKeys),
        Offsets = bin_util:pad(list_to_bitstring(MOffs)),
        [<<(size(SingleSeg)):32, (size(MultiSeg)):32, (size(Offsets)):32>>,
                SingleSeg, MultiSeg, Offsets].

deserialize(Dex) ->
        <<SingleSegSize:32, MultiSegSize:32, OffsetSize:32,
                SingleSeg:SingleSegSize/binary,
                MultiSeg:MultiSegSize/binary,
                Offsets:OffsetSize/binary>> = Dex,
        {SingleSeg, MultiSeg, Offsets}.

find_key(Key, Dex) -> find_key(Key, Dex, true).

find_key(Key, Dex, DoDecode) when is_binary(Key) ->
        find_key(dexhash(Key), Dex, DoDecode);

% non-serialized index. NB: Make sure that when-clause matches with the actual
% data structure used by the active index -- gb_trees is a tuple.
find_key(Key, Dex, DoDecode) when is_tuple(Dex) ->
        case gb_trees:lookup(Key, Dex) of
                none -> {Key, []};
                {value, {_, Lst}} when DoDecode == true ->
                        {Key, decode_poslist(Lst)};
                {value, {_, Lst}} -> {Key, Lst}
        end;

% serialized index
find_key(Key, Dex, DoDecode) when is_binary(Dex) ->
        <<SingleSegSize:32, MultiSegSize:32, OffsetSize:32,
                SingleSeg:SingleSegSize/binary,
                MultiSeg:MultiSegSize/binary,
                Offsets:OffsetSize/binary>> = Dex,
        
        {Key, R} = bin_util:find_kv(Key, SingleSeg),
        if R == none ->
                {Key, Offs} = bin_util:find_kv(Key, MultiSeg),
                if Offs == none -> {Key, []};
                DoDecode == true ->
                        <<_:Offs/bits, V/bits>> = Offsets,
                        {Key, decode_poslist(V)};
                true ->
                        <<_:Offs/bits, V/bits>> = Offsets,
                        {Key, V}
                end;
        DoDecode == true -> {Key, [R]};
        true -> {Key, <<R:32>>}
        end.

decode_poslist(<<P:32, B/bits>>) -> decode_poslist(B, [P]).
decode_poslist(<<>>, L) -> lists:reverse(L);
decode_poslist(B, [P|_] = L) ->
        case elias_decode(B) of
                {1, _} -> lists:reverse(L);
                {D, Rest} -> decode_poslist(Rest, [P + D|L])
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




