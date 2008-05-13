-module(ringo_debug).
-export([dump_ids/1, dump_merkle/1, dump_iblock/1]).

dump_ids(DBName) ->
        io:fwrite("# Date Time SyncID EntryID Leaf~n"),
        ringo_reader:fold(fun(_, _, _, {Time, EntryID}, _, N) ->
                Tstamp = ringo_util:format_timestamp(
                        {Time div 1000000, Time rem 1000000, 0}),
                {Leaf, <<SyncID:64>>} = ringo_sync:sync_id(EntryID, Time),
                io:fwrite("~b ~s ~.16B ~.16B ~b~n",
                        [N, Tstamp, SyncID, EntryID, Leaf]),
                N + 1
        end, 0, DBName).

dump_merkle(DBName) ->
        {LeafHashes, LeafIDs} = ringo_sync:make_leaf_hashes_and_ids(DBName),
        Tree = ringo_sync:build_merkle_tree(LeafHashes),
        print_level(Tree, 0),
        dict:fold(fun(Leaf, List, _) ->
                io:fwrite("L ~b > ", [Leaf]),
                print_leaves(List)
        end, 0, LeafIDs).

dump_iblock(IBName) ->
        {ok, Dex} = ringo_reader:read_file(IBName),
        {SingleSeg, MultiSeg, Offsets} = ringo_index:deserialize(Dex),
        lists:foreach(fun({Key, Offs}) ->
                io:fwrite("~b ~b~n", [Key, Offs])
        end, bin_util:decode_kvsegment(SingleSeg)),
        lists:foreach(fun({Key, Offs}) ->
                <<_:Offs/bits, V/bits>> = Offsets,
                io:fwrite("~b ", [Key]),
                lists:foreach(fun(X) ->
                        io:fwrite("~b ", [X])
                end, ring_index:decode_poslist(V)),
                io:fwrite("~n")
        end, bin_util:decode_kvsegment(MultiSeg)).

print_leaves(List) ->
        lists:map(fun(<<X:64>>) ->
                io:fwrite("~b ", [X])
        end, bin_util:to_list64(List)),
        io:fwrite("~n").

print_level([], _) -> ok;
print_level([Level|Rest], H) ->
        lists:foldl(fun(E, N) ->
                io:fwrite("T ~b ~b ~b~n", [H, N, E]),
                N + 1
        end, 0, Level),
        print_level(Rest, H + 1).
                





