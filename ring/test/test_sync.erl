-module(test_sync).
-export([basic_tree_test/1, idlist_test/0, diff_test/1]).

-include("ringo_store.hrl").

basic_tree_test(Entries) when is_list(Entries) ->
        basic_tree_test(list_to_integer(lists:flatten(Entries)));

basic_tree_test(Entries) ->
        {ok, DB} = file:open("test_data/syncdata", [append, raw]),
        Domain = #domain{home = "test_data", db = DB, z = zlib:open()},
        lists:foreach(fun(_) ->
                EntryID = random:uniform(4294967295),
                ringo_writer:add_entry(Domain, EntryID, <<>>, <<>>, [])
        end, lists:seq(1, Entries)),

        S = now(),
        {LeafHashes, LeafLists} = ringo_sync:make_leaf_hashes_and_ids(
                "test_data/syncdata"),
        io:fwrite("Making ~b leaves and ids lists for ~b entries took ~bms~n",
                [ets:info(LeafHashes, size), Entries, 
                        round(timer:now_diff(now(), S) / 1000)]),

        Slots = lists:sort([round(size(V) / 8) ||
                {_, V} <- dict:to_list(LeafLists)]),
        io:fwrite("Min slot ~b entries~n", [lists:min(Slots)]),
        io:fwrite("Median slot ~b entries~n",
                [lists:nth(round(length(Slots) / 2), Slots)]),
        io:fwrite("Max slot ~b entries~n", [lists:max(Slots)]),

        Sum = lists:sum(Slots),
        if Sum == Entries ->
                io:fwrite("~b Entry IDs in total (ok)~n", [Sum]);
        true ->
                io:fwrite("~b Entry IDs in total (should be ~b)~n",
                        [Sum, Entries]),
                halt()
        end,

        S1 = now(),
        Tree = ringo_sync:build_merkle_tree(LeafHashes),
        lists:foldl(fun
                (L, N) when length(L) == N ->
                        N * 2;
                (L, N) ->
                        io:fwrite("Broken tree. Expected ~b nodes, got ~b",
                                [length(L), N]),
                        halt()
        end, 1, Tree),

        io:fwrite("Building a Merkle tree took ~bms~n",
                [round(timer:now_diff(now(), S1) / 1000)]),
        halt().


idlist_test() ->
        {_, LeafIDs} = ringo_sync:make_leaf_hashes_and_ids(
                "test_data/syncdata"),
        S1 = now(),
        N = ringo_reader:fold(fun(_, _, _, {Time, EntryID}, _, N) ->
                {Leaf, SyncID} = ringo_sync:sync_id(EntryID, Time),
                V = ringo_sync:in_leaves(LeafIDs, SyncID),
                if V -> N + 1;
                true ->
                        io:fwrite("~b. entry missing (id ~b, leaf ~b)~n",
                                [N, EntryID, Leaf]),
                        halt()
                end
        end, 0, "test_data/syncdata"),
        io:fwrite("~b IDs validated ok in ~bms~n", [N,
                round(timer:now_diff(now(), S1) / 1000)]),
        halt().

diff_test(Entries) when is_list(Entries) ->
        diff_test(list_to_integer(lists:flatten(Entries)));

diff_test(Entries) ->
        Z = zlib:open(),
        
        {CLeaves, SyncIDs} = lists:unzip(lists:map(fun(_) ->
                EntryID = random:uniform(4294967295),
                ringo_sync:sync_id(EntryID, 0)
        end, lists:seq(1, Entries))),
        ChangedLeaves = lists:usort(CLeaves),

        {LeafHashes, _} = ringo_sync:make_leaf_hashes_and_ids(
                "test_data/syncdata"),
        [[RootA]|_] = TreeA = ringo_sync:build_merkle_tree(LeafHashes),
        [] = merkle_diff(1, [{1, RootA}], TreeA, TreeA),
        io:fwrite("Identical trees diff test passed.~n", []),
        
        [ringo_sync:update_leaf_hashes(Z, LeafHashes, ID) || ID <- SyncIDs],
        [[RootB]|_] = TreeB = ringo_sync:build_merkle_tree(LeafHashes),

        S1 = now(),
        Diff2 = merkle_diff(1, [{1, RootA}], TreeA, TreeB),
        S2 = now(),
        Diff3 = merkle_diff(1, [{1, RootB}], TreeB, TreeA),

        if Diff2 == Diff3 ->
                io:fwrite("Symmetricity test passed.~n");
        true ->
                io:fwrite("Symmetricity test FAILS!~n"),
                halt()
        end,

        Matches = [X - 1 || X <- Diff2] -- ChangedLeaves,
        if Matches == [] ->
                io:fwrite(
                "Changes in ~b entries were detected correctly in ~bms.~n",
                        [Entries, round(timer:now_diff(S2, S1) / 1000)]);
        true ->
                io:fwrite("Merkle_diff FAILS!~n"),
                io:fwrite("Found changes in ~w. Actually ~w were changed.",
                        [Diff2, ChangedLeaves])
        end,
        halt().


merkle_diff(2, [], _, _) -> [];
merkle_diff(H, LevelA, TreeA, TreeB) ->
        Diff = ringo_sync:diff_parents(H, LevelA, TreeB),
        if H < length(TreeA) ->
                Children = ringo_sync:pick_children(H + 1, Diff, TreeA),
                merkle_diff(H + 1, Children, TreeA, TreeB);
        true ->
                Diff
        end.




                        




        


