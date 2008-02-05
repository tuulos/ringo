-module(test_sync).
-export([basic_tree_test/1]).

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

