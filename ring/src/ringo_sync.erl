-module(ringo_sync).

-export([make_leaf_hashes_and_ids/1, make_leaf_hashes/1,
         make_leaf_hashes/3, build_merkle_tree/1,
         sync_id/2, sync_id_slot/1, update_leaf_ids/2, update_leaf_hashes/3,
         collect_leaves/2, in_leaves/2, diff_parents/3,
         pick_children/3]).

-define(NUM_MERKLE_LEAVES, 8192).

%%%
%%% 
%%%

% IDEA: Instead of binary lists, we could use Bloom filters to save
% the ID lists. False positives don't matter -- we just accidentally
% skip an entry that should have been synced. The trick is that if
% we use _independent_ hashes (changing random salt) on each rsync
% round, probability that the same entry will be skipped on N rounds
% is p^N, which makes it practically certain that the entry will get
% synchronized eventually.
make_leaf_hashes_and_ids(DBName) ->
        LeafIDs = dict:from_list(
                [{I, <<>>} || I <- lists:seq(0, ?NUM_MERKLE_LEAVES - 1)]),
        make_leaf_hashes(DBName, fun(Leaf, SyncID, LLeafIDs) ->
                Lst = dict:fetch(Leaf, LLeafIDs),
                % Binary append should be fast in R12B. This might be REALLY
                % slow on earlier revisions (see Erlang Efficiency Guide).
                dict:store(Leaf, <<Lst/binary, SyncID/binary>>, LLeafIDs)
                % to see that the append-optimization relly works, use the 
                % inserted value for matching here, which should force the
                % binary to be copied. If the function becomes considerably
                % slower this way, we can assume that the optimization works.
        end, LeafIDs).

make_leaf_hashes(DBName) ->
        {LeafHashes, _} = make_leaf_hashes(DBName, none, []),
        LeafHashes.

make_leaf_hashes(DBName, F, Acc0) ->
        Z = zlib:open(),
        LeafHashes = ets:new(leaves, []),
        % create the leaves
        ets:insert(LeafHashes,
                [{I, 0} || I <- lists:seq(0, ?NUM_MERKLE_LEAVES - 1)]),
        AccF = ringo_reader:fold(fun(_, _, _, {Time, EntryID}, _, Acc) ->
                {Leaf, SyncID} = sync_id(EntryID, Time),
                update_leaf_hashes(Z, LeafHashes, SyncID),
                if is_function(F) ->
                        F(Leaf, SyncID, Acc);
                true -> ok
                end
        end, Acc0, DBName),
        zlib:close(Z),
        {LeafHashes, AccF}.

build_merkle_tree(LeafHashes) ->
        Z = zlib:open(),
        {_, Leaves} = lists:unzip(lists:sort(ets:tab2list(LeafHashes))),
        Tree = make_next_level(Z, Leaves, [Leaves]),
        zlib:close(Z),
        Tree.

update_leaf_hashes(Z, LeafHashes, SyncID) ->
        Leaf = sync_id_slot(SyncID),
        [{_, P}] = ets:lookup(LeafHashes, Leaf),
        X = zlib:crc32(Z, P, SyncID),
        ets:insert(LeafHashes, {Leaf, X}).

update_leaf_ids(LeafIDs, SyncID) ->
        Leaf = sync_id_slot(SyncID),
        Lst = dict:fetch(Leaf, LeafIDs),
        dict:store(Leaf, <<Lst/binary, SyncID/binary>>).

in_leaves(LeafIDs, SyncID) ->
        Leaf = sync_id_slot(SyncID),
        Lst = dict:fetch(Leaf, LeafIDs),
        bin_util:member64(Lst, SyncID).

sync_id(EntryID, Time) ->
        % SyncID must have lots of entropy in the least significant
        % bits to ensure uniform allocation of the Merkle leaves
        Leaf = EntryID band (?NUM_MERKLE_LEAVES - 1),
        {Leaf, <<Time:32, EntryID:32>>}.

sync_id_slot(<<_Time:32, EntryID:32>>) ->
        EntryID band (?NUM_MERKLE_LEAVES - 1).

make_next_level(_, [_], Tree) -> Tree;
make_next_level(Z, Level, Tree) ->
        L = make_level(Z, Level, []),
        make_next_level(Z, L, [L|Tree]).

make_level(_, [], L) -> lists:reverse(L);
make_level(Z, [X, Y|R], L) ->
        make_level(Z, R, [zlib:crc32(Z, <<X:32, Y:32>>)|L]).

%%%
%%%
%%%

diff_parents(H, RLevel, OTree) ->
        OLevel = lists:zip(lists:seq(1, 1 bsl (H - 1)), lists:nth(H, OTree)),
        %io:fwrite("Rlevel ~w OLevel ~w~n", [RLevel, OLevel]),
        diff(OLevel, RLevel, []).

diff(_, [], Res) ->
        lists:reverse(Res);
diff([{N1, _}|_], [{N2, _}|_], Res) when N1 > N2 ->
        Res;
diff([{N1, _}|R1], [{N2, _}|_] = L2, Res) when N1 < N2 ->
        diff(R1, L2, Res);
diff([{N1, X1}|R1], [{_, X2}|R2], Res) when X1 =/= X2 ->
        diff(R1, R2, [N1|Res]);
diff([_|R1], [_|R2], Res) ->
        diff(R1, R2, Res).

%%%
%%%
%%%

pick_children(H, Parents, Tree) ->
        Level = lists:zip(lists:seq(1, 1 bsl (H - 1)), lists:nth(H, Tree)),
        pick(Level, Parents, 1, []).

pick(_, [], _, Res) ->
        lists:reverse(Res);
pick([X, Y|Level], [P|Parents], N, Res) when N == P ->
        pick(Level, Parents, N + 1, [Y, X|Res]);
pick([_, _|Level], Parents, N, Res) ->
        pick(Level, Parents, N + 1, Res).

%%%
%%%
%%%

collect_leaves([], _) -> [];
collect_leaves(LeafList, DBName) ->
        Z = zlib:open(),
        LeafBag = ets:new(leaves, [bag]),
        ets:insert(LeafBag, [{N, x} || N <- LeafList]),
        ringo_reader:fold(fun(_, _, _, {Time, EntryID}, _, _) ->
                {Leaf, _SyncID} = X = sync_id(EntryID, Time),
                case ets:member(LeafBag, Leaf) of
                        true -> ets:insert(LeafBag, X);
                        false -> ok
                end
        end, DBName, ok),
        List = group_results([X ||
                {_, V} = X <- ets:tab2list(LeafBag), V =/= x]),
        zlib:close(Z),
        ets:delete(LeafBag),
        List.

%%%
%%%
%%%



% This function converts lists of form: [{2, A}, {3, B}, {2, C}]
% to form: [{2, [A, C]}, {3, [B]}] (shamelessly copied from
% disco/handle_job.erl).
group_results(List) -> 
        lists:foldl(fun
                ({PartID, R}, []) ->
                        [{PartID, [R]}];
                ({PartID, R}, [{PrevID, Lst}|Rest]) when PrevID == PartID ->
                        [{PartID, [R|Lst]}|Rest];
                ({PartID, R}, [{PrevID, _}|_] = Q) when PrevID =/= PartID ->
                        [{PartID, [R]}|Q]
        end, [], lists:keysort(1, List)).
