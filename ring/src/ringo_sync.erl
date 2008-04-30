-module(ringo_sync).

-export([make_leaf_hashes_and_ids/1, make_leaf_hashes/1,
         make_leaf_hashes/3, build_merkle_tree/1,
         sync_id/2, sync_id_slot/1, update_leaf_ids/2, update_leaf_hashes/2,
         collect_leaves/2, in_leaves/2, diff_parents/3, count_entries/1,
         pick_children/3]).

-include("ringo_store.hrl").
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
make_leaf_hashes_and_ids(empty) ->
        LeafIDs = dict:from_list(
                [{I, <<>>} || I <- lists:seq(0, ?NUM_MERKLE_LEAVES - 1)]),
        LeafHashes = ets:new(leaves, []),
        ets:insert(LeafHashes,
                [{I, 0} || I <- lists:seq(0, ?NUM_MERKLE_LEAVES - 1)]),
        {LeafHashes, LeafIDs};

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
        LeafHashes = ets:new(leaves, []),
        % create the leaves
        ets:insert(LeafHashes,
                [{I, 0} || I <- lists:seq(0, ?NUM_MERKLE_LEAVES - 1)]),
        AccF = ringo_reader:fold(fun(_, _, _, {Time, EntryID}, _, Acc) ->
                {Leaf, SyncID} = sync_id(EntryID, Time),
                update_leaf_hashes(LeafHashes, SyncID),
                if is_function(F) ->
                        F(Leaf, SyncID, Acc);
                true -> ok
                end
        end, Acc0, DBName),
        {LeafHashes, AccF}.

count_entries(LeafIDs) ->
        dict:fold(fun(_Leaf, IDList, N) ->
                N + size(IDList) div 8
        end, 0, LeafIDs).

build_merkle_tree(LeafHashes) ->
        {_, Leaves} = lists:unzip(lists:sort(ets:tab2list(LeafHashes))),
        Tree = make_next_level(Leaves, [Leaves]),
        Tree.

% update_leaf_hashes must implement a commutative (accumulated)
% hashing function. It should be reasonably collision-free. However,
% we can survive a modest number of collisions, since once a leaf gets
% more IDs, its hash changes, and the probability of many consequent
% collisions for the same leaf is hopefully small.
%
% Furthermore, by using only the EntryID part of SyncID for hashing,
% input values are likely to be uniformly distributed, as EntryID is
% defined as follows: EntryID = random:uniform(4294967295).

update_leaf_hashes(LeafHashes, <<_Time:32, EntryID:32>> = SyncID) ->
        Leaf = sync_id_slot(SyncID),
        [{_, P}] = ets:lookup(LeafHashes, Leaf),
        ets:insert(LeafHashes, {Leaf, P bxor EntryID}).

update_leaf_ids(LeafIDs, SyncID) ->
        Leaf = sync_id_slot(SyncID),
        Lst = dict:fetch(Leaf, LeafIDs),
        dict:store(Leaf, <<Lst/binary, SyncID/binary>>, LeafIDs).

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

make_next_level([_], Tree) -> Tree;
make_next_level(Level, Tree) ->
        L = make_level(Level, []),
        make_next_level(L, [L|Tree]).

make_level([], L) -> lists:reverse(L);
make_level([X, Y|R], L) ->
        make_level(R, [erlang:crc32(<<X:32, Y:32>>)|L]).

%%%
%%%
%%%

diff_parents(H, RLevel, OTree) ->
        OLevel = lists:zip(lists:seq(0, 1 bsl (H - 1) - 1),
                lists:nth(H, OTree)),
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
        Level = lists:zip(lists:seq(0, 1 bsl (H - 1) - 1),
                lists:nth(H, Tree)),
        pick(Level, Parents, 0, []).

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
collect_leaves(LeafList, empty) -> [{N, []} || N <- LeafList];
collect_leaves(LeafList, DBName) ->
        LeafBag = ets:new(leaves, [bag]),
        ets:insert(LeafBag, [{N, empty} || N <- LeafList]),
        ringo_reader:fold(fun(_, _, _, {Time, EntryID}, _, _) ->
                {Leaf, SyncID} = sync_id(EntryID, Time),
                case ets:member(LeafBag, Leaf) of
                        true -> ets:insert(LeafBag, {Leaf, SyncID});
                        false -> ok
                end
        end, ok, DBName),
        List = group_results(ets:tab2list(LeafBag)),
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
