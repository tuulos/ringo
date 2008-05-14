-module(ringo_syncdomain).

-export([resync/2, global_resync/1]).

-include("ringo_store.hrl").

% Premises about resync:
%
% - We can't know for sure on which nodes all replicas of this domain exist. Nodes may
%   have disappeared, new nodes may have been added etc. Hence, we have to go
%   through the whole ring to find the replicas.
%
% - Since the ring may contain any arbitrary collection of nodes at any point of
%   time

%%%
%%% Synchronization 
%%%

% Owner-end in synchronization -- just update the tree
resync(This, owner) ->
        error_logger:info_report({"resync owner"}),
        #domain{dbname = DBName, db = DB, owner = true, stats = Stats} = 
                gen_server:call(This, get_current_state),
        
        ringo_domain:stats_buffer_add(Stats, sync_time, nu),
        
        DBN = if DB == none -> empty; true -> DBName end,

        {[[Root]|_] = Tree, LeafIDsX, _} = update_sync_tree(This, DBN),
        gen_server:cast(This, {update_sync_data, Tree, LeafIDsX}),
        ringo_domain:stats_buffer_add(Stats, synctree_root, Root),
        flush_sync_outbox(This, DBN);

% Replica-end in synchronization -- the active party
resync(This, replica) ->
        error_logger:info_report({"resync replica"}),
        #domain{dbname = DBName, db = DB, id = DomainID,
                owner = false, stats = Stats} 
                        = gen_server:call(This, get_current_state),
        
        ringo_domain:stats_buffer_add(Stats, sync_time, nu),
        
        DBN = if DB == none -> empty; true -> DBName end,

        {[[Root]|_] = Tree, _, NumEntries} = update_sync_tree(This, DBN),
        ringo_domain:stats_buffer_add(Stats, synctree_root, Root),
        {ok, {_, OPid}, Distance} = find_owner(DomainID),
        % BUG: Shouldn't the domain die if find_owner fails? Now it seems
        % that only the resync process dies. Namely, how to make sure that
        % if there're two owners, one of them will surely die.
        DiffLeaves = merkle_sync(OPid, [{0, Root}], 1, Tree),
        ringo_domain:stats_buffer_add(Stats, diff_size, length(DiffLeaves)),
        if DiffLeaves == [] -> ok;
        true ->
                SyncIDs = ringo_sync:collect_leaves(DiffLeaves, DBName),
                gen_server:cast(OPid,
                        {sync_pack, This, SyncIDs, NumEntries, Distance})
        end,
        flush_sync_outbox(This, DBN).

% merkle_sync compares the replica's Merkle tree to the owner's

% Trees are in sync
merkle_sync(_OPid, [], 2, _Tree) -> [];

merkle_sync(OPid, Level, H, Tree) ->
        {ok, Diff} = gen_server:call(OPid, {sync_tree, H, Level}),
        if H < length(Tree) ->
                merkle_sync(OPid, ringo_sync:pick_children(H + 1, Diff, Tree),
                        H + 1, Tree);
        true ->
                Diff
        end.

% update_sync_tree scans entries in this DB, collects all entry IDs and
% computes the leaf hashes. Entries in the inbox that don't exist in the DB
% already are written to disk. Finally Merkle tree is re-built.

update_sync_tree(This, DBName) ->
        error_logger:info_report({"update sync tree", DBName}),
        {ok, Inbox} = gen_server:call(This, {flush_syncbox, sync_inbox}),
        %error_logger:info_report({"INBOX", Inbox}),
        {LeafHashes, LeafIDs} = ringo_sync:make_leaf_hashes_and_ids(DBName),
        NumEntries = ringo_sync:count_entries(LeafIDs),
        gen_server:cast(This, {update_num_entries, NumEntries}),
        %error_logger:info_report({"LeafIDs", LeafIDs}),
        Entries = lists:filter(fun({SyncID, _, _}) ->
                not ringo_sync:in_leaves(LeafIDs, SyncID)
        end, Inbox),
        {LeafHashesX, LeafIDsX} = flush_sync_inbox(This, Entries,
                LeafHashes, LeafIDs),
        Tree = ringo_sync:build_merkle_tree(LeafHashesX),
        ets:delete(LeafHashesX),
        {Tree, LeafIDsX, NumEntries}.

% flush_sync_inbox writes entries that have been sent to this replica
% (or owner) to disk and updates the leaf hashes accordingly
flush_sync_inbox(_, [], LeafHashes, LeafIDs) ->
        %error_logger:info_report({"flush inbox (empty)"}),
        {LeafHashes, LeafIDs};

flush_sync_inbox(This, [{SyncID, Entry, From}|Rest], LeafHashes, LeafIDs) ->
        %error_logger:info_report({"flush inbox"}),
        ringo_sync:update_leaf_hashes(LeafHashes, SyncID),
        LeafIDsX = ringo_sync:update_leaf_ids(LeafIDs, SyncID),
        gen_server:cast(This, {sync_write_entry, Entry, From}),
        flush_sync_inbox(This, Rest, LeafHashes, LeafIDsX).

% flush_sync_outbox sends entries that exists on this replica or owner to
% another node that requested the entry
flush_sync_outbox(_, empty) ->
        %error_logger:info_report({"flush outbox (empty)"}),
        ok;

flush_sync_outbox(This, DBName) ->
        %error_logger:info_report({"flush outbox"}),
        {ok, Outbox} = gen_server:call(This, {flush_syncbox, sync_outbox}),
        flush_sync_outbox_1(This, DBName, Outbox).

flush_sync_outbox_1(_, _, []) -> ok;
flush_sync_outbox_1(This, DBName, Outbox) ->
        Q = ets:new(x, [bag]),
        ets:insert(Q, Outbox),
        ringo_reader:fold(fun(_, _, _, {Time, EntryID}, Entry, _) ->
                {_, SyncID} = ringo_sync:sync_id(EntryID, Time),
                case ets:lookup(Q, SyncID) of
                        [] -> ok;
                        L -> Msg = {sync_put, SyncID, {Entry, {}}, This},
                             [gen_server:cast(To, Msg) || {_, To} <- L]
                end
        end, ok, DBName),
        ets:delete(Q).

%%%
%%% Global resync
%%% 
%%% In the normal case N replica nodes that precede the domain owner
%%% are responsible for re-syncing themselves with the owner. However,
%%% if the ring goes through a major re-organization, there may be
%%% replicas for this domain further away in the ring as well.
%%%
%%% Global_resync is activated periodically by the domain owner. It
%%% initiates the find_owner operation, that finds the owner node for
%%% the given DomainID. Since find_owner is started by the owner
%%% itself, it has to circulate through the whole ring before reaching
%%% back to this node.
%%%
%%% On its way, it instantiates a domain server for this domain on every
%%% node. Servers on nodes that don't contain a replica of this domain
%%% die away immediately. The others stay alive and start the normal
%%% re_sync process (above) which eventually connects to the owner and
%%% goes through the standard synchronization procedure.
%%%
%%% If a distant replica contains entries that are missing from the
%%% owner, they are sent to it as usual. Otherwise, sync_pack-handler
%%% recognizes the distant replica and kills it.
%%%
global_resync(DomainID) ->
        error_logger:info_report({"global resync"}),
        find_owner(DomainID).

find_owner(DomainID) ->
        {ok, _, Next} = gen_server:call(ringo_node, get_neighbors),
        gen_server:cast({ringo_node, Next},
                {{domain, DomainID}, {find_owner, self(), 1}}),
        receive
                {owner, DomainID, Owner, Distance} ->
                        {ok, Owner, Distance}
        after 10000 ->
                error_logger:warning_report({
                        "Replica resync couldn't find the owner for domain",
                                DomainID}),
                timeout
        end.        

