
-module(ringo_store).
-behaviour(gen_server).

-export([start/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, 
        terminate/2, code_change/3]).

-define(?KEY_MAX, 4096).
-define(?VAL_INTERNAL_MAX, 4096). 
-define(?DOMAIN_CHUNK_MAX, 104857600). % 100MB
-define(?RDONLY, 8#00400).
-define(?SYNC_BUF_MAX_WORDS, 1000000).

% replication
-define(?NREPLICAS, 3).
-define(?MAX_TRIES, 3).
-define(?REPL_TIMEOUT, 2000).

-include_lib("kernel/include/file.hrl").
-record(domain, {this, owner home, host, id, z, db, size,
        sync_tree, sync_ids, sync_req}).

start(Home, DomainID) ->
        case gen_server:start(ringo_domain, 
                [Home, DomainID], [{debug, [trace, log]}]) of
                {ok, Server} -> {ok, Server};
                {error, {already_started, Server}} -> {ok, Server}
        end.

init([Home, DomainID]) ->
        {A1, A2, A3} = now(),
        random:seed(A1, A2, A3),
        ets:new(sync_inbox, [named_table]),
        ets:new(sync_outbox, [named_table, bag]),

        Path = filename:join(Home, "rdomain-" ++ DomainID),
        {ok, #store{this = self(), home = Path, host = net_adm:localhost(),
                    id = DomainID, z = zlib:open(), db = none,
                    sync_tree = none, sync_ids = none, sync_req = []}}.


handle_call({flush_syncbox, Box}, _From, D) ->
        L = ets:tab2list(Box),
        ets:delete_all_objects(Box),
        {reply, {ok, L}, D};

handle_call({update_sync_data, Tree, LeafIDs}, _, #domain{owner = true} = D) ->
        {reply, ok, D#{sync_tree = Tree, sync_ids = LeafIDs}};

handle_call({update_sync_data, Tree, _}, _, D) ->
        {reply, ok, D#{sync_tree = Tree, sync_ids = none}};

handle_call({sync_tree, _, _}, _, #domain{sync_tree = none} = D) ->
        {reply, not_ready, D};

handle_call({sync_tree, H, Level}, _,
        #domain{sync_tree = OTree, owner = true} = D) ->
        
        {reply, {ok, ringo_sync:diff_parents(H, Level, OTree)}, D};

handle_call({new_domain, Name, Chunk, NReplicas}, _From, 
        #domain{home = Path, id = ID} = D) ->
        
        InfoFile = filename:join(Path, "info"),
        ok = file:make_dir(Path),
        ok = file:write_file(InfoFile,
                [integer_to_list(NReplicas), 32, 
                 integer_to_list(Chunk), 32, Name]),
        ok = file:write_file_info(InfoFile, #file_info{mode = ?RDONLY}),
        {reply, ok, open_domain(D)};

handle_call({owner_put, Key, Value} = Req, From, #domain{db = none} = D) ->
        case catch open_domain(D) of
                NewD#domain -> handle_call(A, From, NewD);
                invalid_domain -> {stop, normal, invalid_domain(Req), D};
                closed -> {stop, normal, closed_chunk(Req), D};
        end;

% DB readers must skip any consequent equal EntryIDs in the DB

handle_call({owner_put, Key, Value} = Req, From, #domain{size = Size} = D) ->
        % check that value doesn't exceed DOMAIN_CHUNK_MAX / (MAX_TRIES + 1)
        % worst case: will make MAX_TRIES duplicates of this entry
        New = ringo_writer:encoded_size(Key, Value) * ?MAX_TRIES,
        if Size + New > ?DOMAIN_CHUNK_MAX ->
                {stop, normal, chunk_full(Req), D};
        true ->
                EntryID = random:uniform(4294967295),
                {ok, NewSize} = put_item(Req, EntryID, D),
                {reply, ok, D#domain{size = NewSize}}
        end;

% motto:
% in the worst case data will be lost. Aim at a good average case and try to
% ensure that it is possible to salvage data by hand after a catastrophic
% failure. 
%
% Losing one data item is annoying. Losing all your data is a catastrophy.
% It makes sense to minimize probability of the latter event.
%
% Don't try to make the worst case (catastrophic failure) disappear. Try to
% minimize its effects instead.

% opportunistic replication:
% 1. replicate, wait for replies
% 2. if no replies are received in Q secods, send the request again with
%    duplicate bit on (ring may have changed / healed during the past Q
%    seconds)
% 3. if the step 2 repeats C times, request fails and data is (possibly) lost


handle_call(owner_resync, From, D) ->
        % launch self-check-domain in another process
        % blocking call to ringo_node:
        % request check_domain from the replicas
        % wait for results from replicas
        % - if a replica fails, ignore it
        % - if ringo_node fails, cancel resync (will try again later)
        % wait for self-results
        
% owner multi-calls replicas to fsck
handle_call(check_domain, _From, #domain{db = none} = D) ->
        % launch self-check-domain in another process
        % gen_server:call previous
        % wait for call to finish
        % wait for self to finish
        % return results
        {reply, {ok, 0}, D};

handle_call(check_domain, _From, D) ->
        {ok, N} = ringo_reader:check(D);
        {reply, {ok, N}, D};

handle_cast({sync_external, DstDir}) ->
        error_logger:info_report("launch rsync etc"),
        {noreply, D}.

% - Why sync_inbox is needed:
%
% Sync_put is called by a replica after owner has requested an entry,
% that is missing from the owner's DB, from the replica. The missing
% entry isn't added to the DB immediately as we cannot be sure that the
% entry is _really_ missing from the DB: Since put requests are allowed
% any time _during_ the Merkle tree construction, this entry might have
% just missed the previous tree. Hence, the entry goes to a sync buffer
% to wait for the tree to be updated.
%
% If the new updated tree is still missing this entry, we can safely add
% it to the DB and update the new tree accordingly.
%
% - Why sync_outbox is needed:
%
% Collecting entries from the DB is expensive. Outbox is used to buffer
% requests from several replicas so that they can be served with a single 
% scan over the DB file.
%

handle_cast({sync_put, SyncID, Entry}, {owner = true} = D) ->
        M = ets:info(sync_inbox, memory),
        if M > ?SYNC_BUF_MAX_WORDS ->
                error_logger:warning_report({"Sync buffer full (", M,
                        " words). Entry ignored."});
        true ->
                ets:insert(sync_inbox, {SyncID, Entry})
        end,
        {noreply, D};

handle_cast({sync_get, Owner, Requests}, {owner = false} = D) ->
        ets:insert(sync_outbox, [{SyncID, Owner} || SyncID <- Requests]),
        spawn(fun() -> flush_sync_outbox() end),
        {noreply, D};        

handle_cast({sync_pack, From, Pack},
        #domain{sync_ids = LeafIDs, owner = true} = D) ->

        Requests = lists:foldl(fun({Leaf, RSyncIDs}, RequestList) ->
                
                OSyncIDs = bin_util:to_list32(dict:fetch(Leaf, LeafIDs)),

                SendTo = OSyncIDs -- RSyncIDs,
                if SendTo == [] -> ok;
                true ->
                        ets:insert(sync_outbox, 
                                [{SyncID, From} || SyncID <- SendTo])
                end,

                RequestFrom = RSyncIDs -- OSyncIDs,
                RequestFrom ++ RequestList
        end, [], Pack),
        if Requests == [] -> ok;
        true ->
                gen_server:cast(From, {sync_get, {node(), self()}, Requests})
        end,
        {noreply, D};

% What happens with duplicate SyncIDs?

               

% Replication request
%

% back to the owner
handle_cast({repl_put, _, {_, ONode, _}, _}, D) when ONode == node() ->
        error_logger:warning_report(
                {"A replication request came back to the owner!"}),
        {noreply, D};

% replica on the same physical node as the owner, skip over this node
handle_cast({repl_put, Entry, {OHost, _, _}, N} = R, #domain{host = Host} = D)
        when OHost == Host ->
        
        {ok, Prev} = gen_server:call(ringo_node, get_previous),
        gen_server:cast({ringo_node, Prev}, R),
        {noreply, D}

% domain not open
handle_cast({repl_put, _, {_, ONode, OPid}, _} = R, #domain{db = none} = D) ->
        case catch open_domain(D) of 
                NewD#domain -> handle_cast(R, From, NewD);
                _ -> {ONode, OPid} ! {repl_reply, resync},
                     {noreply, D}
        end;

% normal case
handle_cast({repl_put, Entry, {_, ONode, OPid}, N}, #domain{db = DB} = D) ->
        if N > 1 ->
                {ok, Prev} = gen_server:call(ringo_node, get_previous),
                gen_server:cast({ringo_node, Prev},
                        {repl_put, Entry, Owner, N - 1});
        true -> ok
        end,
        {ok, _, NewSize} = ringo_writer:add_entry(DB, Entry),
        {ONode, OPid} ! {repl_reply, {ok, NewSize}},
        {noreply, D}


% ignore late repl_replies
handle_info({repl_reply, _}, D) -> {noreply, D};

handle_info({nodedown, Node}, R) -> ok.

open_domain(#domain{home = Home} = D) ->
        H = case file:read_file_info(Home) of
                {ok, _} -> open_db(D);
                _ -> throw(invalid_domain)
        end.

open_db(#domain{home = Home} = D) ->
        case file:read_file_info(file:join(Home, "closed")) of
                {ok, _} -> throw(closed);
                {error, _} -> 
                        DataFile = filename:join(Home, "data"),
                        {ok, DB} = file:open(DataFile, [append, raw]),
                        D#domain{db = DB, size = domain_size(Home)}
        end,

domain_size(Home) ->
        [Size, _] = string:tokens(os:cmd(["du -b ", Home, " |tail -1"]), "\t"),
        list_to_integer(Size).

close_domain(DomainID, Home, DB) ->
        ok = file:close(DB),
        DFile = file:join(Home, "data"),
        ok = file:write_file_info(DFile, #file_info{mode = ?RDONLY}),
        CFile = file:join(Home, "closed"),
        ok = file:write_file_info(CFile, #file_info{mode = ?RDONLY}),
        ok = file:write_file(CFile, <<>>), 
        ets:insert(dbs, {DomainID, closed}).

invalid_domain(Request) ->
        % push backwards

closed_chunk(Request) ->
        % forward to the next chunk

chunk_full(Request) ->
        % create a new domain chunk

%
% opportunistic replication: If at least one replica succeeds, we are happy
% (and assume that quite likely more than one have succeeded)
%

put_item(Request, EntryID, #domain{db = DB}) ->
        {ok, Entry, NewSize} = ringo_writer:add_entry(DB, EntryID, Key, Value),

        try_put_item(Entry, EntryID, 0).

try_put_item(_, EntryID, ?MAX_TRIES) ->
        error_logger:warning_report({"Replication failed! EntryID", EntryID}),
        failed.

try_put_item(Entry, EntryID, Tries) ->
        Me = {net_adm:localhost(), node(), self()},
        {ok, Prev} = gen_server:call(ringo_node, get_previous),
        gen_server:cast({ringo_node, Prev},
                {{domain, DomainID}, {repl_put, Entry, Me, ?NREPLICAS}}),
        receive
                {repl_reply, {ok, Size}} ->
                        ok;
                {repl_reply, resync} ->
                        resync(), 
                        try_put_item(Request, EntryID, Tries + 1)
        after ?REPL_TIMEOUT ->
                try_put_item(Request, EntryID, Tries + 1)
        end

% Premises about resync:
%
% - We can't know for sure on which nodes all replicas of this domain exist. Nodes may
%   have disappeared, new nodes may have been added etc. Hence, we have to go
%   through the whole ring to find the replicas.
%
% - Since the ring may contain any arbitrary collection of nodes at any point of
%   time


update_sync_data(Domain, DBName) ->
        % registering ensures that only one updater instance can be 
        % running at the same time
        SyncBuffer = gen_server:call(This, {flush_syncbox, sync_inbox}),
        {LeafHashes, LeafIDs} = ringo_sync:make_leaf_hashes_and_ids(DBName),
        Entries = lists:filter(fun({SyncID, _}) ->
                not ringo_sync:in_leaves(LeafIDs, SyncID)
        end, SyncBuffer),
        {LeafHashesX, LeafIDsX} = sync_entries(Entries, LeafHashes, LeafIDs),
        Tree = ringo_sync:build_merkle_tree(LeafHashesX),
        ets:delete(LeafHashesX),
        gen_server:call(This, {update_sync_data, Tree, LeafIDsX}),
        Tree.

sync_entries([], LeafHashes, LeafIDs) -> {LeafHashes, LeafIDs};

% what if the domain is closed (don't make the DB read-only)
sync_entries([{SyncID, Entry}|Rest], LeafHashes, LeafIDs) ->
        update_leaf_hashes(Z, LeafHashes, SyncID),
        LeafIDsX = update_leaf_ids(Z, LeafIDs, SyncID),
        ringo_writer:add_entry(DB, Entry), 
        sync_entries(Rest, LeafHashes, LeafIDsX).
        
%%%
%%% 
%%%
 
flush_sync_outbox() ->
        {ok, Outbox} = gen_server:call(This, {flush_syncbox, sync_outbox}),
        flush_sync_outbox(Outbox).

flush_sync_outbox([]) -> ok;
flush_sync_outbox(Outbox) ->
        Q = ets:new(x, [bag]),
        ets:insert(Q, Outbox),
        ringo_reader:foreach(fun(_, _, _, {Time, EntryID}, Entry, _) ->
                {_, SyncID} = ringo_sync:sync_id(EntryID, Time),
                case ets:lookup(Q, SyncID) of
                        [] -> ok;
                        L -> Msg = {sync_put, SyncID, Entry},
                             [gen_server:cast(To, Msg) || {_, To} <- L]
                end
        end, DBName, ok),
        ets:delete(Q).


resync(#domain{owner = true} = D) ->
        register(resync, self()),
        update_sync_data(D),
        flush_sync_outbox(),

resync(#domain{this = This, id = DomainID, host = Host, home = Home} = D) ->
        register(resync, self()),
        [Root|Tree] = update_sync_data(D),
        {ok, Owner} = find_owner(DomainID),
        DiffLeaves = merkle_sync({1, Root}, 1, Tree),
        if DiffLeaves == [] -> ok;
        true ->
                SyncIDs = ringo_sync:collect_leaves(DiffLeaves, DBName),
                gen_server:cast(Owner, {sync_pack, SyncIDs, This}),
                gen_server:cast(Owner, {sync_external, Host ++ ":" ++ Home})
        end.
        

% Trees are in sync
merkle_sync(Owner, [], 2, Tree) -> [];

merkle_sync(Owner, Level, H, Tree) ->
        {ok, Diff} = gen_server:call({sync_tree, H, Level}),
        if H < length(Tree) ->
                merkle_sync(Owner, ringo_sync:pick_children(H + 1, Diff, Tree),
                        H + 1, Tree);
        true ->
                Diff
        end.
        
find_owner(DomainID) ->
        {ok, Next} = gen_server:call(ringo_node, get_next),
        gen_server:cast({ringo_node, Next},
                {{domain, DomainID}, {find_owner, {node(), self()}}}),
        receive
                {owner, DomainID, Owner} -> {ok, Owner}
        after 10000 ->
                error_logger:warning_report({
                        "Replica resync couldn't find the owner for domain",
                                DomainID}),
                timeout
        end.



        
        
        


%%% callback stubs


terminate(_Reason, #domain{db = none}) -> {};
terminate(_Reason, #domain{db = DB}) ->
        file:close(DB).

code_change(_OldVsn, State, _Extra) -> {ok, State}.

