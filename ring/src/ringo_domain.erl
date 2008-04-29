
% TODO:
% 1. Put with replicas works
% 2. Simple syncing works (small items)
% 3. Syncing works (large items)
% 4. New replica node from the scratch 
% 5. New node, owner redirect (invalid domain)
% 6. Simple chunking: close, open a new chunk, redirect request
% 7. Put with a closed chunk
% 8. Syncing a closed chunk


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

% what if the domain is closed (don't make the DB read-only)

% What happens with duplicate SyncIDs? -- Collisions should be practically
% impossible when using 64bit SyncIDs (Time + Random). However, a duplicate
% syncid doesn't break anything, it just prevents re-syncing from succeeding.
%
% Note that duplicate SyncIDs doesn't matter per se. They become an issue
% only if an entry with an already existing SyncID wasn't replicated correctly
% and it must be re-synced afterwards. If it's replicated ok, everything's ok.


-module(ringo_domain).
-behaviour(gen_server).

-export([start/5, resync/2, global_resync/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, 
        terminate/2, code_change/3]).

-include("ringo_store.hrl").
-include("ringo_node.hrl").
-include_lib("kernel/include/file.hrl").

-define(DOMAIN_CHUNK_MAX, 104857600). % 100MB
-define(SYNC_BUF_MAX_WORDS, 1000000).

-define(RESYNC_INTERVAL, 30000). % make this longer!
-define(GLOBAL_RESYNC_INTERVAL, 30000). % make this longer!
-define(STATS_WINDOW_LEN, 5).

% replication
-define(MAX_TRIES, 3).
-define(REPL_TIMEOUT, 2000).

start(Home, DomainID, IsOwner, Prev, Next) ->
        case gen_server:start(ringo_domain, 
                %[Home, DomainID, IsOwner], [{debug, [trace, log]}]) of
                [Home, DomainID, IsOwner, Prev, Next], []) of
                {ok, Server} -> {ok, Server};
                {error, {already_started, Server}} -> {ok, Server}
        end.

init([Home, DomainID, IsOwner, Prev, Next]) ->
        error_logger:info_report({"Domain opens", DomainID, IsOwner}),
        {A1, A2, A3} = now(),
        random:seed(A1, A2, A3),
        Inbox = ets:new(sync_inbox, []),
        Outbox = ets:new(sync_outbox, [bag]),
        Stats = ets:new(stats, [public]),
        erlang:monitor(process, ringo_node),
        Path = filename:join(Home, "rdomain-" ++
                erlang:integer_to_list(DomainID, 16)),

        D0 = #domain{this = self(),
                     owner = IsOwner,
                     home = Path, 
                     dbname = filename:join(Path, "data"),
                     host = net_adm:localhost(),
                     id = DomainID, 
                     z = zlib:open(),
                     db = none,
                     full = false,
                     sync_tree = none, 
                     num_entries = undefined,
                     sync_ids = none,
                     sync_inbox = Inbox,
                     sync_outbox = Outbox,
                     stats = Stats,
                     prevnode = Prev,
                     nextnode = Next
                },

        Domain = case catch open_domain(D0) of
                NewD when is_record(NewD, domain) ->
                        error_logger:info_report({"Existing domain opened"}),
                        NewD;
                Error ->
                        error_logger:info_report({"No domain found", Error}),
                        D0
        end,
        
        ets:insert(Stats, {started, ringo_util:format_timestamp(now())}),

        RTime = round(?RESYNC_INTERVAL +
                        random:uniform(?RESYNC_INTERVAL * 0.5)),
        if IsOwner ->
                {ok, _} = timer:apply_interval(RTime, ringo_domain,
                        resync, [self(), owner]),
                GTime = round(?GLOBAL_RESYNC_INTERVAL + 
                                random:uniform(?GLOBAL_RESYNC_INTERVAL * 0.5)),
                {ok, _} = timer:apply_interval(GTime, ringo_domain,
                                global_resync, [DomainID]);
        true ->
                {ok, _} = timer:apply_interval(RTime, ringo_domain,
                        resync, [self(), replica])
        end,
        {ok, Domain}.

%%%
%%% Sync_tree matches a replica's Merkle tree to the owner's.
%%% Called by a replica in merkle_sync.
%%%

handle_call({sync_tree, _, Level}, _,
        #domain{db = none, sync_tree = none, owner = true} = D) ->
        {reply, {ok, [N || {N, _} <- Level]}, D};

handle_call({sync_tree, _, _}, _,
        #domain{sync_tree = none, owner = true} = D) ->
        {reply, not_ready, D};

handle_call({sync_tree, H, Level}, _,
        #domain{sync_tree = OTree, owner = true} = D) ->        
        {reply, {ok, ringo_sync:diff_parents(H, Level, OTree)}, D};

%%%
%%% For internal use -- used by synchronization processes (resync)
%%%

% Dump inbox or outbox
handle_call({flush_syncbox, Box}, _From, #domain{sync_inbox = Inbox,
        sync_outbox = Outbox, stats = Stats} = D) ->

        Tid = case Box of
                sync_inbox -> Inbox;
                sync_outbox -> Outbox
        end,
        L = ets:tab2list(Tid),
        
        stats_buffer_add(Stats, Box, length(L)),
        ets:delete_all_objects(Tid),
        {reply, {ok, L}, D};

handle_call(get_domaininfo, _From, #domain{info = Info} = D) ->
        error_logger:info_report({"get domaininfo"}),
        {reply, {ok, Info}, D};

handle_call(get_current_state, _From, D) ->
        {reply, D, D}.

%%%
%%% Basic domain operations: Create, put, get 
%%%

handle_cast({new_domain, Name, Chunk, From, Params},
        #domain{id = DomainID} = D) ->

        InfoPack = [{nrepl, proplists:get_value(nrepl, Params)},
                    {chunk, Chunk},
                    {name, list_to_binary(Name)},
                    {id, DomainID}],

        case new_domain(D, InfoPack) of
                {error, eexist} ->
                        From ! {ringo_reply, DomainID, {error, eexist}},
                        {noreply, D};
                {ok, NewD} ->     
                        From ! {ringo_reply, DomainID, {ok, {node(), self()}}},
                        {noreply, NewD}
        end;

% DB not open: There may be two reasons for this:
%
% 1) Domain doesn't exists
% 2) Domain owner has changed recently 
%
% In the former case, put operation should fail. In the latter case, however,
% the operation should succeed. Since this instance doesn't have enough
% information to distinguish between 1 and 2, we must pass this request forward
% in the ring (redir_put). 
%
% In the first case the request will eventually come back to this node and
% it will be dropped. In the second case the request will be processed by the
% previous owner, or a replica, of this domain, which will eventually
% synchronize the entry back to this node.
handle_cast({put, _, _, _, _} = P, #domain{db = none,
        id = DomainID, owner = true, prevnode = Prev} = D) ->
        
        gen_server:cast({ringo_node, Prev}, {{domain, DomainID},
                {redir_put, node(), 1, P}}),
        {noreply, D};

% normal case
handle_cast({put, Key, Value, Flags, From}, #domain{owner = true,
        full = false, id = DomainID, info = InfoPack, prevnode = Prev} = D) ->

        EntryID = random:uniform(4294967295),
        Entry = ringo_writer:make_entry(D, EntryID, Key, Value, Flags),
        From ! {ringo_reply, DomainID, {ok, {node(), EntryID}}},
        
        NewD = do_write(Entry, D),

        Nrepl = proplists:get_value(nrepl, InfoPack),
        DServer = self(),

        % See replicate_proc for different replication policies. Currently
        % performing replication in another process is unsafe, since it 
        % de-serializes the order in which entries hit replicas, which in
        % turn may confuse resyncing process (ringo_reader can only detect
        % duplicate EntryIDs that are consequent).
        
        % Currently the safest approach,
        % in resyncing point of view, is to do opportunistic replication (no
        % re-sends) in a serialized manner (i.e. no spawning).
        replicate({DServer, DomainID, EntryID, Entry, Nrepl}, Prev, 0),

        %spawn(fun() -> replicate_proc(
        %        {DServer, DomainID, EntryID, Entry, Nrepl}, 0)
        %end),
        {noreply, NewD};

% chunk full
handle_cast({put, _, _, _, From},
        #domain{id = DomainID, owner = true, full = true} = D) ->

        From ! {ringo_reply, DomainID, {error, domain_full}}, 
        {noreply, D};

%%%
%%% Redirected put
%%%

% Prevent infinite loops
handle_cast({redir_put, _, N, _}, D) when N > ?MAX_RING_SIZE ->
        {noreply, D};

% Request came back to the owner -> domain doesn't exist
handle_cast({redir_put, Owner, _, {put, _, _, _, From}},
        #domain{id = DomainID} = D) when Owner == node() ->

        From ! {ringo_reply, DomainID, {error, invalid_domain}},
        {noreply, D};

% no domain on this node, forward
handle_cast({redir_put, Owner, N, P},
        #domain{db = none, id = DomainID, prevnode = Prev} = D) ->
        gen_server:cast({ringo_node, Prev}, {{domain, DomainID}, 
                {redir_put, Owner, N + 1, P}}),
        {noreply, D};

% put here, if the domain isn't full
handle_cast({redir_put, Owner, _, {put, Key, Value, Flags, From}},
        #domain{id = DomainID, full = false} = D) ->
        
        EntryID = random:uniform(4294967295),
        Entry = ringo_writer:make_entry(D, EntryID, Key, Value, Flags),
        From ! {ringo_reply, DomainID, {ok, {Owner, EntryID}}},
        {noreply, do_write(Entry, D)};

% domain full, notify the sender
handle_cast({redir_put, _, _, {put, _, _, _, From}},
        #domain{id = DomainID, full = true} = D) ->
        
        From ! {ringo_reply, DomainID, {error, domain_full}},
        {noreply, D};


%%% 
%%% Replication
%%%

% back to the owner
handle_cast({repl_put, _EntryID, _, {_, ONode, _OPid}, _, _}, D)
        when ONode == node() ->
        
        %OPid ! {repl_reply, {ring_too_small, EntryID}},
        {noreply, D};

% replica on the same physical node as the owner, skip over this node
handle_cast({repl_put, _, _, {OHost, _, _}, _, _} = R, #domain{host = Host,
        id = DomainID, prevnode = Prev} = D) when OHost == Host ->
        
        gen_server:cast({ringo_node, Prev}, {{domain, DomainID}, R}),
        {noreply, D};

handle_cast({repl_put, _, _, _, ODomain, _} = R, #domain{db = none} = D) ->
        open_or_clone(R, ODomain, D);

% normal case
handle_cast({repl_put, EntryID, Entry, {_, _, _OPid} = Owner, ODomain, N},
        #domain{db = DB, id = DomainID, prevnode = Prev} = D) ->

        error_logger:info_report({"repl put", EntryID}),
        if N > 1 ->
                gen_server:cast({ringo_node, Prev}, {{domain, DomainID},
                        {repl_put, EntryID, Entry, Owner, ODomain, N - 1}});
        true -> ok
        end,
        % Notify the owner before or after writing? Put operation succeeds
        % faster if the reply is sent before, but then any errors in writing
        % will go unnoticed. On the other hand, write errors would probably
        % repeat with re-sent replicas, so the benefit is not clear.
        %OPid ! {repl_reply, {ok, EntryID}},
        ok = ringo_writer:write_entry(DB, Entry),
        {noreply, D};

%%%
%%% Synchronization
%%%

handle_cast({write_entry, _, From} = Req, #domain{db = none} = D) ->
        open_or_clone(Req, From, D);

handle_cast({write_entry, Entry, _}, D) ->
        {noreply, do_write(Entry, D)};

handle_cast({update_sync_data, Tree, LeafIDs}, #domain{owner = true} = D) ->
        {noreply, D#domain{sync_tree = Tree, sync_ids = LeafIDs}};

handle_cast({sync_put, SyncID, Entry, From},
        #domain{sync_inbox = Inbox} = D) ->
        error_logger:info_report({"sync put", SyncID, From}),

        M = ets:info(Inbox, memory),
        if M > ?SYNC_BUF_MAX_WORDS ->
                error_logger:warning_report({"Sync buffer full (", M,
                        " words). Entry ignored."});
        true ->
                ets:insert(Inbox, {SyncID, Entry, From})
        end,
        {noreply, D};

handle_cast({sync_get, Owner, Requests},
        #domain{owner = false, sync_outbox = Outbox} = D) ->

        ets:insert(Outbox, [{SyncID, Owner} || SyncID <- Requests]),
        %spawn_link(fun() -> 
        %        case catch register(sync_outbox, self()) of
        %                true -> flush_sync_outbox(This, DBName);
        %                _ -> ok
        %        end
        %end),
        {noreply, D};        

handle_cast({sync_external, _DstDir}, D) ->
        error_logger:info_report("launch rsync etc"),
        {noreply, D};

handle_cast({sync_pack, From, Pack, _Distance}, #domain{info = _InfoPack,
        sync_ids = LeafIDs, sync_outbox = Outbox, owner = true} = D) ->

        Requests = lists:foldl(fun({Leaf, RSyncIDs}, RequestList) ->
                if LeafIDs == none -> 
                        OSyncIDs = [];
                true ->
                        OSyncIDs = bin_util:to_list64(
                                dict:fetch(Leaf, LeafIDs))
                end,     

                SendTo = OSyncIDs -- RSyncIDs,
                if SendTo == [] -> ok;
                true ->
                        ets:insert(Outbox,
                                [{SyncID, From} || SyncID <- SendTo])
                end,

                RequestFrom = RSyncIDs -- [empty|OSyncIDs],
                RequestFrom ++ RequestList
        end, [], Pack),

        % -- XXX ---
        % Killing a distant replica: If an inactive domain will
        % kill itself after a timeout, we don't have to kill a distant
        % domain explicitely. Killing it might be a bit problematic anyway
        % since we may not have InfoPack available when we get the only
        % sync packet from it.

        % If this is the first resync for an uninitialized owner, InfoPack
        % is not initialized yet.
        %Nrepl = if InfoPack == undefined -> false;
        %        true -> proplists:get_value(nrepl, InfoPack)
        %        end,
        %error_logger:info_report({"Hep", Requests, Distance, Nrepl}),
        %if Requests == [], Distance > Nrepl ->
        %        error_logger:info_report({"NUF"}),
        %        gen_server:cast(From, {kill_domain, "Distant domain"});
        % --- XXX ---
                
        % NB: A small(?) performance issue: We may request the same IDs from
        % many, possible all, replicas. This is of course unnecessary. Since
        % duplicates will be removed in flush_sync_inbox, current code works
        % correctly.
        if Requests == [] -> ok;
        true ->
                gen_server:cast(From, {sync_get, self(), Requests})
        end,
        {noreply, D};

handle_cast({find_owner, Node, N}, #domain{id = DomainID, owner = true} = D) ->
        error_logger:info_report(
                {"Owner found:", node(), "owns the domain", DomainID}),
        Node ! {owner, DomainID, {node(), self()}, N},
        {noreply, D};

% N < MAX_RING_SIZE prevents theoretical infinite loops
handle_cast({find_owner, Node, N}, #domain{owner = false, id = DomainID,
        db = DB, nextnode = Next} = D) when N < ?MAX_RING_SIZE ->

        gen_server:cast({ringo_node, Next},
                {{domain, DomainID}, {find_owner, Node, N + 1}}),
        
        if DB == none ->
                {stop, normal, D};
        true ->
                {noreply, D}
        end;

handle_cast({get_status, From}, #domain{id = DomainID, size = Size,
        num_entries = NumE, full = Full, owner = Owner, stats = Stats} = D) ->
        From ! {status, node(), 
                [{id, DomainID},
                 {num_entries, NumE},
                 {size, Size},
                 {full, Full},
                 {owner, Owner}] ++ ets:tab2list(Stats)},
        {noreply, D};

handle_cast({kill_domain, Reason}, D) ->
        error_logger:info_report({"Domain killed: ", Reason}),
        {stop, normal, D};

handle_cast({update_num_entries, N}, D) ->
        {noreply, D#domain{num_entries = N}};

handle_cast(Msg, R) ->
        error_logger:info_report({"Unknown cast", Msg}),
        {stop, normal, R}.
        
%%%
%%% Random messages
%%%


%handle_info({repl_domain, Info}, D) ->
%        {ok, NewD} = new_domain(D, Info),
%        {noreply, NewD};


handle_info({'DOWN', _, _, _, _}, R) ->
        error_logger:info_report("Ringo_node down, domain dies too"),
        {stop, normal, R}.


%%%
%%% Domain maintenance
%%%

new_domain(#domain{home = Path} = D, Info) ->
        InfoFile = filename:join(Path, "info"),
        case file:make_dir(Path) of
                ok ->
                        ok = file:write_file(InfoFile,
                                [io_lib:print(Info), "."]),
                        ok = file:write_file_info(InfoFile,
                                #file_info{mode = ?RDONLY}),
                        {ok, open_domain(D)};
                E -> E
        end. 

open_domain(#domain{home = Home} = D) ->
        case file:read_file_info(Home) of
                {ok, _} -> open_db(D);
                Error ->
                        error_logger:info_report({"DB open said", Home, Error}),
                        throw(invalid_domain)
        end.

open_db(#domain{home = Home, dbname = DBName} = D) ->
        % This will crash if there's a second instance of this domain
        % already running on this node. Note that it is crucial that
        % this function is called *after* we have ensured that the domain
        % really exists on this node. Otherwise someone could pollute
        % out non-gc'ed atom tables with random domain names.
        %register(list_to_atom("ringo_domain-" ++ integer_to_list(DomainID)),
        %        self()),
        Full = case file:read_file_info(filename:join(Home, "closed")) of
                {ok, _} -> true;
                _ -> false
        end,
        InfoFile = filename:join(Home, "info"),
        {ok, DB} = file:open(DBName, [append, raw]),
        {ok, Nfo} = file:script(InfoFile),
        D#domain{db = DB, size = domain_size(Home),
                 full = Full, info = Nfo}.

domain_size(Home) ->
        [Size, _] = string:tokens(os:cmd(["du -b ", Home, " |tail -1"]), "\t"),
        list_to_integer(Size).



%%%
%%% Put with replication
%%%

replicate(_, _, ?MAX_TRIES) ->
        error_logger:warning_report({"Replication failed!"}),
        failed;

replicate({DServer, DomainID, EntryID, Entry, Nrepl} = _R, Prev, _Tries) ->
        % XXX: This is the correct line:
        %Me = {net_adm:localhost(), node(), self()},
        % XXX: This is for debugging:
        Me = {os:getenv("DBGHOST"), node(), self()},
        if Prev == node() -> ok;
        true ->
                error_logger:info_report({"replicate semd", EntryID}),
                gen_server:cast({ringo_node, Prev}, {{domain, DomainID},
                        {repl_put, EntryID, Entry, Me, DServer, Nrepl}})
        
                % For super-opportunistic replication, the following line can
                % be commented out. It means that we don't wait for any replies
                % from replicas.
                %receive_repl_replies(R, Prev, Tries, Nrepl)
        end.

% If receive_repl_replies is run on the same process as ringo_domain, note that
% receive may have to select repl_reply replies amongst are large number of 
% incoming messages, which is expensive.
receive_repl_replies(_, _, _, 0) -> ok;
receive_repl_replies({_, _, EntryID, _, _} = R, Prev, Tries, _N) ->
        receive
                {repl_reply, {ring_too_small, EntryID}} ->
                        ok;
                {repl_reply, {ok, EntryID}} ->
                        % Opportunistic replication: If at least
                        % one replica succeeds, we are happy (and
                        % assume that quite likely more than one have
                        % succeeded)
                        ok
                        % For less opportunistic replication, uncomment
                        % the following line to wait for more replies.
                        %receive_repl_replies(R, Tries, N - 1)
        after ?REPL_TIMEOUT ->
                % Re-send policy: Disable this for faster operation. 
                replicate(R, Prev, Tries + 1)
        end.

open_or_clone(Req, From, D) ->
        case catch open_domain(D) of 
                NewD when is_record(NewD, domain) -> handle_cast(Req, NewD);
                _ -> {ok, InfoPack} = gen_server:call(From, get_domaininfo),
                     {ok, NewD} = new_domain(D, InfoPack),
                     handle_cast(Req, NewD)
        end.

% should we prevent replica or sync entries to be added if the resulting
% chunk would exceed the maximum size? If yes, spontaneously corrupted entries
% can't be fixed. If no, a chunk may grow infinitely large. In this case, it is
% possible that a nasty bug causes a large amounts of sync entries to be sent
% which would eventually fill the disk.


% We trust that any function that calls do_write has checked fullness of the
% domain already. If the domain is full and do_write() is called anyway, we 
% believe that the caller has a good reason for that and perform write normally.
do_write(Entry, #domain{db = DB, full = true} = D) ->
        ringo_writer:write_entry(DB, Entry),
        D;

do_write(Entry, #domain{db = DB, home = Home, size = Size} = D) ->
        ringo_writer:write_entry(DB, Entry),
        S = size(Entry) + Size,
        if S > ?DOMAIN_CHUNK_MAX ->
                close_domain(Home, DB),
                D#domain{size = S, full = true};
        true ->
                D#domain{size = S, full = false}
        end.   

close_domain(Home, DB) ->
        ok = file:sync(DB),
        CFile = filename:join(Home, "closed"),
        ok = file:write_file(CFile, <<>>), 
        ok = file:write_file_info(CFile, #file_info{mode = ?RDONLY}).


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
        
        stats_buffer_add(Stats, sync_time, nu),
        
        DBN = if DB == none -> empty; true -> DBName end,

        {[[Root]|_] = Tree, LeafIDsX} = update_sync_tree(This, DBN),
        gen_server:cast(This, {update_sync_data, Tree, LeafIDsX}),
        stats_buffer_add(Stats, synctree_root, Root),
        flush_sync_outbox(This, DBN);

% Replica-end in synchronization -- the active party
resync(This, replica) ->
        error_logger:info_report({"resync replica"}),
        #domain{dbname = DBName, db = DB, id = DomainID, owner = false,
                stats = Stats, host = Host, home = Home} 
                        = gen_server:call(This, get_current_state),
        
        stats_buffer_add(Stats, sync_time, nu),
        
        DBN = if DB == none -> empty; true -> DBName end,

        {[[Root]|_] = Tree, _} = update_sync_tree(This, DBN),
        stats_buffer_add(Stats, synctree_root, Root),
        {ok, {_, OPid}, Distance} = find_owner(DomainID),
        % BUG: Shouldn't the domain die if find_owner fails? Now it seems
        % that only the resync process dies. Namely, how to make sure that
        % if there're two owners, one of them will surely die.
        DiffLeaves = merkle_sync(OPid, [{0, Root}], 1, Tree),
        stats_buffer_add(Stats, diff_size, length(DiffLeaves)),
        if DiffLeaves == [] -> ok;
        true ->
                SyncIDs = ringo_sync:collect_leaves(DiffLeaves, DBName),
                error_logger:info_report({"syncids", SyncIDs}),
                gen_server:cast(OPid, {sync_pack, This, SyncIDs, Distance}),
                gen_server:cast(OPid, {sync_external, Host ++ ":" ++ Home})
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
        gen_server:cast(This, {update_num_entries,
                ringo_sync:count_entries(LeafIDs)}),
        %error_logger:info_report({"LeafIDs", LeafIDs}),
        Entries = lists:filter(fun({SyncID, _, _}) ->
                not ringo_sync:in_leaves(LeafIDs, SyncID)
        end, Inbox),
        {LeafHashesX, LeafIDsX} = flush_sync_inbox(This, Entries,
                LeafHashes, LeafIDs),
        Tree = ringo_sync:build_merkle_tree(LeafHashesX),
        ets:delete(LeafHashesX),
        {Tree, LeafIDsX}.

% flush_sync_inbox writes entries that have been sent to this replica
% (or owner) to disk and updates the leaf hashes accordingly
flush_sync_inbox(_, [], LeafHashes, LeafIDs) ->
        error_logger:info_report({"flush inbox (empty)"}),
        {LeafHashes, LeafIDs};

flush_sync_inbox(This, [{SyncID, Entry, From}|Rest], LeafHashes, LeafIDs) ->
        error_logger:info_report({"flush inbox"}),
        ringo_sync:update_leaf_hashes(LeafHashes, SyncID),
        LeafIDsX = ringo_sync:update_leaf_ids(LeafIDs, SyncID),
        gen_server:cast(This, {write_entry, Entry, From}),
        flush_sync_inbox(This, Rest, LeafHashes, LeafIDsX).

% flush_sync_outbox sends entries that exists on this replica or owner to
% another node that requested the entry
flush_sync_outbox(_, empty) ->
        error_logger:info_report({"flush outbox (empty)"}),
        ok;

flush_sync_outbox(This, DBName) ->
        error_logger:info_report({"flush outbox"}),
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

stats_buffer_add(Stats, Key, Value) ->
        case ets:lookup(Stats, Key) of
                [] -> ets:insert(Stats, {Key,
                        [{ringo_util:format_timestamp(now()), Value}]});
                [{_, W}] -> ets:insert(Stats, {Key, lists:sublist(
                        [{ringo_util:format_timestamp(now()), Value}|W],
                                ?STATS_WINDOW_LEN)})
        end.


%%% callback stubs


terminate(_Reason, #domain{db = none}) -> {};
terminate(_Reason, #domain{db = DB}) ->
        file:close(DB).

code_change(_OldVsn, State, _Extra) -> {ok, State}.

