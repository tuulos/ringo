
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

-export([start/5, init/1, handle_call/3, handle_cast/2, handle_info/2, 
        stats_buffer_add/3, terminate/2, code_change/3]).

-include("ringo_store.hrl").
-include("ringo_node.hrl").
-include_lib("kernel/include/file.hrl").

-define(DOMAIN_CHUNK_MAX, 104857600). % 100MB
-define(SYNC_BUF_MAX_WORDS, 1000000).

-define(CHECK_EXTERNAL_INTERVAL, 30000). % make this longer!
-define(RESYNC_INTERVAL, 30000). % make this longer!
-define(GLOBAL_RESYNC_INTERVAL, 30000). % make this longer!
-define(STATS_WINDOW_LEN, 5).

% replication
-define(MAX_TRIES, 3).
-define(REPL_TIMEOUT, 2000).

start(Home, DomainID, IsOwner, Prev, Next) ->
        case gen_server:start(ringo_domain, 
                %[Home, DomainID, IsOwner, Prev, Next], [{debug, [trace, log]}]) of
                [Home, DomainID, IsOwner, Prev, Next], []) of
                {ok, Server} -> {ok, Server};
                {error, {already_started, Server}} -> {ok, Server}
        end.


init([Home, DomainID, IsOwner, Prev, Next]) ->
        error_logger:info_report({"Domain opens", DomainID, IsOwner}),
        {A1, A2, A3} = now(),
        random:seed(A1, A2, A3),
        % NB: Inbox must keep only a single entry for a key, i.e. a
        % list won't work.
        Inbox = ets:new(sync_inbox, []),
        Outbox = ets:new(sync_outbox, [bag]),
        Stats = ets:new(stats, [public]),
        erlang:monitor(process, ringo_node),
        Path = filename:join(Home, "rdomain-" ++
                erlang:integer_to_list(DomainID, 16)),
               
        process_flag(trap_exit, true),
        ExtProc = spawn_link(ringo_external, fetch_external, [Path]),

        D0 = #domain{this = self(),
                     owner = IsOwner,
                     home = Path, 
                     dbname = filename:join(Path, "data"),
                     host = net_adm:localhost(),
                     id = DomainID, 
                     db = none,
                     full = false,
                     sync_tree = none, 
                     num_entries = 0,
                     max_repl_entries = 0,
                     sync_ids = none,
                     sync_inbox = Inbox,
                     sync_outbox = Outbox,
                     stats = Stats,
                     prevnode = Prev,
                     nextnode = Next,
                     extproc = ExtProc,
                     index = none
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

        ResyncInt = ringo_util:get_iparam(
                "RESYNC_INTERVAL", ?RESYNC_INTERVAL),
        GlobalInt = ringo_util:get_iparam(
                "GLOBAL_INTERVAL", ?GLOBAL_RESYNC_INTERVAL),
        ExtInt = ringo_util:get_iparam(
                "CHECK_EXT_INTERVAL", ?CHECK_EXTERNAL_INTERVAL),
        
        RTime = round(ResyncInt + random:uniform(ResyncInt * 0.5)),
        if IsOwner ->
                {ok, _} = timer:apply_interval(RTime, ringo_syncdomain,
                        resync, [self(), owner]),
                GTime = round(GlobalInt + random:uniform(GlobalInt * 0.5)),
                {ok, _} = timer:apply_interval(GTime, ringo_syncdomain,
                                global_resync, [DomainID]);
        true ->
                {ok, _} = timer:apply_interval(RTime, ringo_syncdomain,
                        resync, [self(), replica])
        end,
        {ok, _} = timer:apply_interval(ExtInt, ringo_external, 
                check_external, [self()]),
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
        {reply, {ok, Info}, D};

handle_call(get_current_state, _From, D) ->
        {reply, D, D};

handle_call({get_file_handle, ExtFile}, _, #domain{home = Home} = D) ->
        ExtPath = filename:join(Home, ExtFile),
        F = case file:open(ExtPath, [read, binary]) of
                {ok, File} = R -> 
                        % caller takes care of the file, not me
                        unlink(File), R;
                Other -> Other
        end,
        {reply, F, D}.

              
%%%
%%% Basic domain operations: Create, put, get 
%%%

handle_cast({new_domain, Name, Chunk, From, Params},
        #domain{id = DomainID} = D) ->

        InfoPack = [{nrepl, proplists:get_value(nrepl, Params)},
                    {keycache, proplists:is_defined(keycache, Params)},
                    {noindex, proplists:is_defined(noindex, Params)},
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

%%%
%%% Put
%%%


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

% Index is opened lazily: Now it's the time unless index is already open.
handle_cast({put, _, _, _, _} = P, #domain{index = none, home = Home, 
        owner = true, dbname = DBName, info = InfoPack} = D) ->

        {ok, S} = ringo_indexdomain:start_link(self(), Home, DBName, InfoPack),
        handle_cast(P, D#domain{index = S});

% normal case
handle_cast({put, Key, Value, Flags, From}, #domain{owner = true, 
        index = Index, full = Full, id = DomainID, info = InfoPack, db = DB,
        prevnode = Prev} = D) when Full == false; Flags == [iblock] ->

        EntryID = random:uniform(4294967295),
        {E, _} = Entry = ringo_writer:make_entry(EntryID, Key, Value, Flags),
        From ! {ringo_reply, DomainID, {ok, {node(), EntryID}}},
       
        {ok, Pos} = bfile:ftell(DB),
        NewD = do_write(Entry, D),

        % Don't index index blocks
        if Flags =/= [iblock] ->
                gen_server:cast(Index, {put, Key, Pos, Pos + iolist_size(E)});
        true -> ok
        end,

        % See replicate_proc for different replication policies. Currently
        % performing replication in another process is unsafe, since it 
        % de-serializes the order in which entries hit replicas, which in
        % turn may confuse resyncing process (ringo_reader can only detect
        % duplicate EntryIDs that are consequent).
        
        % Currently the safest approach,
        % in resyncing point of view, is to do opportunistic replication (no
        % re-sends) in a serialized manner (i.e. no spawning).
        Nrepl = proplists:get_value(nrepl, InfoPack),
        DServer = self(),
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

% Put to a replica: This happens if a get request is redirected to
% a replica that re-builds an index and tries to save an iblock. The
% request is ignored. Owner will re-build the index again by itself.
handle_cast({put, _, _, _, _}, #domain{owner = false} = D) ->
        {noreply, D};
        

%%%
%%% Redirected put
%%%

% Request came back to the owner or an infinite loop -> domain doesn't exist
handle_cast({redir_put, Owner, N, {put, _, _, _, From}},
        #domain{id = DomainID} = D) when Owner == node(); N > ?MAX_RING_SIZE ->

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
        Entry = ringo_writer:make_entry(EntryID, Key, Value, Flags),
        From ! {ringo_reply, DomainID, {ok, {Owner, EntryID}}},
        {noreply, do_write(Entry, D)};

% domain full, notify the sender
handle_cast({redir_put, _, _, {put, _, _, _, From}},
        #domain{id = DomainID, full = true} = D) ->
        
        From ! {ringo_reply, DomainID, {error, domain_full}},
        {noreply, D};

%%%
%%% Get
%%%

% Redirected get: Get requests are forwarded to a replica if either
%
% 1. DB is not open (see the corresponding put-case for more information),
% 2. Owner currently has less entries than a replica.
%
% The latter case tries to ensure that get-requests are not based on a DB file
% that is in the middle of resyncing. Note that this doesn't guarantee that 
% all GET requests succeed. It may happen that some entries are missing from
% GET responses when the system is out-of-sync.
handle_cast({get, Key, From}, #domain{owner = true, id = DomainID, db = DB, 
        prevnode = Prev, num_entries = N, max_repl_entries = M} = D)
                when DB == none; N < M -> 
        %error_logger:info_report({"redir get init", Key, DB, N, M}),
        gen_server:cast({ringo_node, Prev}, {{domain, DomainID},
                {get, Key, From, M, node(), 1}}),
        {noreply, D};

% Index not open: Now it's time to open it.
handle_cast({get, _, _} = P, #domain{index = none, home = Home,
        dbname = DBName, info = InfoPack} = D) ->
        {ok, S} = ringo_indexdomain:start_link(self(), Home, DBName, InfoPack),
        handle_cast(P, D#domain{index = S});

% Normal case
handle_cast({get, Key, From}, #domain{index = Index} = D) ->
        %error_logger:info_report({"normal get", Key}),
        gen_server:cast(Index, {get, Key, From}),
        {noreply, D};

% Redirected get: Came back to the originator, or stuck in an infinite loop.
handle_cast({get, _, From, _, Node, N}, #domain{id = DomainID} = D)
        when Node == node(); N > ?MAX_RING_SIZE ->
        %error_logger:info_report({"redir get inf"}),
        From ! {ringo_reply, DomainID, {error, invalid_domain}},

        % If we end up here, we couldn't find a replica that has more and
        % equal number of entries than specified by the owner's
        % max_repl_entries record. This happens if the replica that announced
        % the highest max_repl_entries value has died, thus clearly
        % max_repl_entries value is not valid anymore and we should reset it.
        {noreply, D#domain{max_repl_entries = 0}};

% Redirected get: DB not open or this replica doesn't have all the entries
% -> redirect to previous.
handle_cast({get, Key, From, M, Node, N}, #domain{id = DomainID, db = DB,
        prevnode = Prev, num_entries = Num} = D) when DB == none; Num < M ->
        
        %error_logger:info_report({"redir get next", DB, Key, Num, M}),
        gen_server:cast({ringo_node, Prev}, {{domain, DomainID},
                {get, Key, From, M, Node, N + 1}}),
        {noreply, D};

% Redirected get: DB open, proceed as with normal get
handle_cast({get, Key, From, _, _, _}, D) ->
        %error_logger:info_report({"redir get match", Key}),
        handle_cast({get, Key, From}, D);
                

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
        #domain{id = DomainID, prevnode = Prev} = D) ->

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
        {noreply, do_write(Entry, D)};

%%%
%%% Synchronization
%%%

handle_cast({sync_write_entry, _, From} = Req, #domain{db = none} = D) ->
        open_or_clone(Req, From, D);

handle_cast({sync_write_entry, {Entry, {}} = E, From},
        #domain{extproc = Ext, index = Index, db = DB} = D) ->
        
        {_, _, Flags, Key, Val} = ringo_reader:decode(Entry),
        case Val of
                {ext, _} -> Ext ! {fetch, {From, Entry}};
                _ -> ok
        end,
        case proplists:is_defined(iblock, Flags) of
                true ->
                        {ok, Pos} = bfile:ftell(DB),
                        gen_server:cast(Index,
                                {put, Key, Pos, Pos + size(Entry)});
                false -> ok
        end,
        {noreply, do_write(E, D)};

handle_cast({update_sync_data, Tree, LeafIDs}, #domain{owner = true} = D) ->
        {noreply, D#domain{sync_tree = Tree, sync_ids = LeafIDs}};

handle_cast({sync_put, SyncID, Entry, From},
        #domain{sync_inbox = Inbox} = D) ->
        %error_logger:info_report({"sync put", SyncID, From}),

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

handle_cast({sync_pack, From, Pack, NumEntries, _Distance}, #domain{info = _InfoPack,
        sync_ids = LeafIDs, sync_outbox = Outbox, owner = true} = D) ->

        D0 = if D#domain.max_repl_entries < NumEntries ->
                D#domain{max_repl_entries = NumEntries};
        true -> D
        end,
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
        {noreply, D0};

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

handle_cast({find_owner, _, _}, D) ->
        {noreply, D};

handle_cast({find_file, ExtFile, {Pid, Node} = From, N},
                #domain{home = Home, prevnode = Prev, id = DomainID} = D) 
                        when N == 0; N < ?MAX_RING_SIZE, Node =/= node() ->
        
        case file:read_file_info(filename:join(Home, ExtFile)) of
                {ok, _} -> error_logger:info_report({"File", ExtFile, "Found"}),
                        Pid ! {file_found, ExtFile, self()};
                _ -> gen_server:cast({ringo_node, Prev},
                        {{domain, DomainID}, {find_file, ExtFile, From, N + 1}})
        end,
        {noreply, D};

handle_cast({find_file, _, {Pid, _}, _}, D) ->
        Pid ! file_not_found,
        {noreply, D};

handle_cast({get_status, From}, D) ->
        From ! {status, node(), 
                [{id, D#domain.id},
                 {num_entries, D#domain.num_entries},
                 {max_repl_entries, D#domain.max_repl_entries},
                 {size, D#domain.size},
                 {full, D#domain.full},
                 {owner, D#domain.owner}] ++ ets:tab2list(D#domain.stats)},
        {noreply, D};

handle_cast({kill_domain, Reason}, D) ->
        error_logger:info_report({"Domain killed: ", Reason}),
        {stop, normal, D};

handle_cast({update_num_entries, N}, D) ->
        {noreply, D#domain{num_entries = N}};

handle_cast(Msg, D) ->
        error_logger:warning_report({"Unknown cast", Msg}),
        {noreply, D}.
        
%%%
%%% Random messages
%%%


%handle_info({repl_domain, Info}, D) ->
%        {ok, NewD} = new_domain(D, Info),
%        {noreply, NewD};

handle_info({'EXIT', Pid, _}, #domain{extproc = Pid, home = Home} = D) ->
        error_logger:info_report("ringo_external dies, respawning it"),
        ExtProc = spawn_link(ringo_external, fetch_external, [Home]),
        {noreply, D#domain{extproc = ExtProc}};

handle_info({'EXIT', Pid, _}, #domain{index = Pid} = D) ->
        
        % Why not to respawn index here similarly to ringo_external above?
        % Ringo_indexdomain does pretty heavy stuff during initialization, so
        % it might well crash right there. If we respawn it as soon as it
        % crashes, we might end up in a busy-loop, kicking a dead horse. In
        % contrast, by letting the next get / put request to respawn index,
        % there's a slight chance that the problem has fixed itself (e.g. by
        % resycing) by that time, or at least we end up kicking the horse less
        % ferociously.
        error_logger:info_report(
                "index dies, next get/put request will respawn it"),
        {noreply, D#domain{index = none}};

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
        {ok, DB} = bfile:fopen(DBName, "a"),
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
        %error_logger:info_report({"replicate", EntryID, size(Entry)}),
        Me = {os:getenv("DBGHOST"), node(), self()},
        if Prev == node() -> ok;
        true ->
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
do_write(Entry, #domain{home = Home, db = DB, full = true} = D) ->
        ringo_writer:write_entry(Home, DB, Entry),
        bfile:fflush(DB),
        D#domain{num_entries = D#domain.num_entries + 1};

do_write(Entry, #domain{db = DB, home = Home, size = Size} = D) ->
        ringo_writer:write_entry(Home, DB, Entry),
        bfile:fflush(DB),
        S = size(Entry) + Size,
        if S > ?DOMAIN_CHUNK_MAX ->
                close_domain(Home, DB),
                D#domain{size = S, full = true,
                        num_entries = D#domain.num_entries + 1};
        true ->
                D#domain{size = S, full = false,
                        num_entries = D#domain.num_entries + 1}
        end.   

close_domain(Home, DB) ->
        ok = bfile:fclose(DB),
        CFile = filename:join(Home, "closed"),
        ok = file:write_file(CFile, <<>>), 
        ok = file:write_file_info(CFile, #file_info{mode = ?RDONLY}).

stats_buffer_add(Stats, Key, Value) ->
        case ets:lookup(Stats, Key) of
                [] -> ets:insert(Stats, {Key,
                        [{ringo_util:format_timestamp(now()), Value}]});
                [{_, W}] -> ets:insert(Stats, {Key, lists:sublist(
                        [{ringo_util:format_timestamp(now()), Value}|W],
                                ?STATS_WINDOW_LEN)})
        end.

%%% callback stubs


terminate(Reason, #domain{db = none}) -> 
        error_logger:info_report({"Terminate", Reason});
terminate(Reason, #domain{db = DB}) ->
        error_logger:info_report({"Terminate (close DB)", Reason}),
        bfile:fclose(DB).

code_change(_OldVsn, State, _Extra) -> {ok, State}.


