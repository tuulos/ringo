-module(ringo_indexdomain).
-behaviour(gen_server).

-define(IBLOCK_SIZE, 10000).
-define(KEYCACHE_LIMIT, 16 * 1024).

% - cur_iblock is the currently active index (iblock), as returned by 
%    ringo_index:new_dex()
% - cur_size is the number of entries in the current index
% - cur_start is the offset in the DB to the first entry in the
%   current index
% - cur_offs is the end offset in the DB to the latest entry in the
%   current index
-record(index, {cur_iblock, cur_size, cur_start, cur_offs, iblocks, db,
        cache_type, cache, domain, home, dbname, cache_limit}).

-export([start_link/4, init/1, handle_call/3, handle_cast/2, handle_info/2, 
         terminate/2, code_change/3]).

% TODO: Make test that gradually builds index (several iblocks) with put requests,
% then deletes iblocks and re-creates them with scan_iblocks. The resulting
% files should be identical.

% Starting the domain index: In the worst case index needs to be rebuilt from
% scratch during initialization of the index server. This can take tens of
% seconds but start_link returns instantly anyway. However, this means that
% requests to the server will be queued until re-indexing finishes.

% The assumed benefit of keycache is that if there's such a large number
% iblocks that keeping them in memory is infeasible, and due to the
% nature of the application the LRU assumption holds for keys, keycache
% should be a viable alternative. The crucial point is that it should
% take less memory than the iblock cache, which seems to be a difficult
% goal to achieve, given that we have to maintain *two* gb_trees, one for
% the cache proper and for the LRU structure.

start_link(Domain, Home, DBName, Options) ->
        S = case gen_server:start_link(ringo_indexdomain, 
                        [Domain, Home, DBName, Options], []) of
                {ok, Server} -> Server;
                {error, {already_started, Server}} -> Server
        end,
        gen_server:cast(S, initialize),
        {ok, S}.

init([Domain, Home, DBName, Options]) ->
        error_logger:info_report({"Index opens for", DBName, Options}),
        {CacheType, Cache} = case {
                proplists:get_value(keycache, Options, false),
                proplists:get_value(noindex, Options, false)} of
                        {_, true} -> {none, none};
                        {true, false} -> {key, {gb_trees:empty(), lrucache:new()}};
                        {false, false} -> {iblock, []}
                end,
        CacheLimit = ringo_util:get_param("KEYCACHE_LIMIT", ?KEYCACHE_LIMIT),
        
        {ok, DB} = bfile:fopen(DBName, "r"),
        {ok, #index{cur_iblock = ringo_index:new_dex(),
                   cur_size = 0,
                   cur_start = 0,
                   cur_offs = 0,
                   cache_type = CacheType,
                   cache_limit = CacheLimit,
                   cache = Cache,
                   domain = Domain,
                   db = DB,
                   home = Home,
                   dbname = DBName,
                   iblocks = []
        }}.

handle_call(_, _, D) -> {reply, error, D}.

handle_cast({get, Key, From}, #index{cache_type = iblock, db = DB,
        cache = Cache, cur_iblock = Current, home = Home} = D) ->
        
        Hash = ringo_index:dexhash(Key),
        Offsets = lists:flatten([begin
                {_, L} = ringo_index:find_key(Hash, Iblock), L
        end || Iblock <- lists:reverse([Current|Cache])]),
        send_entries(Offsets, From, DB, Home, Key),
        {noreply, D};

handle_cast({get, Key, From}, #index{cache_type = key,
        home = Home, db = DB, cur_iblock = Current} = D) ->
        
        Hash = ringo_index:dexhash(Key),
        {Lst, D0} = keycache_get(Key, D),
        {_, CL} = ringo_index:find_key(Hash, Current, false),
        Offsets = lists:flatten([ringo_index:decode_poslist(P) ||
                P <- Lst ++ [CL], is_bitstring(P)]),
        send_entries(Offsets, From, DB, Home, Key),
        {noreply, D0};

handle_cast({get, _, _}, #index{cache_type = none,
        home = _Home, db = _DB} = D) ->
        {noreply, D};

handle_cast({put, _, _, _}, #index{cache_type = none} = D) ->
        {noreply, D};

% ignore entries that were already indexed during initialization
handle_cast({put, _, Pos, _}, #index{cur_offs = Offs} = D) when Pos < Offs ->
        {noreply, D};
                
handle_cast({put, Key, Pos, EndPos}, #index{cur_iblock = Iblock,
        cur_size = Size} = D) ->

        NIblock = ringo_index:add_item(Iblock, Key, Pos),
        {noreply, save_iblock(D#index{cur_iblock = NIblock,
                cur_offs = EndPos, cur_size = Size + 1})};


% Initialize re-indexes the DB. Indexing is done in two phases:
% First, we check all existing iblocks on disk and load and 
% re-process them if possible. Secondly, rest of the DB which is
% not covered by the existing iblocks, is reindexed in index_iblock().
handle_cast(initialize, #index{home = Home} = D) ->
        % Find existing iblocks in the domain's home directory
        Cands = lists:keysort(1, [X || X <- lists:map(fun(F) ->
                case string:tokens(F, "-.") of
                        [_, S, E] ->
                                {list_to_integer(S), list_to_integer(E), F};
                        _ -> error_logger:warning_report(
                                {"Invalid iblock file", F}), none
                end
        end, filelib:wildcard("iblock-*", Home)), is_tuple(X)]),
        
        % Find out how much of the index the existing iblocks cover (holes are
        % not allowed in the coverage). Load iblocks and process them as they 
        % would have been just created.
        {_, D0} = lists:foldl(fun
                ({S, E, F}, {Pos, Dx} = X) when S == Pos ->
                        Path = filename:join(Home, F),
                        case ringo_reader:read_file(Path) of
                                {ok, Iblock} -> {E, save_iblock(F, Iblock,
                                        Dx#index{cur_offs = E})};
                                _ -> X
                        end;
                (_, X) -> X
        end, {0, D}, Cands),
        % Re-index the rest
        {noreply, index_iblock(D0, ?IBLOCK_SIZE)}.

index_iblock(D, N) when N < ?IBLOCK_SIZE -> D;
index_iblock(#index{dbname = DBName, cur_offs = StartPos} = D, _) ->
        {N, Dex, EndPos} = ringo_index:build_index(DBName, StartPos,
                ?IBLOCK_SIZE),
        D0 = save_iblock(D#index{cur_iblock = Dex, cur_start = StartPos,
                cur_offs = EndPos, cur_size = N}),
        index_iblock(D0, N).

% reply to save_iblock's put request
handle_info({ringo_reply, _, _}, D) ->
        {noreply, D}.

%%%
%%% Send entries, one by one, to the requester
%%%

send_entries(Offsets, From, DB, Home, Key) ->
        % Offsets should be in increasing order to benefit most from read-ahead
        % buffering and page caching.
        lists:foreach(fun(Offset) ->
                case ringo_index:fetch_entry(DB, Home, Key, Offset) of
                        {_Time, _Key, Value} ->
                                From ! {ringo_get, {entry, Value}};
                        % ignore corruped entries -- might not be wise
                        invalid_entry -> ok;
                        ignore -> ok
                end
        end, Offsets),
        From ! {ringo_get, done}.

%%%
%%% Iblock becomes full
%%%

save_iblock(#index{cur_size = Size} = D) when Size < ?IBLOCK_SIZE -> D;
save_iblock(#index{cur_iblock = Iblock, cur_start = Start,
        cur_offs = End} = D) ->
        
        error_logger:info_report({"Iblock full!"}),
        Key = io_lib:format("iblock-~b-~b", [Start, End]),
        save_iblock(Key, iolist_to_binary(ringo_index:serialize(Iblock)), D).
        
save_iblock(Key0, SIblock, #index{domain = Domain, cur_offs = End,
        iblocks = Iblocks} = D) ->

        Key = iolist_to_binary(Key0),
        error_logger:info_report({"handling iblock", Key, "end", End}),
        gen_server:cast(Domain, {put, Key, SIblock, [iblock], self()}),
        D0 = update_cache(SIblock, D),
        error_logger:info_report({"Iblock full! ok"}),
        D0#index{cur_start = End, cur_iblock = ringo_index:new_dex(),
                 cur_size = 0, iblocks = Iblocks ++ [Key]}.

update_cache(SIblock, #index{cache_type = iblock, cache = Cache} = D) ->
        D#index{cache = [SIblock|Cache]};

update_cache(SIblock, #index{cache_type = key, cache = {Cache, LRU}} = D) ->
        error_logger:info_report({"Ipdate cache"}),
        NC = lists:foldl(fun({Key, {Sze, V} = X}, C) ->
                {_, L} = ringo_index:find_key(Key, SIblock, false),
                if L == [] ->
                        gb_trees:insert(Key, X, C);
                true ->  
                        gb_trees:insert(Key, {Sze + size(L), V ++ [L]}, C)
                end
        end, gb_trees:empty(), gb_trees:to_list(Cache)),
        D#index{cache = {NC, LRU}}.
       
%%%
%%% Keycache
%%%

keycache_get(Key, #index{cache = {Cache, _}} = D) ->
        update_keycache(Key, gb_trees:lookup(Key, Cache), D).

% cache hit
update_keycache(Key, {value, {_, Lst}}, #index{cache = {Cache, LRU}} = D) ->
        {Lst, D#index{cache = {Cache, lrucache:update(Key, LRU)}}};

% cache miss
update_keycache(Key, none, #index{home = Home, iblocks = Iblocks,
        cache = {Cache, _}} = D) ->

        Sze = gb_fold(fun(S, _, {Sze, _}) -> S + Sze end, 0, 
                gb_trees:iterator(Cache)),
        KeyOffsets = keycache_newentry(Key, Iblocks, Home),
        EntrySize = entry_size(Key, KeyOffsets),
        D0 = keycache_evict(Sze, EntrySize, D),
        {Cache0, LRU0} = D0#index.cache,
        CacheValue = {EntrySize, KeyOffsets}, 
        update_keycache(Key, {value, CacheValue}, D0#index{cache = 
                {gb_trees:insert(Key, CacheValue, Cache0), LRU0}}).

keycache_newentry(Key, Iblocks, Home) ->
        Hash = ringo_index:dexhash(Key),
        lists:map(fun(IblockFile) ->
                Path = filename:join(Home, binary_to_list(IblockFile)),
                case ringo_reader:read_file(Path) of
                        {ok, Iblock} -> {_, L} = ringo_index:find_key(
                                Hash, Iblock, false), L;
                        _ -> []
                end
        end, Iblocks).

keycache_evict(CacheSze, EntrySze, #index{cache_limit = Limit} = D)
        when CacheSze + EntrySze < Limit -> D;

keycache_evict(CacheSze, EntrySze, #index{cache = {Cache, LRU}} = D) ->
        X = lrucache:get_lru(LRU),
        if X == nil -> D;
        true ->
                {Key, LRU0} = X, 
                {Sze, _} = gb_trees:get(Key, Cache),
                keycache_evict(CacheSze - Sze, EntrySze, D#index{cache =
                        {gb_trees:delete(Key, Cache), LRU0}})
        end.

gb_fold(F, S, Iter) -> gb_fold0(F, S, gb_trees:next(Iter)).
gb_fold0(_, S, none) -> S;
gb_fold0(F, S, {K, V, Iter0}) -> gb_fold0(F, F(S, K, V), gb_trees:next(Iter0)).

% Calculate cache size. 64 is an approximate cost in bytes  to upkeep a key
% in the cache
entry_size(K, V) -> entry_size0(size(K) + 64, V).
entry_size0(S, []) -> S;
entry_size0(S, [X|R]) when is_bitstring(X) ->
        entry_size0(S + size(bin_util:pad(X)), R);
entry_size0(S, [X|R]) when is_binary(X) ->
        entry_size0(S + size(X), R);
entry_size0(S, [_|R]) ->
        entry_size0(S, R).

%%%
%%%
%%%

terminate(_Reason, #index{db = DB}) -> bfile:fclose(DB).
code_change(_OldVsn, State, _Extra) -> {ok, State}.

