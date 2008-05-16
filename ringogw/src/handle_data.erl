-module(handle_data).
-export([op/2, op/3, start_active_node_updater/0, start_chunk_cache/0]).

-include("ringo_store.hrl").

-define(ACTIVE_NODE_UPDATE, 5000).

-define(CREATE_DEFAULTS, [{i, "nrepl", "3"}, {i, "timeout", "10000"}, 
        {b, "create", false}, {b, "keycache", false}, {b, "noindex", false}]).
-define(CREATE_FLAGS, [nrepl, keycache, noindex]).

-define(PUT_DEFAULTS, [{i, "timeout", "10000"}]).
-define(PUT_FLAGS, []).
-define(GET_DEFAULTS, [{i, "timeout", "30000"}, {b, "single", false}]).
-define(GET_FLAGS, []).

% FIXME: What happens when we send a request to a node that doesn't have a 
% valid successor in the ring? Does it die (no good, if it's trying to
% heal itself at the same time)?

% CREATE: /gw/data/domain_name?num_repl=3
% GET: /gw/data/domain_name/key_name
% PUT: /gw/data/domain_name/key_name

%op(Script, Params, Data) ->
        %spawn(fun update_active_nodes/0),
%        op1(Script, Params, Data).

% CREATE
op([C|_] = Domain, Params, _Data) when is_integer(C) ->
        PParams = parse_params(Params, ?CREATE_DEFAULTS),
        Flags = parse_flags(PParams, ?CREATE_FLAGS),
        %error_logger:info_report({"CREATE", Domain, "WITH", PParams}),
        V = proplists:is_defined(create, PParams),
        if V ->
                DomainID = ringo_util:domain_id(Domain, 0),
                ok = ringo_send(DomainID,
                        {new_domain, Domain, 0, self(), Flags}),
                T = proplists:get_value(timeout, PParams),
                case ringo_receive(DomainID, T) of
                        {ok, {Node, _Pid}} ->
                                {json, {ok, {Node, formatid(DomainID)}}};
                        {error, eexist} ->
                                {json, {error, <<"domain already exists">>}};
                        Error ->
                                error_logger:warning_report(
                                        {"Unknown create reply", Error}),
                                throw({'EXIT', Error})
                end;
        true ->
                throw({http_error, 400, <<"Create flag missing">>})
        end;

% PUT
op([_Domain, Key], _Params, _Value) when length(Key) > ?KEY_MAX ->
        throw({http_error, 400, <<"Key too large">>});

op([Domain, Key], Params, Value) ->
        PParams = parse_params(Params, ?PUT_DEFAULTS),
        Flags = parse_flags(PParams, ?PUT_FLAGS),
        Msg = {put, list_to_binary(Key), Value, Flags, self()},
        chunk_put(Domain, Msg, proplists:get_value(timeout, PParams), 0);


op(_, _, _) ->
        throw({http_error, 400, <<"Invalid request">>}).

%op(Script, Params) ->
        %spawn(fun update_active_nodes/0),
%        op1(Script, Params).

% GET
% unless parameter ?single is specified, values are sent one by one to the
% requested using chunked encoding. Each value is prefixed by its length, so 
% the client can decode individual values from the sequence/ If ?single is 
% specified, only the first value is sent to the requester without the length
% prefix, which makes it possible to view the value directly in the browser.
% (Consider supporting different mime-types, given a proper parameter).
op([Domain, Key], Params) ->
        Msg =  {get, list_to_binary(Key), self()},
        Req = fun(Chunk) ->
                DomainID = ringo_util:domain_id(Domain, Chunk),
                ok = ringo_send(DomainID, Msg)
        end,
        Req(0),
        PParams = parse_params(Params, ?GET_DEFAULTS),
        Single = proplists:get_value(single, PParams),
        T = proplists:get_value(timeout, PParams),
        if Single ->
                ringo_receive_chunked(single, Req, 0, T);
        true ->
                ringo_receive_chunked(many, Req, 0, T)
        end;

op(_, _) ->
        throw({http_error, 400, <<"Invalid request">>}).

%%%
%%% Chunked put
%%%

chunk_put(Domain, Msg, T, NumTries) ->
        {Chunk, DomainID} = chunk_id(Domain),
        ok = ringo_send(DomainID, Msg),
        case ringo_receive(DomainID, T) of
                % Entry put ok
                {ok, {Node, EntryID}} ->
                        {json, {ok, Node, formatid(DomainID),
                                formatid(EntryID)}};
                % Domain doesn't exist
                {error, invalid_domain} when Chunk == 0 ->
                        {json, {error, <<"Domain doesn't exist">>}};
                % A new chunk should have been created by the
                % domain_full case below. If we end up here, something
                % went wrong there. We reset the chunk cache, try 
                % again, and hope that a subsequent put that gets a
                % domain_full reply will create the chunk correctly.
                {error, invalid_domain} when NumTries == 0 ->
                        chunk_reset(Domain),
                        chunk_put(Domain, Msg, T, 1);
                % Resetting didn't help. Hopeless.
                {error, invalid_domain} ->
                        throw({'EXIT', "Broken domain"});
                % Chunk is full. Try the next chunk.
                {error, domain_full, Chunk, Flags} ->
                        {NChunk, ChunkID} = chunk_full(Domain, Chunk),
                        % Try to create the next chunk -- it may well exist
                        % already
                        ok = ringo_send(ChunkID,
                                {new_domain, Domain, NChunk, self(), Flags}),
                        case ringo_receive(ChunkID, T) of
                                {ok, _} -> ok;
                                {error, eexist} -> ok;
                                Error -> throw({'EXIT', Error})
                        end,
                        chunk_put(Domain, Msg, T, 0);
                Error ->
                        error_logger:warning_report(
                                {"Unknown put reply", Error}),
                        throw({'EXIT', Error})
        end.

%%%
%%% Ringo communication
%%%

ringo_send(DomainID, Msg) ->
        case ringo_util:best_matching_node(DomainID, get_active_nodes()) of
                {ok, Node} -> 
                        gen_server:cast({ringo_node, Node}, {match, 
                                DomainID, domain, self(), Msg});
                {error, no_nodes} ->
                        throw({http_error, 503, <<"Empty ring">>});
                Error -> throw({'EXIT', Error})
        end, ok.

ringo_receive_chunked(Mode, Req, N, Timeout) when Timeout > 60000 ->
        ringo_receive_chunked(Mode, Req, N, 60000);

ringo_receive_chunked(single, Req, N, Timeout) ->
        receive
                {ringo_get, {entry, E}} ->
                        {data, E};
                {ringo_get, done} when N == 0 ->
                        throw({http_error, 404, <<"Key not found">>});
                {ringo_get, done}  ->
                        ringo_receive_chunked(single, Req, N - 1, Timeout);
                {ringo_get, full, Chunk} ->
                        Req(Chunk + 1),
                        ringo_receive_chunked(single, Req, N + 1, Timeout);
                {ringo_get, invalid_domain} when N == 0 ->
                        throw({http_error, 404, <<"Key not found">>});
                {ringo_get, invalid_domain} ->
                        % This happens if the previous chunk is 
                        % full and the next one hasn't been created yet.
                        ringo_receive_chunked(single, Req, N - 1, Timeout)
        after Timeout ->
                throw({http_error, 408, <<"Request timeout">>})
        end;

ringo_receive_chunked(many, Req, _, Timeout) ->
        {chunked, fun(N) ->
                receive 
                        {ringo_get, {entry, _} = E} -> E;
                        {ringo_get, done} when N == 0 -> done;
                        {ringo_get, done} -> {next, N - 1};
                        {ringo_get, full, Chunk} ->
                                Req(Chunk + 1),
                                {next, N + 1};
                        {ringo_get, invalid_domain} when N == 0 -> done;
                        {ringo_get, invalid_domain} -> {next, N - 1}
                after Timeout -> timeout
                end
        end}.

ringo_receive(DomainID, Timeout) when Timeout > 60000 ->
        ringo_receive(DomainID, 60000);

ringo_receive(DomainID, Timeout) ->
        receive
                {ringo_reply, DomainID, Reply} -> Reply;
                _ -> ringo_receive(DomainID, Timeout)
        after Timeout ->
                % Ring may have changed, update node list immediately
                active_node_updater ! update,
                throw({http_error, 408, <<"Request timeout">>})
        end.

%%%
%%% Parameter parsing
%%%

parse_params(Params, Defaults) ->
        T = fun(false) -> undefined; (true) -> true end,
        [X || {_, W} = X <- lists:map(fun
               ({i, K, V}) ->
                        {list_to_atom(K), list_to_integer(
                                proplists:get_value(K, Params, V))};
               ({b, K, _}) ->
                        {list_to_atom(K), T(proplists:is_defined(K, Params))};
               ({_, K, V}) ->
                        {list_to_atom(K), proplists:get_value(K, Params, V)}
        end, Defaults), W =/= undefined].

parse_flags(Params, AllowedFlags) ->
        [P || {K, _} = P <- Params, proplists:is_defined(K, AllowedFlags)].

formatid(ID) ->
        list_to_binary(erlang:integer_to_list(ID, 16)).

%%%
%%% Node list update
%%%

get_active_nodes() ->
        active_node_updater ! {get, self()},
        receive
                {nodes, Nodes} -> Nodes
        after 100 ->
                throw({http_error, 408, "Active node request timeout"})
        end.

start_active_node_updater() ->
        {ok, spawn_link(fun() -> update_active_nodes() end)}.

update_active_nodes() ->
        register(active_node_updater, self()),
        timer:send_interval(?ACTIVE_NODE_UPDATE, update),
        update_active_nodes(active_nodes()).

update_active_nodes(Nodes) ->
        receive
                {get, From} ->
                        From ! {nodes, Nodes},
                        update_active_nodes(Nodes); 
                update ->
                        update_active_nodes(active_nodes())
        end.

active_nodes() ->
        ringo_util:sort_nodes(ringo_util:ringo_nodes()).

%%%
%%% Chunk cache
%%%

chunk_id(Domain) ->
        DomainID = ringo_util:domain_id(Domain, 0),
        case ets:lookup(chunk_cache, DomainID) of
                [] -> {0, DomainID};
                [{_, Chunk, ChunkID}] -> {Chunk, ChunkID}
        end.

chunk_full(Domain, Chunk) ->
        DomainID = ringo_util:domain_id(Domain, 0),
        NChunk = Chunk + 1,
        ChunkID = ringo_util:domain_id(Domain, NChunk),
        ets:insert(chunk_cache, {DomainID, NChunk, ChunkID}),
        {NChunk, ChunkID}.

chunk_reset(Domain) ->
        DomainID = ringo_util:domain_id(Domain, 0),
        ets:delete(chunk_cache, DomainID).

start_chunk_cache() ->
        {ok, spawn_link(fun() ->
                ets:new(chunk_cache, [named_table, public]),
                receive _ -> ok end
        end)}.
        
