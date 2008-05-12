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
        error_logger:info_report({"CREATE", Domain, "WITH", PParams}),
        V = proplists:is_defined(create, PParams),
        if V ->
                {ok, DomainID} = ringo_send(Domain,
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

        % CHUNK FIX: If chunk 0 fails, try chunk C + 1 etc.
        {ok, DomainID} = ringo_send(Domain, {put,
                list_to_binary(Key), Value, Flags, self()}),
       
        T = proplists:get_value(timeout, PParams),
        case ringo_receive(DomainID, T) of
                {ok, {Node, EntryID}} ->
                        {json, {ok, Node, formatid(DomainID),
                                formatid(EntryID)}};
                {error, invalid_domain} ->
                        {json, {error, <<"Domain doesn't exist">>}};
                {error, domain_full} ->
                        % CHUNK FIX: Try next chunk
                        {json, {error, <<"Domain full">>}};
                Error ->
                        error_logger:warning_report(
                                {"Unknown put reply", Error}),
                        throw({'EXIT', Error})
        end;

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
        PParams = parse_params(Params, ?GET_DEFAULTS),
        {ok, _} = ringo_send(Domain, 
                {get, list_to_binary(Key), self()}),
                proplists:get_value(timeout, PParams),
        Single = proplists:get_value(single, PParams),
        T = proplists:get_value(timeout, PParams),
        if Single ->
                ringo_receive_chunked(single, T);
        true ->
                ringo_receive_chunked(many, T)
        end;

op(_, _) ->
        throw({http_error, 400, <<"Invalid request">>}).

%%%
%%% Ringo communication
%%%

ringo_send(Domain, Msg) ->
        DomainID = get_chunk(ringo_util:domain_id(Domain, 0)),
        case ringo_util:best_matching_node(DomainID, get_active_nodes()) of
                {ok, Node} -> 
                        gen_server:cast({ringo_node, Node}, {match, 
                                DomainID, domain, self(), Msg});
                {error, no_nodes} ->
                        throw({http_error, 503, <<"Empty ring">>});
                Error -> throw({'EXIT', Error})
        end,
        {ok, DomainID}.

ringo_receive_chunked(Mode, Timeout) when Timeout > 60000 ->
        ringo_receive_chunked(Mode, 60000);

ringo_receive_chunked(single, Timeout) ->
        receive
                {entry, E} ->
                        {data, E}
        after Timeout ->
                throw({http_error, 408, <<"Request timeout">>})
        end;

ringo_receive_chunked(many, Timeout) ->
        {chunked, fun() ->
                receive 
                        {entry, _} = E -> E;
                        done -> done
                after Timeout -> timeout
                end
        end}.

ringo_receive(DomainID, Timeout) when Timeout > 60000 ->
        ringo_receive(DomainID, 60000);

ringo_receive(DomainID, Timeout) ->
        receive
                {ringo_reply, DomainID, Reply} -> Reply;
                Other -> Other
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

get_chunk(DomainID) ->
        case ets:lookup(chunk_cache, DomainID) of
                [] -> DomainID;
                [{_, Chunk}] -> Chunk
        end.

set_chunk(DomainID, Chunk) ->
        ets:insert(chunk_cache, {DomainID, Chunk}).

start_chunk_cache() ->
        {ok, spawn_link(fun() ->
                ets:new(chunk_cache, [named_table, public]),
                receive _ -> ok end
        end)}.
        
