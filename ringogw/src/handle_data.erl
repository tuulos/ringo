-module(handle_data).
-export([op/2, op/3]).

-include("ringo_store.hrl").

-define(ACTIVE_NODE_UPDATE, 5000).

-define(CREATE_DEFAULTS, [{i, "nrepl", "3"}, {i, "timeout", "1000"}, 
        {b, "create", false}]).
-define(CREATE_FLAGS, [nrepl]).

-define(PUT_DEFAULTS, [{b, "overwrite", false}, {i, "timeout", "1000"}]).
-define(PUT_FLAGS, [overwrite]).

% FIXME: What happens when we send a request to a node that doesn't have a 
% valid successor in the ring? Does it die (no good, if it's trying to
% heal itself at the same time)?

% CREATE: /gw/data/domain_name?num_repl=3
% GET: /gw/data/domain_name/key_name
% PUT: /gw/data/domain_name/key_name

op(Script, Params, Data) ->
        spawn(fun update_active_nodes/0),
        op1(Script, Params, Data).

% CREATE
op1([C|_] = Domain, Params, _Data) when is_integer(C) ->
        PParams = parse_params(Params, ?CREATE_DEFAULTS),
        Flags = parse_flags(PParams, ?CREATE_FLAGS),
        error_logger:info_report({"CREATE", Domain, "WITH", PParams}),
        V = proplists:is_defined(create, PParams),
        if V ->
                {ok, DomainID} = ringo_send(Domain, 0,
                        {new_domain, Domain, 0, self(), Flags}),
                T = proplists:get_value(timeout, PParams),
                case ringo_receive(DomainID, T) of
                        {ok, {Node, _Pid}} ->
                                {ok, {ok, <<"domain created">>,
                                        {Node, formatid(DomainID)}}};
                        {error, eexist} ->
                                {ok, {error, <<"domain already exists">>}};
                        Error ->
                                error_logger:warning_report(
                                        {"Unknown create reply", Error}),
                                throw({'EXIT', Error})
                end;
        true ->
                throw({http_error, 400, <<"Create flag missing">>})
        end;

% PUT
op1([_Domain, Key], _Params, _Value) when length(Key) > ?KEY_MAX ->
        throw({http_error, 400, <<"Key too large">>});

op1([Domain, Key], Params, Value) ->
        PParams = parse_params(Params, ?PUT_DEFAULTS),
        Flags = parse_flags(PParams, ?PUT_FLAGS),

        error_logger:info_report({"PUT", Domain, "KEY", Key,
                "DATA LENGTH", size(Value)}),
        % CHUNK FIX: If chunk 0 fails, try chunk C + 1 etc.
        {ok, DomainID} = ringo_send(Domain, 0, 
                {put, list_to_binary(Key), Value, Flags, self()}),
       
        T = proplists:get_value(timeout, PParams),
        case ringo_receive(DomainID, T) of
                {ok, {Node, EntryID}} ->
                        {ok, {ok, <<"put ok">>, Node,
                                formatid(DomainID), formatid(EntryID)}};
                {error, invalid_domain} ->
                        {ok, {error, <<"Domain doesn't exist">>}};
                {error, domain_full} ->
                        % CHUNK FIX: Try next chunk
                        {ok, {error, <<"Domain full">>}};
                Error ->
                        error_logger:warning_report(
                                {"Unknown put reply", Error}),
                        throw({'EXIT', Error})
        end;

op1(_, _, _) ->
        throw({http_error, 400, <<"Invalid request">>}).

op(Script, Params) ->
        spawn(fun update_active_nodes/0),
        op1(Script, Params).

% GET
op1([Domain, Key], _Params) ->
        error_logger:info_report({"GET", Domain, "KEY", Key}),
        {ok, []};

op1(_, _) ->
        throw({http_error, 400, <<"Invalid request">>}).

%%%
%%% Ringo communication
%%%

ringo_send(Domain, Chunk, Msg) ->
        DomainID = ringo_util:domain_id(Domain, Chunk),
        case ringo_util:best_matching_node(DomainID, get_active_nodes()) of
                {ok, Node} -> 
                        error_logger:info_report(
                                {"DOMAIN", DomainID, "BEST NODE", Node}),
                        gen_server:cast({ringo_node, Node}, {match, 
                                DomainID, domain, self(), Msg});
                {error, no_nodes} ->
                        throw({http_error, 503, <<"Empty ring">>});
                Error -> throw({'EXIT', Error})
        end,
        {ok, DomainID}.

ringo_receive(DomainID, Timeout) when Timeout > 10000 ->
        ringo_receive(DomainID, 10000);

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
        Pid = whereis(active_node_updater),
        if Pid == undefined ->
                active_nodes();
        true ->
                Pid ! {get, self()},
                receive
                        {nodes, Nodes} ->
                                error_logger:info_report({"Sorted Nodes", Nodes}),
                                Nodes
                after 100 ->
                        throw({http_error, 408, "Active node request timeout"})
                end
        end.

update_active_nodes() ->
        case catch register(active_node_updater, self()) of
                {'EXIT', _} -> ok;
                _ -> {ok, _} = 
                        timer:send_interval(?ACTIVE_NODE_UPDATE, update),
                        update_active_nodes(active_nodes())
        end.

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
        
