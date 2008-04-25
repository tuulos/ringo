-module(handle_data).
-export([op/2, op/3]).

-define(ACTIVE_NODE_UPDATE, 5000). % microseconds
-define(CREATE_DEFAULTS, [{i, "nrepl", "3"}, {i, "timeout", "1000"},
        {x, "create", undefined}]).

% CREATE: /gw/data/domain_name?num_repl=3
% GET: /gw/data/domain_name/key_name
% PUT: /gw/data/domain_name/key_name

op(Script, Params, Data) ->
        spawn(fun update_active_nodes/0),
        op1(Script, Params, Data).

% CREATE
op1([C|_] = Domain, Params, _Data) when is_integer(C) ->
        PParams = parse_params(Params, ?CREATE_DEFAULTS),
        error_logger:info_report({"CREATE", Domain, "WITH", PParams}),
        V = proplists:is_defined(create, PParams),
        if V ->
                {ok, DomainID} = ringo_send(Domain, 0,
                        {new_domain, Domain, 0, self(), Params}),
                T = proplists:get_value(timeout, PParams),
                case ringo_receive(DomainID, T) of
                        {ok, {_Node, _Pid}} ->
                                {ok, {ok, <<"domain created">>}};
                        {error, eexist} ->
                                {ok, {error, <<"domain already exists">>}}
                end;
        true ->
                throw({http_error, 400, "Create flag missing"})
        end;

% PUT
op1([Domain, Key], _Params, Data) ->
        error_logger:info_report({"PUT", Domain, "KEY", Key,
                "DATA LENGTH", size(Data)}),
        {ok, []}.


op(Script, Params) ->
        spawn(fun update_active_nodes/0),
        op1(Script, Params).

% GET
op1([Domain, Key], _Params) ->
        error_logger:info_report({"GET", Domain, "KEY", Key}),
        {ok, []}.

parse_params(Params, Defaults) ->
        [X || {_, W} = X <- lists:map(fun
               ({i, K, V}) ->
                        {list_to_atom(K), list_to_integer(
                                proplists:get_value(K, Params, V))};
               ({_, K, V}) ->
                        {list_to_atom(K), proplists:get_value(K, Params, V)}
        end, Defaults), W =/= undefined].

ringo_send(Domain, Chunk, Msg) ->
        DomainID = ringo_util:domain_id(Domain, Chunk),
        case ringo_util:best_matching_node(DomainID, get_active_nodes()) of
                {ok, Node} -> 
                error_logger:info_report({"DOMAIN", DomainID, "BEST NODE", Node}),
                gen_server:cast({ringo_node, Node},
                                {match, DomainID, domain, self(), Msg});
                {error, no_nodes} -> throw({http_error, 503, "Empty ring"});
                Error -> throw({'EXIT', Error})
        end,
        {ok, DomainID}.

ringo_receive(DomainID, Timeout) when Timeout > 10000 ->
        ringo_receive(DomainID, 10000);

ringo_receive(DomainID, Timeout) ->
        receive
                {ringo_reply, DomainID, Reply} ->
                        error_logger:info_report({"Got reply", Reply}),
                        Reply
        after Timeout ->
                throw({http_error, 503, "Request timeout"})
        end.

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
                after 100 -> throw(timeout)
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
                {get, From} -> From ! {nodes, Nodes};
                update -> update_active_nodes(active_nodes())
        end.

active_nodes() ->
        ringo_util:sort_nodes(ringo_util:ringo_nodes()).
        
