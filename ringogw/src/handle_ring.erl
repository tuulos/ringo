-module(handle_ring).
-export([op/2, start_check_node_status/0]).

op("nodes", _Query) ->
        case catch ets:tab2list(node_status_table) of 
                {'EXIT', _} -> {json, []};
                L -> {json, check_ring(ringo_util:group_pairs(L))}
        end;

op("reset", _Query) ->
        catch exit(whereis(check_node_status), kill),
        {json, {ok, <<"killed">>}}.

start_check_node_status() ->
        {ok, spawn_link(fun() -> register(check_node_status, self()),
                check_node_status() end)}.
        
check_node_status() ->
        ets:new(tmptable, [named_table, bag]),
        
        Nodes = ringo_util:ringo_nodes(),
       
        {OkNodes, BadNodes} = gen_server:multi_call(Nodes,
                ringo_node, get_neighbors, 2000),
        
        ets:insert(tmptable, [{N, {neighbors, {Prev, Next}}} ||
                {N, {ok, Prev, Next}} <- OkNodes]),
        ets:insert(tmptable, [{N, {neighbors, timeout}} || N <- BadNodes]),
        
        lists:foreach(fun(Node) ->
                spawn(Node, ringo_util, send_system_status, [self()])
        end, Nodes),
        ok = collect_results(),

        % We have an obvious race condition here: Check_node_status process may
        % delete node_status_table while a query handler is accessing it. We
        % just assume that this is an infrequent event and it doesn't matter if
        % a query fails occasionally as it is unlikely that many consequent
        % update requests would fail.
        catch ets:delete(node_status_table),
        ets:rename(tmptable, node_status_table),
        check_node_status().

collect_results() ->
        receive
                {node_results, NodeStatus} ->
                        ets:insert(tmptable, NodeStatus),
                        collect_results();
                _ -> collect_results()
        after 10000 -> ok
        end.

check_ring([]) -> [];
check_ring(Nodes) ->
        % First sort the nodes according to ascending node ID
        {_, Sorted} = lists:unzip(ringo_util:sort_nodes(Nodes)),
        
        % Obtain the last node's information, which will be check against the
        % first one.
        [Last|_] = lists:reverse(Sorted),
        {_, {LastNode, LastNext}} = check_node(Last, {false, false}),
        % Check validity of each node's position in the ring, that is 
        % prev and next entries match, and construct a JSON object as the
        % result.
        {Ret, _} = lists:mapfoldl(fun check_node/2,
                {LastNode, LastNext}, Sorted),
        Ret.

check_node({Node, Attr}, {PrevNode, PrevNext}) ->
        case lists:keysearch(neighbors, 1, Attr) of
                {value, {neighbors, {Prev, Next}}} ->
                        V = (PrevNode == Prev) and (PrevNext == Node),
                        {{obj, [{node, Node}, {ok, V}|Attr]}, {Node, Next}};
                {value, {neighbors, timeout}} ->
                        {{obj, [{node, Node}, {ok, false}|Attr]}, {Node, Node}}
        end.

