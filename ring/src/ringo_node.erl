%%%
%%% Everything in this module aims at maintaining the following ring
%%% invariant:
%%%
%%% ********************************************************************
%%% All active nodes [1] belong to a single ring [2]. Nodes are assigned
%%% to the ring according to ascending node IDs. The node with the
%%% largest ID connects back to the node with the smallest ID [3].
%%% ********************************************************************
%%% 
%%% [1] Since there is no single master overlooking all active
%%% nodes, each node takes care of itself and aims at maintaining
%%% the invariant. In this task, functions join_existing_ring and
%%% find_new_successor are central.
%%%
%%% [2] Since there is not single master overlooking all active nodes,
%%% making sure that all the nodes belong to a single ring is a non-trivial
%%% task.
%%%
%%% A central concept here is the Single Right Ring (SRR): It is the ring
%%% that contains the node that has the globally smallest NodeID. Since
%%% NodeIDs are positive integers, we know that a node with the smallest
%%% ID exists, although it might not be active currently.
%%% 
%%% Function check_parallel_rings is central in finding and maintaining
%%% the SRR. Its job is to find and kill all active nodes that belong to
%%% some other ring that the SRR.
%%% 
%%% [3] The integrity of a ring is maintained by check_ring_route(). It 
%%% circulates through all the nodes in the ring periodically and ensures
%%% that the invariant [3] holds, and if not, kills the violating nodes.
%%%

-module(ringo_node).
-behaviour(gen_server).

-export([start_link/1, check_ring_route/0, check_parallel_rings/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, 
        terminate/2, code_change/3]).

-record(rnode, {myid, previd, nextid, prevnode, nextnode, route}).


-define(RING_ROUTE_INTERVAL, 10 * 1000000). % microseconds
-define(RING_ZOMBIE_LIMIT, 30 * 1000000). % microseconds
-define(PARALLEL_CHECK_INTERVAL, 30 * 1000). % milliseconds

start_link(Id) ->
        case gen_server:start_link({local, ringo_node},
                ringo_node, [Id], [{debug, [trace, log]}]) of
                {ok, Server} -> {ok, Server};
                {error, {already_started, Server}} -> {ok, Server}
        end,
        error_logger:info_report({"Node", node(), "started"}),
        ok = gen_server:call(Server, new_ring_route),
        ok = gen_server:call(Server, {op, join_existing_ring, {}, node(), 0}),
        {ok, Server}.


init([Id]) ->
        RTime = round(?RING_ROUTE_INTERVAL / 1000 + random:uniform(10000)),
        {ok, _T1} = timer:apply_interval(RTime, 
                ringo_node, check_ring_route, []),
        PTime = ?PARALLEL_CHECK_INTERVAL + random:uniform(10000),
        {ok, _T2} = timer:apply_interval(PTime,
                ringo_node, check_parallel_rings, []),

        {ok, #rnode{myid = Id, previd = Id, nextid = Id, prevnode = node(),
                    nextnode = node(), route = {now(), []}}}.


%handle_call(connect_to_ring, _From, #rnode{myid = MyID} = R) ->

        % Find the seed node(s)
%        Nodes = lists:dropwhile(fun(Node) ->
%                {ok, {_, Ring}} = gen_server:call({ringo_node, Node},
%                        get_ring_route),
%                length(Ring) == 0
%        end, ringo_util:ringo_nodes() -- [node()]),

%        if length(Nodes) > 0 ->
%                [Node|_] = Nodes,
%                error_logger:info_report({"Using seed node: ", Node}),
%                gen_server:cast({ringo_node, Node},
%                        {match, MyID, new_node, node(), []}),
%                {reply, ok, R};
%        true ->
%                error_logger:warning_report({"Could not find a seed node"}),
%                {stop, normal, R}
%        end;

handle_call({update_ring_route, Ring}, _From, R) ->
        {reply, ok, R#rnode{route = {now(), Ring}}};

handle_call(get_ring_route, _From, #rnode{route = Ring} = R) ->
        {reply, {ok, Ring}, R};

handle_call(new_ring_route, _From, R) ->
        spawn_link(fun() -> record_ring_route(R) end),
        {reply, ok, R};

handle_call(get_previous, _From, #rnode{prevnode = {Prev} = R) ->
        {reply, {ok, Prev}, R};

%%% Place a node in the ring.
%%% Consider the following setting: X -> N -> Y
%%% where N is the new node, X its predecessor and Y its successor.
%%%
%%% Two scenarios:
%%%
%%% - New node join the ring:
%%%   An existing node X calls N:assign_node and informs it about X and Y. 
%%%   N connects to Y:assign_node and assigns itself as the Y's previous.
%%%
%%% - An existing node N loses a successor:
%%%   N calls Y:assign_node and assigns itself as the Y's previous.

% Set new predecessor
handle_call({assign_node, NewPrev, NewPrevID, none, none}, _From,
        #rnode{myid = MyID, prevnode = OldPrev} = R) ->
        
        monitor_node(OldPrev, false),
        monitor_node(NewPrev, true),
        {reply, {ok, MyID}, R#rnode{previd = NewPrevID, prevnode = NewPrev}};

% Set new successor
handle_call({assign_node, none, none, NewNext, NewNextID}, _From,
        #rnode{myid = MyID, nextnode = OldNext} = R) ->
        
        monitor_node(OldNext, false),
        monitor_node(NewNext, true),
        {reply, {ok, MyID}, R#rnode{nextid = NewNextID, nextnode = NewNext}};

% Change both the successor and predecessor for this node. Update all the 
% affected nodes.
handle_call({assign_node, NewPrev, NewPrevID, NewNext, NewNextID}, _From, 
        #rnode{myid = MyID, prevnode = OldPrev, nextnode = OldNext} = R) ->

        {ok, NewNextID} = gen_server:call({ringo_node, NewNext},
                {assign_node, node(), MyID, none, none}, 250),
        
        monitor_node(OldNext, false),
        monitor_node(NewNext, true),

        % update only successor?
        if NewPrev =/= current ->
                {ok, NewPrevID} = gen_server:call({ringo_node, NewPrev},
                        {assign_node, none, none, node(), MyID}, 250),
                monitor_node(OldPrev, false),
                monitor_node(NewPrev, true),
                {reply, {ok, MyID}, 
                        R#rnode{previd = NewPrevID, prevnode = NewPrev,
                                nextid = NewNextID, nextnode = NewNext}};
        true ->
                {reply, {ok, MyID}, 
                        R#rnode{nextid = NewNextID, nextnode = NewNext}}
        end;
                
handle_call({op, Op, Args, From, ReqID}, _, RNode) ->
        spawn_link(fun() -> op(Op, Args, From, ReqID, RNode) end),
        {reply, ok, RNode}.

handle_cast({kill_node, Reason}, RNode) ->
        error_logger:warning_report({"Kill node requested", Reason}),
        {stop, node_killed, RNode};

% Length(Ring) > 0 check ensures that no operation is performed until the
% node is a valid member of the ring.
handle_cast({match, ReqID, Op, From, Args} = Req, 
        #rnode{route = {_, Ring}} = R) when length(Ring) > 0 ->
        
        Match = match(ReqID, R),
        if Match ->
                spawn_link(fun() -> op(Op, Args, From, ReqID, R) end),
                {noreply, R};
        true ->
                gen_server:cast({ringo_node, R#rnode.nextnode}, Req),
                {noreply, R}
        end;

handle_cast({match, _ReqID, _Op, _From, _Args}, R) ->
        {noreply, R}.

%%% What to do when a neighbor dies? Only predecessor must react to its
%%% successor's death. It is responsible for finding a new successor
%%% for itself.

% Node == NextNode case must come before Node == PrevNode:
% If NextNode == PrevNode, we must start find_new_successor.
handle_info({nodedown, Node}, #rnode{nextnode = NextNode} = R)
        when Node == NextNode ->
        error_logger:info_report({"Next node", NextNode, "down"}),
        spawn_link(fun() -> find_new_successor(R) end),
        {noreply, R};

handle_info({nodedown, Node}, #rnode{prevnode = PrevNode} = R)
        when Node == PrevNode ->
        error_logger:info_report({"Previous node", PrevNode, "down"}),
        {noreply, R};
                
handle_info({nodedown, Node}, R) ->
        error_logger:info_report({"Unknown node", Node, "down"}),
        {noreply, R}.


%%% A new node at From wants to join the ring. This node should become
%%% its predecessor and this node's successor becomes its successor.

% Special case: New node already attached to this node: duplicate request
op(new_node, _Args, From, ReqID, #rnode{nextnode = Next, nextid = NextID})
        when From == Next, ReqID == NextID -> ok;

% Special case: New node is this node: duplicate request
op(new_node, _Args, From, _ReqID, _R) when From == node() -> ok;

% Special case (rare): New node's ID equals to my ID. Kill the new node.
op(new_node, _Args, From, ReqID, #rnode{myid = MyID}) when ReqID == MyID ->
        error_logger:warning_report({"Duplicate ID", ReqID, "From", From}),
        gen_server:cast({ringo_node, From}, {kill_node,
                "Duplicate ID at " ++ atom_to_list(node())});

% Special case: This is the first node in the ring and the new node has a 
% lower ID, thus it should be the first node. Redirect the request to the
% last node in the ring which becomes the new node's predecessor.
op(new_node, Args, From, ReqID, #rnode{myid = MyID, previd = PrevID} = R)
        when ReqID < MyID, MyID < PrevID ->

        ok = gen_server:call({ringo_node, R#rnode.prevnode},
                        {op, new_node, Args, From, ReqID});

% Normal case: Call the new node and inform it about its new predecessor and
% successor. If succesful, initiate ring route recording.
op(new_node, _Args, From, ReqID,
        #rnode{myid = MyID, nextnode = Next, nextid = NextID}) ->

        FindActive = (catch is_process_alive(whereis(find_new_successor))),
        if FindActive == true ->
                gen_server:cast({ringo_node, From}, {kill_node,
                        "Node finding a new successor. Try again later."});
        true ->
                case catch gen_server:call({ringo_node, From}, 
                        {assign_node, node(), MyID, Next, NextID}) of
                
                        {ok, ReqID} -> gen_server:call(ringo_node, 
                                        new_ring_route);
                        Error -> gen_server:cast(ringo_node, {kill_node,
                                {"Joining a new node failed. New node: ",
                                        From, "Error", Error}})
                end
        end;

%%% op(circulate) provides a generic way to run a function, NodeOp, on each 
%%% node of the ring. Circulate also collects a list of nodes in the ring,
%%% validates predecessor and successor of each node, and in the end makes
%%% sure that the list is sorted in ascending order.
%%%
%%% Circulate is used by record_ring_route and publish_ring_route.

% Recording done: Back where we started. Validate the list and call the
% finalizer.
op(circulate, {Ring, _NodeOp, EndOp}, From, _ReqID, RNode)
        when length(Ring) > 0, From == node() ->
        RRing = lists:reverse(Ring),
        RogueNodes = ringo_util:validate_ring(RRing),
        gen_server:abcast(RogueNodes, ringo_node,
                {kill_node, "Node is misplaced"}),
        ok = EndOp(RRing, RNode);

% Normal case: Previous item in the ring list equals to this node's 
% predecessor.
op(circulate, {[{PrevID, _}|_], _, _} = Args, From, 0,
        #rnode{previd = MyPrevID} = R) when PrevID == MyPrevID ->
        
        op(circulate, Args, From, 1, R);

% Error case: Previous item in the ring list doesn't equal to this node's
% predecessor. Killing the unknown predecessor.
op(circulate, {[{PrevID, Prev}|_], _, _}, _From, 0,
        #rnode{previd = MyPrevID, prevnode = MyPrev}) ->
        
        error_logger:warning_report({"Circulate: Wrong predecessor.",
                "Expected", {MyPrevID, MyPrev}, "got", {PrevID, Prev}}),
        gen_server:cast({ringo_node, Prev},
                {kill_node, "Successor doesn't recognize this node"});

% Forward request to the next node in the ring. If the call fails, find a 
% new successor.
op(circulate, {Ring, NodeOp, EndOp}, From, 1,
        #rnode{myid = MyID, nextnode = Next} = R) ->

        case catch NodeOp(Ring, R) of
                ok -> ok;
                Error -> error_logger:warning_report(
                        {"Circulate: NodeOp failed", Error}),
                        gen_server:cast(ringo_node, {kill_node, "NodeOp failed"}),
                        throw(Error)
        end,
        
        error_logger:info_report({"Calling next", Next}),

        case catch gen_server:call({ringo_node, Next},
                {op, circulate, {[{MyID, node()}|Ring], NodeOp, EndOp},
                        From, 0}) of

                ok -> ok;
                Error2 -> error_logger:warning_report(
                         {"Circulate: Successor failed", Error2}),
                         find_new_successor(R)
        end;

%%% Since ringo_nodes() returns available nodes in a consistent order, the
%%% ring should grow around a single seed. There might be multiple seeds in
%%% the first place, only one of them will survive. The others will get killed
%%% by check_parallel_rings().

op(join_existing_ring, _, _, _, #rnode{myid = MyID, route = Route}) ->
        Candidates = ringo_util:ringo_nodes() -- [node()],
        join_existing_ring(MyID, Candidates, Route).


%%% Record_ring_route circulates through the ring, records the nodes
%%% seen, and when retuned to the starting node, initiates publish_ring_route.
%%% Publish_ring_route circulates through the ring and updates the newly
%%% recorded ring route list on each node.

record_ring_route(RNode) ->
        NodeOp = fun(_, _) -> ok end,
        EndOp = fun(EndRing, EndRNode) -> 
                publish_ring_route(EndRing, EndRNode) end,
        op(circulate, {[], NodeOp, EndOp}, node(), 1, RNode).

publish_ring_route(Ring, RNode) ->
        NodeOp = fun(_, _) ->
                gen_server:call(ringo_node, {update_ring_route, Ring}) end,
        EndOp = fun(_, _) -> ok end,
        op(circulate, {[], NodeOp, EndOp}, node(), 1, RNode). 
                

%%% Match serves as a partitioning function for consistent hashing. Node
%%% with ID = X serves requests in the range [X..X+1[ where X + 1 is X's
%%% successor's ID. The last node serves requests in the range [X..inf[
%%% and the first node in the range [0..X+1[.
%%%
%%% Returns true if ReqID belongs to MyID.

% request matches a middle node
match(ReqID, #rnode{myid = MyID, nextid = NextID})
        when ReqID >= MyID, ReqID < NextID -> true;
% request matches the last node (MyID >= NextID guarantees that the seed,
% which is connected to itself, matches)
match(ReqID, #rnode{myid = MyID, nextid = NextID})
        when MyID >= NextID, ReqID >= MyID -> true;
% request matches the first node (MyID =< PrevID guarantees that the seed,
% which is connected to itself, matches)
match(ReqID, #rnode{myid = MyID, previd = PrevID}) 
        when MyID =< PrevID, ReqID =< MyID -> true;
match(_, _) ->
        false.

%%% Logic in starting a node is as follows: We would like to connect to
%%% the Single Right Ring (SRR) right away, that is, to the ring that
%%% includes the globally smallest node ID (see check_parallel_rings()
%%% below).
%%% 
%%% However, we don't know which of the nodes belongs to that ring,
%%% or if the SRR currently exists at all. Hence our best guess is to
%%% start connecting to active nodes, starting from the one with the
%%% smallest ID, and hope that they will connect back to this node (call
%%% assign_node) in a reasonable time (5 seconds). If they do (i.e.
%%% length(route) > 1), we stop looking for other nodes.
%%%
%%% While we are trying to connect to other active nodes, any node may
%%% connect to us at the same time. If it so happens (i.e. length(route) >
%%% 1), we stop looking for other nodes.
%%%
%%% In any case, the end result is that we may be or may not
%%% be part of the SRR. This ambiguity is finally resolved by
%%% check_parallel_rings(). If we are the SRR, others will get killed.
%%% If not, others will eventually kill us and the process begins again.

join_existing_ring(_, _, Route) when length(Route) > 1 ->
        error_logger:info_report({"Join_existing_ring exits.",
                "Node is already part of a ring", Route});

join_existing_ring(_, [], _) ->
        error_logger:info_report({"Join_existing_ring exits.",
                "No more candidates to connect to."});

join_existing_ring(MyID, [Node|Nodes], _) ->
        
        error_logger:info_report({"Sending join request to", Node}),
        
        catch gen_server:cast({ringo_node, Node},
                {match, MyID, new_node, node(), []}),
       
        % wait for 5 seconds
        receive
        after 5000 -> ok
        end,

        {ok, {_, Route}} = gen_server:call(ringo_node, get_ring_route),
        join_existing_ring(MyID, Nodes, Route).
        

%%% Find_new_successor tries to connect this node to the next available
%%% successor node in the ring. This function is called when a successor
%%% dies. The function must be run in a process of its own.

% First check that the successor finder isn't already running
find_new_successor(Args) ->
        case catch register(find_new_successor, self()) of
                true -> error_logger:info_report(
                        "Starting to find a new successor"),
                        find_new_successor2(Args);
                _ -> error_logger:info_report(
                        "find_new_successor already running")
        end.

% Find successor candidates
find_new_successor2(#rnode{myid = MyID, route = {_, Ring}, nextnode = Next}) ->
        gen_server:cast({ringo_node, Next},
                {kill_node, "Predecessor finds a new successor"}),

        {NextNodes, PrevNodes} = lists:partition(fun({NodeID, _Node}) ->
                NodeID > MyID end, Ring -- [{MyID, node()}]),
        
        Candidates = lists:filter(fun({_NodeID, Node}) ->
                net_adm:ping(Node) == pong
        end, NextNodes ++ PrevNodes),
        find_new_successor3(Candidates).

% Try to attach to a successor candidate.

find_new_successor3([]) ->
        gen_server:cast(ringo_node, {kill_node,
                "Couldn't find a new successor for this node"});

find_new_successor3([{NodeID, Node}|Rest]) ->

        case catch gen_server:call(ringo_node,
                {assign_node, current, current, Node, NodeID}) of

                {ok, _} -> gen_server:call(ringo_node, new_ring_route);
                Error -> error_logger:info_report(
                        {"Couldn't connect to a new successor at ",
                                Node, Error}),
                        gen_server:cast({ringo_node, Node}, {kill_node,
                        "Couldn't make this node a new successor."}),
                        find_new_successor3(Rest)
        end.

%%% Check_ring_route runs periodically to ensure that the node is still
%%% a valid part of the ring (ref. ring invariant [1]). This function is
%%% run in a process of its own.

check_ring_route() ->
        {ok, {T, _}} = gen_server:call(ringo_node, get_ring_route),
        D = timer:now_diff(now(), T),
        if D > ?RING_ZOMBIE_LIMIT ->
                gen_server:cast(ringo_node, {kill_node,
                        {"Node hasn't received a new ring list for", 
                        D / 1000000, "seconds. Killing zombie."}});
        D > ?RING_ROUTE_INTERVAL ->
                gen_server:call(ringo_node, new_ring_route);
        true -> ok
        end.

%%% Check_parallel_rings runs periodically to ensure that no parallel
%%% rings exist besides this ring (ref. ring invariant [2]).
%%% If a valid parallel ring (i.e. circulate completes) is detected, the
%%% ring with the largest smallest node ID will be killed, which might be
%%% this ring. 
%%%
%%% This function is run in a process of its own.
%%%
%%% Note that parallel ring detection requires an up-to-date hosts list,
%%% i.e. net_adm:host_file() must return all active hosts.

check_parallel_rings() ->
        {ok, {_, R}} = gen_server:call(ringo_node, get_ring_route),
        {_, Ring} = lists:unzip(R),
        error_logger:warning_report({"Ring", Ring, "Other", ringo_util:ringo_nodes()}),

        Nodes = ringo_util:ringo_nodes(),
        Aliens = Nodes -- Ring,
        Unknown = Ring -- Nodes,

        if length(Ring) > 0, length(Aliens) > 0 ->
                error_logger:warning_report({"Aliens detected:", Aliens}),
                Alien = lists:nth(random:uniform(length(Aliens)), Aliens),
                Node = node(),
                NodeOp = fun(_, _) -> ok end,
                EndOp = fun(EndRing, _) ->
                                {ok, {_, OtherRing}} = gen_server:call(
                                        {ringo_node, Node}, get_ring_route),
                                kill_parallel_ring(EndRing, OtherRing)
                        end,
                gen_server:call({ringo_node, Alien},
                        {op, circulate, {[], NodeOp, EndOp}, Alien, 1});

        length(Unknown) > 0 ->
                error_logger:warning_report({"Unknown nodes in the ring:",
                        Unknown, " -- Host file not up to date?"});
        true -> ok
        end.

% If the rings don't overlap, i.e. their difference is empty, kill
% the ring which has the largest value of the smallest node ID. This
% guarantees that the Single Right Ring will never be killed.
kill_parallel_ring(RingA, RingB) ->
        D = RingA -- RingB,
        if length(D) == length(RingA) ->
                {_, KillRing} = lists:max([{lists:min(RingA), RingA},
                                           {lists:min(RingB), RingB}]),
                Nodes = [Node || {_, Node} <- KillRing],
                error_logger:warning_report({"Parallel ring detected.",
                        "Killing ring: ", Nodes}),
                gen_server:abcast(Nodes, ringo_node,
                        {kill_node, "Parallel ring"});
        true -> ok
        end.


%%% callback stubs

terminate(_Reason, _State) -> {}.

code_change(_OldVsn, State, _Extra) -> {ok, State}.
