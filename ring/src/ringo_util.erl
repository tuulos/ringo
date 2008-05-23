-module(ringo_util).

-export([match/2, match/4, ringo_nodes/0, validate_ring/1, domain_id/1,
         domain_id/2, group_pairs/1, send_system_status/1, get_param/2,
         format_timestamp/1, sort_nodes/1, best_matching_node/2]).

-include("ringo_node.hrl").

ringo_nodes() ->
        Hosts = case net_adm:host_file() of
                {error, _} -> throw("Could not read the hosts file");
                Lst -> Lst
        end,

        % Nodes list should be in a consistent order, hence sorting
        lists:sort(lists:flatten(lists:map(fun(Host) ->
                H = atom_to_list(Host),
                case net_adm:names(Host) of
                        {ok, Names} -> [list_to_atom(Name ++ "@" ++ H) ||
                                {Name, _} <- Names, 
                                        string:str(Name, "ringo-") > 0];
                        _ -> []
                end
        end, Hosts))).

sort_nodes(Nodes) ->
        lists:keysort(1, lists:map(fun
                ({N, _} = T) -> {ID, _} = nodename_to_nodeid(N), {ID, T};
                (N) -> nodename_to_nodeid(N)
        end, Nodes)).

nodename_to_nodeid(N) ->
        [_, X1] = string:tokens(atom_to_list(N), "-"),
        [ID, _] = string:tokens(X1, "@"),
        {erlang:list_to_integer(ID, 16), N}.

best_matching_node(_DomainID, []) -> {error, no_nodes};

best_matching_node(DomainID, [{NodeID, _}|_] = Nodes) ->
        best_matching_node(DomainID, Nodes, NodeID + 1).

best_matching_node(DomainID,
        [{NodeID, Node}|[{NextID, _}|_] = Nodes], PrevID) ->

        Match = match(DomainID, NodeID, NextID, PrevID),
        if Match ->
                {ok, Node};
        true ->
                best_matching_node(DomainID, Nodes, NodeID)
        end;

best_matching_node(_, [{_, Node}], _) -> {ok, Node}.

validate_ring(Ring) ->
        Min = lists:min(Ring),
        Prefix = lists:takewhile(fun(X) -> X =/= Min end, Ring),
        Suffix = Ring -- Prefix,
        {_, Bad} = lists:foldl(fun
                ({ID, _Node}, {}) ->
                        {ID, []};
                ({ID, _Node}, {PrevID, Rogues}) when ID > PrevID ->
                        {ID, Rogues};
                ({_ID, Node}, {PrevID, Rogues}) ->
                        {PrevID, [Node|Rogues]}
        end, {}, Suffix ++ Prefix),
        Bad. 

domain_id(Name) -> domain_id(Name, 0).
domain_id(Name, Chunk) when is_integer(Chunk), is_list(Name) ->
        <<ID:128>> = erlang:md5([integer_to_list(Chunk), " ", Name]),
        ID.

% This function converts lists of form: [{2, A}, {3, B}, {2, C}]
% to form: [{2, [A, C]}, {3, [B]}].
group_pairs(L) -> 
        lists:foldl(fun
                ({PartID, R}, []) ->
                        [{PartID, [R]}];
                ({PartID, R}, [{PrevID, Lst}|Rest]) when PrevID == PartID ->
                        [{PartID, [R|Lst]}|Rest];
                ({PartID, R}, [{PrevID, _}|_] = Q) when PrevID =/= PartID ->
                        [{PartID, [R]}|Q]
        end, [], lists:keysort(1, L)).

send_system_status(Parent) -> 
        [_, UseB, _, Used|_] = lists:reverse(
                string:tokens(os:cmd("df -h . | tail -1"), " ")),
        [_, UseI|_] = lists:reverse(
                string:tokens(os:cmd("df -i . | tail -1"), " ")),
        {value, {total, Mem}} = lists:keysearch(total, 1, erlang:memory()),
        Parent ! {node_results, {node(), {disk, {list_to_binary(UseB),
                list_to_binary(UseI), list_to_binary(Used), Mem}}}}.

format_timestamp(Tstamp) ->
        {Date, Time} = calendar:now_to_local_time(Tstamp),
        DateStr = io_lib:fwrite("~w/~.2.0w/~.2.0w ", tuple_to_list(Date)),
        TimeStr = io_lib:fwrite("~.2.0w:~.2.0w:~.2.0w", tuple_to_list(Time)),
        list_to_binary(DateStr ++ TimeStr).

get_param(Name, Default) ->
        case os:getenv(Name) of
                false -> Default;
                Value when is_integer(Default) -> list_to_integer(Value);
                Value -> Value
        end.

%%% Match serves as a partitioning function for consistent hashing. Node
%%% with ID = X serves requests in the range [X..X+1[ where X + 1 is X's
%%% successor's ID. The last node serves requests in the range [X..inf[
%%% and the first node in the range [0..X + 1[.
%%%
%%% Returns true if ReqID belongs to MyID.

match(ReqID, #rnode{myid = MyID, nextid = NextID, previd = PrevID}) ->
        match(ReqID, MyID, NextID, PrevID).

% request matches a middle node
match(ReqID, MyID, NextID, _PrevID) when ReqID >= MyID, ReqID < NextID -> true;

% request matches the last node (MyID >= NextID guarantees that the seed,
% which is connected to itself, matches)
match(ReqID, MyID, NextID, _PrevID) when MyID >= NextID, ReqID >= MyID -> true;

% request matches the first node (MyID =< PrevID guarantees that the seed,
% which is connected to itself, matches)
match(ReqID, MyID, _NextID, PrevID) when MyID =< PrevID, ReqID =< MyID -> true;

match(_, _, _, _) -> false.
