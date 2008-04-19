-module(ringo_util).

-export([ringo_nodes/0, validate_ring/1, domain_id/1, domain_id/2,
        group_pairs/1, send_system_status/1]).

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
        <<ID:64, _/binary>> = erlang:md5([integer_to_list(Chunk), " ", Name]),
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




