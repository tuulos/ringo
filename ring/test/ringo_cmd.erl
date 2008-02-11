-module(ringo_cmd).
-export([create/1]).

request(Name, Chunk, Msg) ->
        DomainID = ringo_util:domain_id(Name, Chunk),
        io:fwrite("DomainID: ~b~n", [DomainID]),
        [First|_] = Nodes = ringo_util:ringo_nodes(),
        io:fwrite("Nodes: ~w~n", [Nodes]),
        ok = gen_server:cast({ringo_node, First},
                {match, DomainID, domain, x, Msg}),
        receive
                {ok, Node} ->
                        io:fwrite("Got ok from ~w~n", [Node]);
                Other ->
                        io:fwrite("Error: ~w~n", [Other])
        after 5000 ->
                io:fwrite("Timeout~n", [])
        end.

create([Name, Chunk, NReplicas]) ->
        ChunkN = list_to_integer(Chunk),
        ReplN = list_to_integer(NReplicas),
        request(Name, ChunkN, {new_domain, Name, ChunkN,
                ReplN, {node(), self()}}),
        halt().

                
        


        
