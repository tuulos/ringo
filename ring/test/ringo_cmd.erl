-module(ringo_cmd).
-export([create/1, put/1]).

request(Name, Chunk, Msg) ->
        DomainID = ringo_util:domain_id(Name, Chunk),
        io:fwrite("DomainID: ~b~n", [DomainID]),
        [First|_] = Nodes = ringo_util:ringo_nodes(),
        io:fwrite("Nodes: ~w~n", [Nodes]),
        ok = gen_server:cast({ringo_node, First},
                {match, DomainID, domain, x, Msg}),
        receive
                {ringo_reply, DomainID, {ok, M}} ->
                        io:fwrite("Got ok: ~w~n", [M]);
                Other ->
                        io:fwrite("Error: ~w~n", [Other])
        after 5000 ->
                io:fwrite("Timeout~n", [])
        end.

create([Name, NReplicas]) ->
        ReplN = list_to_integer(NReplicas),
        request(Name, 0, {new_domain, Name, 0, self(), [{nrepl, ReplN}]}),
        halt().

put([Name, Key, Value]) ->
        request(Name, 0, {put, list_to_binary(Key), 
                               list_to_binary(Value), [], self()}),
        halt().

        


        
