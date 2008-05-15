-module(handle_domains).
-export([op/2, start_check_domains/0]).

op("node", [{"name", NodeS}|_]) ->
        Node = list_to_existing_atom(NodeS),
        {json, [fetch_domaininfo(X) || X <- infopack_lookup({node, Node})]};

op("domain", [{"name", NameS}|_]) ->
        Name = list_to_binary(NameS),
        {json, [fetch_domaininfo(X) || X <- infopack_lookup({name, Name})]};

op("domain", [{"id", [$0, $x|IdS]}|_]) ->
        op("domain", [{"id", integer_to_list(
                erlang:list_to_integer(IdS, 16))}]);

op("domain", [{"id", IdS}|_]) ->
        DomainID = list_to_integer(IdS),
       
        [{_, {Name, _, Chunk}}|_] = Repl = infopack_lookup({id, DomainID}),
        Nodes = [Node || {_, {_, Node, _}} <- Repl],
        
        gen_server:abcast(Nodes, ringo_node, {{domain, DomainID},
                {get_status, self()}}),
        Status = receive_domainstatus([], length(Nodes)),

        {json, [list_to_binary(erlang:integer_to_list(DomainID, 16)),
                Name, Chunk, lists:map(fun(N) ->
                case lists:keysearch(N, 1, Status) of
                        {value, {_, S}} -> {obj, [{node, N}|S]};
                        _ -> {obj, [{node, N}, {error, noreply}]}
                end
        end, Nodes)]};

op("reset", _Query) ->
        catch exit(whereis(check_domains), kill),
        {json, {ok, <<"killed">>}}.

fetch_domaininfo({_, DomainID}) ->
        [{_, {Name, Node, Chunk}}|_] = Repl = infopack_lookup({id, DomainID}),
        Active = case catch ets:lookup(active_domains, DomainID) of
                [{_, A}] -> A;
                _ -> false
        end,
        {list_to_binary(erlang:integer_to_list(DomainID, 16)),
                Name, Node, Chunk, Active, length(Repl)}.

start_check_domains() ->
        {ok, spawn_link(fun() -> register(check_domains, self()),
                check_domains() end)}.

infopack_lookup(Key) ->
        case ets:lookup(infopack_cache, Key) of
                [] -> throw({http_error, 404, <<"Unknown domain">>});
                X -> X
        end.

check_domains() ->
        catch ets:new(infopack_cache, [named_table, bag]),
        catch ets:new(domains_t, [named_table]),

        Nodes = ringo_util:ringo_nodes(),
        lists:foreach(fun(Node) ->
                case catch gen_server:call({ringo_node, Node},
                        get_domainlist) of
                        {ok, Domains} -> insert_and_index(Node, Domains);
                        Error -> error_logger:warning_report(
                                {"Couldn't get domainlist from", Node, Error})
                end
        end, Nodes),
        catch ets:delete(active_domains),
        ets:rename(domains_t, active_domains),
        timer:sleep(10000),
        check_domains().

insert_and_index(Node, Domains) ->
        lists:foreach(fun({DomainID, _}) ->
                case ets:member(infopack_cache, DomainID) of
                        false -> get_infopack(Node, DomainID);
                        true -> ok
                end
        end, Domains),
        ets:insert(domains_t, Domains).

get_infopack(Node, DomainID) ->
        case catch gen_server:call({ringo_node, Node},
                        {get_infopack, DomainID}) of
                {ok, Nfo} ->
                        {value, {name, Name}} = lists:keysearch(name, 1, Nfo),
                        {value, {chunk, Chunk}} = lists:keysearch(chunk, 1, Nfo),
                        ets:insert(infopack_cache, {{name, Name}, DomainID}),
                        ets:insert(infopack_cache, {{node, Node}, DomainID}),
                        ets:insert(infopack_cache, {{id, DomainID}, {Name, Node, Chunk}});
                Error -> error_logger:warning_report({"Couldn't get infopack for",
                        DomainID, "from", Node, Error})
        end.

receive_domainstatus(L, 0) -> L;
receive_domainstatus(L, N) ->
        receive
                {status, Node, E} ->
                        receive_domainstatus([{Node, E}|L], N - 1);
                _ ->
                        receive_domainstatus(L, N)
        after 2000 -> L
        end.
                


        






        


