-module(handle_domains).
-export([handle/2]).

-define(HTTP_HEADER, "HTTP/1.1 200 OK\n"
                     "Status: 200 OK\n"
                     "Content-type: text/plain\n\n").

op("node", Query) ->
        {value, {_, NodeS}} = lists:keysearch("name", 1, Query),
        Node = list_to_existing_atom(NodeS),
        {ok, [fetch_domaininfo(X) || X <- 
                ets:lookup(infopack_cache, {node, Node})]};

op("domain", Query) ->
        {value, {_, NameS}} = lists:keysearch("name", 1, Query),
        Name = list_to_binary(NameS),
        {ok, [fetch_domaininfo(X) || X <-
                ets:lookup(infopack_cache, {name, Name})]};

op("reset", _Query) ->
        exit(whereis(check_domains), kill),
        {ok, killed}.

handle(Socket, Msg) ->
        V = (catch is_process_alive(whereis(check_domains))),
        if V -> ok;
        true -> spawn(fun() ->
                register(check_domains, self()),
                check_domains()
                end)
        end,

        {value, {_, Script}} = lists:keysearch("SCRIPT_NAME", 1, Msg),
        {value, {_, Query}} = lists:keysearch("QUERY_STRING", 1, Msg),
        
        Op = lists:last(string:tokens(Script, "/")),
        {ok, Res} = op(Op, httpd:parse_query(Query)),
        gen_tcp:send(Socket, [?HTTP_HEADER, json:encode(Res)]).

fetch_domaininfo({_, DomainID}) ->
        [{_, {Name, Node, Chunk}}] =
                ets:lookup(infopack_cache, {id, DomainID}),
        Active = case catch ets:lookup(active_domains, DomainID) of
                [{_, A}] -> A;
                _ -> false
        end,
        {DomainID, Name, Node, Chunk, Active}.

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
                        



        






        


