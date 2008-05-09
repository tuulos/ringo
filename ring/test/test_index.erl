-module(test_index).
-export([buildindex_test/1, serialize_test/1, kv_test/0, indexuse_test/1]).
-export([lrucache_test/0]).

write_data(NumKeys) ->
        S = now(),
        %{ok, DB} = file:open("test_data/indexdata", [write, raw]),
        {ok, DB} = bfile:fopen("test_data/indexdata", "w"),
        Keys = lists:map(fun(_) ->
                EntryID = random:uniform(4294967295),
                N = random:uniform(NumKeys),
                Key = <<"KeyYek:", N:32>>,
                Entry = ringo_writer:make_entry(EntryID, Key,
                                <<"ValueEulav:", N:32>>, []),
                ok = ringo_writer:write_entry("test_data", DB, Entry),
                Key
        end, lists:seq(1, 10000)),
        io:fwrite("Writing ~b items (max key ~b) took ~bms~n",
                [10000, NumKeys, round(timer:now_diff(now(), S) / 1000)]),
        bfile:fclose(DB),
        Keys.

buildindex_test(Keys) when is_list(Keys) -> 
        buildindex_test(list_to_integer(lists:flatten(Keys)));

buildindex_test(Keys) ->
        write_data(Keys),
        S = now(),
        {_, Dex, _} = ringo_index:build_index("test_data/indexdata", 0, inf),
        Ser = ringo_index:serialize(Dex),
        {memory, Mem} = erlang:process_info(self(), memory),
        io:fwrite("Building index took ~bms~n",
                [round(timer:now_diff(now(), S) / 1000)]),
        io:fwrite("Process takes ~bK memory. Index takes ~bK.~n",
                [Mem div 1024, iolist_size(Ser) div 1024]),
        io:fwrite("~b keys in the index~n", [gb_trees:size(Dex)]),
        halt().


serialize_test(Keys) when is_list(Keys) -> 
        serialize_test(list_to_integer(lists:flatten(Keys)));

serialize_test(Keys) ->
        write_data(Keys),
        {_, Dex, _} = ringo_index:build_index("test_data/indexdata", 0, inf),
        S = now(),
        Ser = iolist_to_binary(ringo_index:serialize(Dex)),
        io:fwrite("Serialization took ~bms~n",
                [round(timer:now_diff(now(), S) / 1000)]),
        io:fwrite("Serialized index takes ~bK~n", [iolist_size(Ser) div 1024]),
        io:fwrite("~b keys in the index~n", [gb_trees:size(Dex)]),
        S2 = now(),
        lists:foreach(fun(ID) ->
                %io:fwrite("ID ~b~n", [ID]),
                {ID, [_|_]} = ringo_index:find_key(ID, Ser)
        end, gb_trees:keys(Dex)),
        io:fwrite("All keys found ok in ~bms~n",
                [round(timer:now_diff(now(), S2) / 1000)]),
        halt().

kv_test() ->
        lists:foreach(fun(I) ->
                L = [{X, X + 1} || X <- lists:seq(1, I)],
                S = bin_util:encode_kvsegment(L),
                lists:foreach(fun({K, _} = R) ->
                        R = bin_util:find_kv(K, S)
                end, L)
                %io:fwrite("~b-item segment ok~n", [I])
        end, lists:seq(1, 1000)),
        io:fwrite("Binary search for all segments ok~n", []),
        halt().

lrucache_test() ->
        S = now(),
        LRU = lists:foldl(fun(I, LRUx) ->
                lrucache:update(I, LRUx)
        end, lrucache:new(), lists:seq(1, 10000)),
        io:fwrite("10000 items updated in ~bms~n",
                [round(timer:now_diff(now(), S) / 1000)]),
        
        S0 = now(),
        LRU0 = lists:foldl(fun(I, LRUx) ->
                {I, LRUy} = lrucache:get_lru(LRUx), LRUy
        end, LRU, lists:seq(1, 10000)),
        io:fwrite("10000 lrus got in ~bms~n",
                [round(timer:now_diff(now(), S0) / 1000)]),
        
        true = lrucache:is_empty(LRU0),
        io:fwrite("is_empty() works~n", []),
        
        LRU1 = lists:foldl(fun(I, LRUx) ->
                lrucache:update(I, LRUx)
        end, lrucache:new(), lists:seq(1, 10000)),
        
        io:fwrite("Testing random access..~n", []),
        Hits = dict:from_list([{I, true} || I <- lists:seq(1, 10000)]),
        {Hits0, LRU2} = lists:foldl(fun(_, {Hx, LRUx}) ->
                N = random:uniform(9000),
                {dict:store(N, false, Hx), lrucache:update(N, LRUx)}
        end, {Hits, LRU1}, lists:seq(1, 10000)),

        lists:foldl(fun(_, LRUx) ->
                {I, LRUy} = lrucache:get_lru(LRUx),
                true = dict:fetch(I, Hits0), LRUy
        end, LRU2, lists:seq(1, 1000)),
        io:fwrite("Random access works~n", []),
        halt().

indexuse_test(NumKeys) when is_list(NumKeys) -> 
        indexuse_test(list_to_integer(lists:flatten(NumKeys)));
indexuse_test(NumKeys) ->
        Keys = write_data(NumKeys),
        {_, Dex, _} = ringo_index:build_index("test_data/indexdata", 0, inf),
        %{ok, DB} = file:open("test_data/indexdata", [read, raw, binary]),
        {ok, DB} = bfile:fopen("test_data/indexdata", "r"),
        Ser = iolist_to_binary(ringo_index:serialize(Dex)),
        S = now(),
        lists:foreach(fun(Key) ->
                {_, Offsets} = ringo_index:find_key(Key, Ser),
                E = [ringo_index:fetch_entry(DB, "test_data", Key, Offs)
                        || Offs <- Offsets],
                [_|_] = lists:filter(fun
                        ({_, K, _}) when K == Key -> true; 
                        (ignore) -> false
                end, E)
        end, Keys),
        io:fwrite("All keys read ok in ~bms~n",
                [round(timer:now_diff(now(), S) / 1000)]),
        halt().
        
