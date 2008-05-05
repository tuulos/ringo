-module(test_index).
-export([buildindex_test/1, serialize_test/1, kv_test/0]).

write_data(Keys) ->
        S = now(),
        {ok, DB} = file:open("test_data/indexdata", [write, raw]),
        lists:foreach(fun(_) ->
                EntryID = random:uniform(4294967295),
                N = random:uniform(Keys),
                Entry = ringo_writer:make_entry(EntryID, <<"KeyYek:", N:32>>,
                                <<"ValueEulav:", N:32>>, []),
                ok = ringo_writer:write_entry("test_data", DB, Entry)
        end, lists:seq(1, 10000)),
        io:fwrite("Writing ~b items (max key ~b) took ~bms~n",
                [10000, Keys, round(timer:now_diff(now(), S) / 1000)]),
        file:close(DB).

buildindex_test(Keys) when is_list(Keys) -> 
        buildindex_test(list_to_integer(lists:flatten(Keys)));

buildindex_test(Keys) ->
        write_data(Keys),
        S = now(),
        Dex = ringo_index:build_index("test_data/indexdata"),
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
        Dex = ringo_index:build_index("test_data/indexdata"),
        S = now(),
        Ser = iolist_to_binary(ringo_index:serialize(Dex)),
        io:fwrite("Serialization took ~bms~n",
                [round(timer:now_diff(now(), S) / 1000)]),
        io:fwrite("Serialized index takes ~bK~n", [iolist_size(Ser) div 1024]),
        io:fwrite("~b keys in the index~n", [gb_trees:size(Dex)]),
        S2 = now(),
        lists:foreach(fun(ID) ->
                %io:fwrite("ID ~b~n", [ID]),
                {ID, _} = ringo_index:find_key(ID, Ser)
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
