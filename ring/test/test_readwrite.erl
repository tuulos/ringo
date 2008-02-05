-module(test_readwrite).
-export([write_test/1, read_test/1]).

-include("ringo_store.hrl").

write_test(Entries) when is_list(Entries) ->
        write_test(list_to_integer(lists:flatten(Entries)));

write_test(Entries) ->
        {ok, DB} = file:open("test_data/data", [append, raw]),
        Domain = #domain{home = "test_data", db = DB, z = zlib:open()},
        S = now(),
        lists:foreach(fun(I) ->
                EntryID = random:uniform(4294967295),
                N = I + 100,
                ringo_writer:add_entry(Domain, EntryID, <<"KeyYek:", I:32>>,
                        <<"ValueEulav:", N:32>>, [])
        end, lists:seq(1, Entries)),
        io:fwrite("Writing ~b items took ~bms~n",
                [Entries, round(timer:now_diff(now(), S) / 1000)]),
        halt().

read_test(Entries) when is_list(Entries) ->
        read_test(list_to_integer(lists:flatten(Entries)));

read_test(Entries) ->
        S = now(),
        N = ringo_reader:fold(fun
                (<<"KeyYek:", N:32>>, <<"ValueEulav:", M:32>>, _, _, _, Acc)
                        when N == Acc, M == Acc + 100 ->
                        Acc + 1;
                (<<"KeyYek:", N:32>>, <<"ValueEulav:", M:32>>, _, _, _, Acc) ->
                        io:fwrite("~b. entry is invalid. Got keys ~b and ~b.~n",
                                [Acc, N, M]),
                        N + 1;
                (Key, Val, _, _, _, Acc) ->
                        io:fwrite("~b. entry is invalid. Key <~p> Val <~p>.~n",
                                [Acc, Key, Val]),
                        halt()
                        
        end, 1, "test_data/data"),
        if N - 1 == Entries ->
                io:fwrite("~b entries read ok in ~bms~n", [N - 1,
                        round(timer:now_diff(now(), S) / 1000)]);
        true ->
                io:fwrite("Expected ~b entries, read ~b entries~n",
                        [Entries, N])
        end,
        halt().
