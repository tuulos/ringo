-module(test_readwrite).
-export([write_test/1, read_test/1, extfile_write_test/1, extfile_read_test/0]).

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
        {_, N} = ringo_reader:fold(fun

                (<<"KeyYek:", N:32>>, <<"ValueEulav:", M:32>>, _, _, _, {C, T})
                        when N == C, M == C + 100 ->
                        {C + 1, T + 1};

                (<<"KeyYek:", N:32>>, <<"ValueEulav:", M:32>>, _, _, _, 
                        {_, T}) ->
                        io:fwrite("~b. entry is invalid. Got keys ~b and ~b.~n",
                                [T, N, M]),
                        {N + 1, T + 1};

                (Key, Val, _, _, _, {_, T}) ->
                        io:fwrite("~b. entry is invalid. Key <~p> Val <~p>.~n",
                                [T, Key, Val]),
                        halt()
                        
        end, {1, 0}, "test_data/data"),
        if N == Entries ->
                io:fwrite("~b entries read ok in ~bms~n", [N,
                        round(timer:now_diff(now(), S) / 1000)]);
        true ->
                io:fwrite("Expected ~b entries, read ~b entries~n",
                        [Entries, N])
        end,
        halt().


extfile_write_test(Entries) when is_list(Entries) ->
        extfile_write_test(list_to_integer(lists:flatten(Entries)));

extfile_write_test(Entries) ->
        {ok, DB} = file:open("test_data/bigdata", [append, raw]),
        Domain = #domain{home = "test_data", db = DB, z = zlib:open()},
        {ok, Bash} = file:read_file("/bin/bash"),
        Z = zlib:open(),
        BashCRC = zlib:crc32(Z, Bash),
        S = now(),
        lists:foreach(fun(_) ->
                EntryID = random:uniform(4294967295),
                ringo_writer:add_entry(Domain, EntryID, 
                        <<"Bash:", BashCRC:32>>, Bash, []),
                % entry with an equal EntryID, should be skipped
                ringo_writer:add_entry(Domain, EntryID, <<"skipme">>, <<>>, []),
                NextID = random:uniform(4294967295),
                ringo_writer:add_entry(Domain, NextID, <<"small">>,
                        <<"fall">>, [])
        end, lists:seq(1, Entries)),
        io:fwrite("Writing ~b big items took ~bms~n",
                [Entries, round(timer:now_diff(now(), S) / 1000)]),
        halt().

extfile_read_test() ->
        S = now(),
        Z = zlib:open(),
        ringo_reader:fold(fun

                (<<"Bash:", BashCRC:32>>, Val, [external], ID, _, small) ->
                        {ok, Bash} = ringo_reader:read_external(
                                "test_data", Z, Val),
                        M = zlib:crc32(Z, Bash),
                        if M == BashCRC -> {big, ID};
                        true ->
                                io:fwrite("Invalid checksum~n"),
                                halt()
                        end;

                (<<"small">>, <<"fall">>, [], _, _, {big, _}) ->
                        small;

                (Key, _, _, ID, _, A) ->
                        io:fwrite(
                                "Invalid entry. Key <~p> Acc <~w> ID <~w>.~n",
                                        [Key, A, ID]),
                        halt()

        end, small, "test_data/bigdata"),
        io:fwrite("Reading big and small items took ~bms~n",
                [round(timer:now_diff(now(), S) / 1000)]),
        halt().



                        
                        


