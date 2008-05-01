-module(ringo_external).
-export([fetch_external/1]).

fetch_external(Home) ->
        fetch_external(Home, [], {none, none}).

fetch_external(Home, [{_, {_, TmpFile, _}} = Entry|FetchList], {none, none}) ->
        {_, Ref} = spawn_monitor(fun() -> fetch(Entry) end),
        fetch_external(Home, FetchList, {Ref, TmpFile});               

fetch_external(Home, FetchList, {Worker, TmpFile} = P) ->
        receive
                {fetch, {From, Entry}} ->
                        fetch_external(Home, [{From,
                                parse_entry(Home, Entry)}|FetchList], P);
                {'DOWN', Worker, _, _, Reason} ->
                        if Reason == normal ->
                                error_logger:info_report(
                                        {"File", TmpFile, "fetched ok"});
                        true -> 
                                error_logger:info_report(
                                        {"File", TmpFile, "error", Reason}),
                                file:delete(TmpFile)
                        end,
                        fetch_external(Home, FetchList, {none, none})
        end.

parse_entry(Home, Entry) ->
        {_, _, _, _, {ext, {_CRC, SrcFile}}} = ringo_reader:decode(Entry),
        FetchID = integer_to_list(random:uniform(4294967295)),
        TmpFile = filename:join(Home, lists:flatten(
                [SrcFile, "-", FetchID, ".partial"])),
        DstFile = filename:join(Home, SrcFile),
        {SrcFile, TmpFile, DstFile}.       

fetch({From, {SrcFile, TmpFile, DstFile}}) ->
        {ok, SrcIO} = gen_server:call(From, {get_file_handle, SrcFile}),
        link(SrcIO),
        {ok, DstIO} = file:open(TmpFile, [write, binary, raw]),
        T = now(),
        {ok, _} = file:copy(SrcIO, DstIO),
        error_logger:info_report({"File", SrcFile, "copied in",
                timer:now_diff(now(), T) div 1000, "ms"}),
        file:rename(TmpFile, DstFile),
        file:close(SrcIO),
        file:close(DstIO).


%copy_and_checksum(DstIO, SrcIO) ->
%        {ok, Data} = file:read(SrcIO, ?BUF_SIZE),
%        ok = file:write(DstIO, Data),
%        copy_and_checksum(DstIO, SrcIO, crc32(Data)).
%
%copy_and_checksum(DstIO, SrcIO, CRC) ->
%        case file:read(SrcIO, ?BUF_SIZE) of
%                {ok, Data} ->
%                        ok = file:write(DstIO, Data),
%                        copy_and_checksum(DstIO, SrcIO, crc32(CRC, Data));
%                eof -> {ok, CRC};
%                Error -> {error, Error}
%        end
