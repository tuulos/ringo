-module(ringo_external).
-export([fetch_external/1]).

fetch_external(Home) ->
        error_logger:info_report("Fetch external starts"),
        receive
                {fetch, From, Entry} ->
                        error_logger:info_report({"Got external!"}),
                        {_, SrcFile, TmpFile, DstFile} = parse_entry(Home, Entry),
                        case catch fetch(From, SrcFile, TmpFile, DstFile) of
                                ok -> error_logger:info_report(
                                        {"File", SrcFile, "fetched succesfully"});
                                Error -> error_logger:warning_report(
                                        {"File", SrcFile, "error", Error}),
                                        file:delete(TmpFile)
                        end;
                _ -> error_logger:warning_report("Unknown message received")
        end,
        fetch_external(Home).

parse_entry(Home, Entry) ->
        {_, _, _, _, {ext, {CRC, SrcFile}}} = ringo_reader:decode(Entry),
        FetchID = integer_to_list(random:uniform(4294967295)),
        TmpFile = filename:join(Home, lists:flatten(
                [SrcFile, "-", FetchID, ".partial"])),
        DstFile = filename:join(Home, SrcFile),
        {CRC, SrcFile, TmpFile, DstFile}.       

fetch(From, SrcFile, TmpFile, DstFile) ->
        error_logger:info_report({"S", SrcFile, "T", TmpFile, "D", DstFile}),
        {ok, SrcIO} = gen_server:call(From, {get_file_handle, SrcFile}),
        error_logger:info_report({"Src ok"}),
        {ok, DstIO} = file:open(TmpFile, [write, binary, raw]),
        error_logger:info_report({"Me ok"}),
        T = now(),
        {ok, _} = file:copy(SrcIO, DstIO),
        error_logger:info_report({"File", SrcFile, "copied in",
                timer:now_diff(now(), T) div 1000, "ms"}),
        file:rename(TmpFile, DstFile).


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
