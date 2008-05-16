-module(ringo_external).
-export([fetch_external/2, check_external/1]).

-include("ringo_store.hrl").

%%% fetch_external() is a permanent process that takes care of copying external
%%% files to this node as a part of the syncing process. In the normal put / 
%%% replica put case this is not needed.

fetch_external(Domain, Home) ->
        fetch_external(Domain, Home, [], {none, none}).

fetch_external(Domain, Home, [{_, {_, TmpFile, _}} = Entry|FetchList],
        {none, none}) ->

        {_, Ref} = spawn_monitor(fun() -> fetch(Entry) end),
        fetch_external(Domain, Home, FetchList, {Ref, TmpFile});               

fetch_external(Domain, Home, FetchList, {Worker, TmpFile} = P) ->
        receive
                {fetch, {From, Entry}} ->
                        fetch_external(Domain, Home, [{From,
                                parse_entry(Home, Entry)}|FetchList], P);
                {'DOWN', Worker, _, _, normal} when FetchList == []->
                        error_logger:info_report(
                                {"File", TmpFile, "fetched ok"}),
                        gen_server:cast(Domain, update_domain_size),
                        fetch_external(Domain, Home, FetchList, {none, none});
                {'DOWN', Worker, _, _, normal} ->
                        fetch_external(Domain, Home, FetchList, {none, none}); 
                {'DOWN', Worker, _, _, Reason} ->
                        error_logger:info_report(
                                {"File", TmpFile, "error", Reason}),
                        file:delete(TmpFile),
                        fetch_external(Domain, Home, FetchList, {none, none})
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


%%%
%%% check_external is a periodic process that goes through the DB file, and for
%%% each external entry, checks that the corresponding file really exists on
%%% the domain directory. If it doesn't, it tries to find a copy of the file
%%% from another node using a find_file request. Once a copy has been found,
%%% it sends a fetch request to the fetch_external process (above).
%%%
%%% Note that the find_file request is made only for the first missing file.
%%% If multiple files are missing, we opportunistically assume that the same
%%% node might have a copy of them, too. If the assumption fails, next time
%%% we will make a find_file request for the second missing file etc. In the
%%% worst case, finding N missing files requires N check_external() runs and 
%%% N find_file requests to be found.
%%%

check_external(This) ->
        error_logger:info_report({"Check external starts"}),
        #domain{dbname = DBName, stats = Stats, home = Home, extproc = Ext} 
                = gen_server:call(This, get_current_state),
        % Note that this doesn't sync iblocks since ringo_reader:fold() ignores
        % them. If ringo_reader:fold() gets a flag that allows iblocks to be
        % included in the iteration, we could sync them here as well.
        {NExt, NMiss, _} = ringo_reader:fold(fun(_, _, _, _, Entry, Nfo) ->
                check_entry(ringo_reader:is_external(Entry), 
                        Home, Entry, This, Nfo, Ext)
        end, {0, 0, none}, DBName),
        error_logger:info_report({"Check external finishes:",
                NExt, "external", NMiss, "missing"}),
        if NMiss > 0 ->
                error_logger:info_report({"Update domain size"}),
                gen_server:cast(This, update_domain_size);
        true -> ok
        end,
        ringo_domain:stats_buffer_add(Stats, external_entries, {NExt, NMiss}).

% internal entry, do nothing
check_entry(false, _, _, _, Nfo, _) -> Nfo;
% external entry, check that the file exists
check_entry(true, Home, Entry, This, {N, M, FileSrc}, Ext) ->
        % CRC checkin could be added here to detect random disk corruption
        {_, _, _, _, {ext, {_CRC, ExtFile}}} = ringo_reader:decode(Entry),
        case file:read_file_info(filename:join(Home, ExtFile)) of
                {ok, _} -> {N + 1, M, FileSrc};
                _ -> {N + 1, M + 1, handle_missing_file(This, 
                        Entry, ExtFile, FileSrc, Ext)}
        end.

% We don't know yet from which node we can fetch the missing files ->
% Find it out with a find_file request.
handle_missing_file(This, Entry, ExtFile, none, Ext) ->
        error_logger:warning_report({"File", ExtFile, "missing"}),
        gen_server:cast(This, {find_file, ExtFile, {self(), node()}, 0}),
        receive
                {file_found, ExtFile, FileSrc} -> handle_missing_file(This,
                        Entry, ExtFile, FileSrc, Ext);
                _ -> none
        after 30000 -> none
        end;

% We already know that FileSrc matched some previous missing file, so quite
% likely it has this Entry as well. If not, fetch() will fail, but maybe next
% time we will make a find_file call specifically for this Entry.
handle_missing_file(_, Entry, _, FileSrc, Ext) ->
        Ext ! {fetch, {FileSrc, Entry}},
        FileSrc. 


