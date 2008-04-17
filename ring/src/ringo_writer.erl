
-module(ringo_writer).

-export([write_entry/2, make_entry/5, entry_size/1]).

-include("ringo_store.hrl").
-include_lib("kernel/include/file.hrl").

entry_size({Entry, {}}) -> iolist_size(Entry);
entry_size({Entry, {_, Value}}) -> iolist_size(Entry) + iolist_size(Value).

write_entry(DB, {Entry, {}}) ->
        ok = file:write(DB, Entry), ok;

write_entry(DB, {Entry, {ExtPath, Value}}) ->
        % Write first with a different name, rename then. This ensures that
        % resyncing won't copy partial files. BUT: When async-threads are
        % enabled write_file probably doesn't block and there's no way to 
        % know when the bits have actually hit the disk, other than syncing
        % every time, hence renaming wouldn't help much.
        ok = file:write(DB, Entry),
        ok = file:write_file(ExtPath, Value), ok.

make_entry(#domain{z = Z}, EntryID, Key, Value, Flags)
        when is_binary(Key), is_binary(Value), size(Key) < ?KEY_MAX,
                size(Value) < ?VAL_INTERNAL_MAX ->

        {encode(Key, Value, EntryID, Flags, Z), {}};

% Store value to a separate file
make_entry(#domain{home = Home, z = Z}, EntryID, Key, Value, Flags)
        when is_binary(Key), is_binary(Value), size(Key) < ?KEY_MAX ->
        
        CRC = zlib:crc32(Z, Value),
        ExtFile = io_lib:format("value-~.16b-~.16b", [EntryID, CRC]),
        ExtPath = filename:join(Home, ExtFile),
        Link = [<<CRC:32>>, ExtFile],
        {encode(Key, list_to_binary(Link), EntryID, [external|Flags], Z),
                {ExtPath, Value}};

make_entry(_, _, Key, Value, _) ->
        error_logger:warning_report({"Invalid put request. Key",
                trunc_io:fprint(Key, 500), "Value", 
                trunc_io:fprint(Value, 500)}),
        invalid_request.

encode(Key, Value, EntryID, FlagList, Z) when is_binary(Key), is_binary(Value) ->
        Flags = lists:foldl(fun(X, F) ->
                {value, {_, V}} = lists:keysearch(X, 1, ?FLAGS),
                F bor V
        end, 0, FlagList),
        {MSecs, Secs, _} = now(),

        Head = [pint(MSecs * 1000000 + Secs),
                pint(EntryID),
                pint(Flags),
                pint(zlib:crc32(Z, Key)),
                pint(size(Key)),
                pint(zlib:crc32(Z, Value)),
                pint(size(Value))],
        
        [?MAGIC_HEAD_B, pint(zlib:crc32(Z, Head)),
                Head, Key, Value, ?MAGIC_TAIL_B].

% not quite right if Value is an external
%encoded_size(Key, Value) when is_binary(Key), is_binary(Value),
%                size(Value) < ?VAL_INTERNAL_MAX ->
%
%        10 * 4 + iolist_size(Key) + iolist_size(Value);
%
%encoded_size(Key, Value) when is_binary(Key), is_binary(Value) ->
%        27 + 10 * 4 + iolist_size(Key) + iolist_size(Value).
        

pint(V) when V < (1 bsl 32) ->
        <<V:32/little>>;
pint(V) ->
        error_logger:warning_report({"Integer overflow in ringo_codec", V}),
        throw(integer_overflow).
         
%format_md5(MD5) ->
%        % who said that Erlang's string handling sucks :P
%        io_lib:format(lists:flatten(lists:duplicate(16, "~.16b")),
%                binary_to_list(MD5)).



        

