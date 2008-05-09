-module(ringo_reader).

-export([fold/3, fold/5, read_entry/2, is_external/1, decode/1]).
-export([read_external/2, parse_flags/1, read_file/1]).
-include("ringo_store.hrl").

-define(NI, :32/little).
-record(iter, {db, f, prev, prev_head, acc, skipbad}).

is_external(X) when is_list(X) -> is_external(iolist_to_binary(X));
is_external(<<_:16/binary, _:7, 1:1, _/binary>>) -> true;        
is_external(_) -> false.

decode(X) when is_list(X) ->
        decode(iolist_to_binary(X));

decode(<<?MAGIC_HEAD?NI, _HeadCRC?NI, Time?NI, EntryID?NI, FlagsB?NI,
        _KeyCRC?NI, KeyLen?NI, _ValCRC?NI, ValLen?NI, Key:KeyLen/binary,
        ValueB:ValLen/binary, ?MAGIC_TAIL?NI>>) ->

        Flags = parse_flags(FlagsB),
        Value = case proplists:is_defined(external, Flags) of
                true -> <<FileCRC:32, FileName/binary>> = ValueB,
                        {ext, {FileCRC, binary_to_list(FileName)}};
                false -> {int, ValueB}
        end,
        {Time, EntryID, Flags, Key, Value}.

fold(F, Acc0, DBName) ->
        fold(F, Acc0, DBName, false, 0).

fold(F, Acc0, DBName, WithPos, StartPos) ->
        {ok, DB} = bfile:fopen(DBName, "r"),
        if StartPos > 0 ->
                ok = bfile:fseek(DB, StartPos, seek_set);
        true -> ok
        end,
        try
                read_item(#iter{db = DB, f = F, prev = {0, 0}, skipbad = true,
                        prev_head = StartPos, acc = Acc0}, WithPos)
        catch
                {eof, #iter{acc = Acc}} ->
                        bfile:fclose(DB),
                        Acc;
                % callback function may throw(eof) if it wants
                % to stop iterating
                {eof, Acc} ->
                        bfile:fclose(DB),
                        Acc
        end.

read_item(#iter{f = F, prev = Prev, acc = Acc} = Q, WithPos) ->
        {NQ, {Time, EntryID, Flags, Key, Val, Entry}} = read_entry(Q),
        ID = {Time, EntryID},
        % skip duplicate items
        AccN = if Prev == EntryID -> Acc;
        % skip iblocks
        ?FLAG_UP(Flags, ?IBLOCK_FLAG) -> Acc;
        true ->
                PFlags = parse_flags(Flags),
                if WithPos ->
                        Pos = NQ#iter.prev_head - 8, 
                        F(Key, Val, PFlags, ID, Entry, Acc, Pos);
                true ->
                        F(Key, Val, PFlags, ID, Entry, Acc)
                end
        end,
        read_item(NQ#iter{prev = EntryID, acc = AccN}, WithPos).

read_entry(DB, Pos) ->
        ok = bfile:fseek(DB, Pos, seek_set),
        Q = #iter{db = DB, skipbad = false},
        {_, R} = read_head(Q, read(Q, 8)), 
        R.

read_entry(Q) ->
        read_head(Q, read(Q, 8)).

% If reading an entry fails, we have no guarantee on how many bytes
% actually belong to this entry (in the extreme case only the magic head
% is valid), so we have to backtrack to the field head and continue
% seeking for the next magic head from there. Hence we need to
% record position of the latest entry head below.
read_head(#iter{db = DB} = Q, <<?MAGIC_HEAD?NI, HeadCRC?NI>> = PHead) ->
        {ok, Pos} = bfile:ftell(DB),
        NQ = Q#iter{prev_head = Pos},
        Head = read(NQ, 7 * 4),
        read_body(NQ, Head, check(Head, HeadCRC), PHead);

read_head(Q, _) -> seek_magic(Q).

read_body(Q, <<Time?NI, EntryID?NI, Flags?NI, KeyCRC?NI, 
                KeyLen?NI, ValCRC?NI, ValLen?NI>> = Head, true, PHead) ->

        <<Key:KeyLen/binary, Val:ValLen/binary, End:4/binary>> = Body =
                read(Q, KeyLen + ValLen + 4),
        Entry = <<PHead/binary, Head/binary, Body/binary>>,
        validate(Q, {Time, EntryID, Flags, Key, Val, Entry},
                check(Key, KeyCRC), check(Val, ValCRC), End);

read_body(Q, _, false, _) -> seek_magic(Q).

validate(Q, Ret, true, true, ?MAGIC_TAIL_B) -> {Q, Ret};
validate(Q, _, _, _, _) -> seek_magic(Q).
        
check(Val, CRC) -> erlang:crc32(Val) == CRC.

seek_magic(#iter{skipbad = false}) -> invalid_entry;

seek_magic(#iter{db = DB, prev_head = 0} = Q) ->
        ok = bfile:fseek(DB, 1, seek_set),
        seek_magic(Q, read(Q, 8));

seek_magic(#iter{db = DB, prev_head = Pos} = Q) ->
        ok = bfile:fseek(DB, Pos - 7, seek_set),
        seek_magic(Q, read(Q, 8)).

% Found a potentially valid entry head
seek_magic(Q, <<?MAGIC_HEAD?NI, _?NI>> = D) ->
        read_head(Q, D);

% Skip a byte, continue seeking
seek_magic(Q, <<_:1/binary, D/binary>>) ->
        E = read(Q, 1),
        seek_magic(Q, <<D/binary, E/binary>>).

read(#iter{db = DB} = Q, N) ->
        % fread() should return a short byte count only if an error
        % occurs or eof is reached, which should not happen here.
        % However, we may try to read a new entry that hasn't hit
        % the disk yet fully, causing a short byte count. This
        % shouldn't be considered an error.
        case bfile:fread(DB, N) of
                {ok, D} when size(D) == N -> D;
                {ok, _} -> throw({eof, Q});
                eof -> throw({eof, Q});
                Error -> throw(Error)
        end.

%read(_, B, {ok, D}, N) when size(B) + size(D) == N ->
%        {ok, <<B/binary, D/binary>>};
%read(F, B, {ok, D}, N) ->
%        B0 = <<B/binary, D/binary>>,
%        read(F, B0, bfile:fread(F, N - size(B0)), N);
%read(_, _, R, _) -> R.

parse_flags(Flags) ->
        [S || {S, F} <- ?FLAGS, Flags band F > 0].

read_external(Home, <<_CRC:32, ExtFile/binary>>) ->
        ExtPath = filename:join(Home, binary_to_list(ExtFile)),
        case read_file(ExtPath) of
                {error, Reason} -> {io_error, Reason};
                {ok, _Value} = R -> R
                        %V = erlang:crc32(Value),
                        %if V == CRC -> {ok, Value};
                        %true -> corrupted_file
                        %end
        end.


read_file(FName) ->
        case bfile:fopen(FName, "r") of
                {ok, F} -> read_file(F, <<>>, bfile:fread(F, 65536));
                E -> E
        end.
                
read_file(F, D, {ok, B}) ->
        read_file(F, <<D/binary, B/binary>>, bfile:fread(F, 65536));

read_file(F, D, eof) ->
        bfile:fclose(F), {ok, D}.



