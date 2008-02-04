-module(ringo_writer).

-export([encode/4, encoded_size/2]).


%
%
%

% Special case: Convert key to binary
handle_call({put, DomainID, Key, Value}, From, S) when is_list(Key) ->
        handle_call({put, DomainID, list_to_binary(Key), Value}, From, S);

% Special case: Convert value to binary
handle_call({put, DomainID, Key, Value}, From, S) when is_list(Value) ->
        handle_call({put, DomainID, Key, list_to_binary(Value)}, From, S);

% Store value in the DB
handle_call({put, DomainID, Key, Value}, _From, #store(z = Z) = S) 
        when is_binary(Key), is_binary(Value), length(Key) < ?KEY_MAX,
                length(Value) < ?VAL_INTERNAL_MAX ->

        {ok, DB} = open_domain(DomainID, S),
        Entry = ringo_codec:encode(Key, Value, [], Z),
        R = file:write(DB, Entry),
        ets:update_counter(domain_sizes, DomainID, iolist_size(Entry)),
        {reply, R, S};       

% Store value to a separate file
handle_call({put, DomainID, Key, Value}, From, #store{home = Home} = S)when is_binary(Key), is_binary(Value), length(Key) < ?KEY_MAX ->
        
        % XXX: Write first with a different name, rename then. This ensures that
        % resyncing won't copy partial files.
        {ok, DB} = open_domain(DomainID, S),
        {ok, Offs} = file:position(DB, bof),
        ExtFile = "value-" ++ integer_to_list(Offs),
        ExtPath = filename:join(Home, ExtFile),
        ok = file:write_file(ExtPath, Value),
        ok = file:write_file_info(ExtPath, #file_info{mode = ?RDONLY}),
        Link = [crypto:sha(Value), ExtFile],
        ets:update_counter(domain_sizes, DomainID, length(Value)),
        handle_call({put, DomainID, Key, Link}, From, S);

handle_call({put, DomainID, Key, Value}, _From, S) ->
        error_logger:warning_report({"Invalid put request. Key",
                trunc_io:fprint(Key, 500), "Value", 
                trunc_io:fprint(Value, 500)}),
        {reply, invalid_request, S}.



encode(Key, Value, EntryID, FlagList, Z) when is_binary(Key), is_binary(Value) ->
        Flags = lists:foldl(fun(X, F) ->
                {value, {_, V}} = lists:keysearch(X, 1, ?FLAGS),
                F bor V
        end, 0, FlagList),
        {MSecs, Secs, _} = now(),

        Head = [pint(MSecs * 1000000 + Secs)),
                pint(EntryID),
                pint(Flags),
                pint(zlib:crc32(Z, Key)),
                pint(length(Key)),
                pint(zlib:crc32(Z, Value)),
                pint(length(Value)],
        
        [?MAGIC_HEAD_B, pint(zlib:crc32(Z, Head)),
                Head, Key, Value, ?MAGIC_END_B].

% not quite right if Value is an external
encoded_size(Key, Value) ->
        10 * 4 + iolist_size(Key) + iolist_size(Value).

pint(V) when V < (1 bsl 32) ->
        <<V:32/little>>;
pint(V) ->
        error_logger:warning_report({"Integer overflow in ringo_codec", V}),
        throw(integer_overflow).
         
         


        

