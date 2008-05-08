-module(lrucache).
-export([new/0, update/2, is_empty/1, get_lru/1]).

%%%
%%% Thanks to Heli Tuulos for this beautiful design of LRU cache.
%%% This implementation is based on the dict module to avoid copying
%%% of large binary keys, as would happen with ets.
%%%

new() -> {dict:new(), nil, nil}.

is_empty({_, nil, nil}) -> true;
is_empty(_) -> false.

% empty cache
get_lru({_, nil, _}) -> nil;

% one key
get_lru({_, H, H}) -> {H, new()};

% normal case 
get_lru({D, H, T}) ->
        {Prev, _} = dict:fetch(H, D),
        {Prev0, _} = dict:fetch(Prev, D),
        {H, {dict:store(Prev, {Prev0, nil}, D), Prev, T}}.
                
update(Key, {D, _, _} = LRU) ->
        update(Key, dict:find(Key, D), LRU).

% first key in the cache
update(Key, error, {D, nil, nil}) ->
        {dict:store(Key, {nil, nil}, D), Key, Key};

% new key 
update(Key, error, LRU) -> add_tail(Key, LRU);

% update tail
update(_, {ok, {nil, _}}, LRU) -> LRU;

% update head
update(Key, {ok, {Prev, nil}}, {D, _, T}) ->
        {Prev0, _} = dict:fetch(Prev, D),
        add_tail(Key, {dict:store(Prev, {Prev0, nil}, D), Key, T});

% existing key in the middle
update(Key, {ok, {Prev, Next}}, {D, H, T}) ->
        {Prev0, _} = dict:fetch(Prev, D),
        {_, Next0} = dict:fetch(Next, D),
        D0 = dict:store(Prev, {Prev0, Next}, D),
        add_tail(Key, {dict:store(Next, {Prev, Next0}, D0), H, T}).
        
add_tail(Key, {D, H, T}) ->
        {nil, Next} = dict:fetch(T, D),
        D0 = dict:store(T, {Key, Next}, D),
        {dict:store(Key, {nil, T}, D0), H, Key}. 
