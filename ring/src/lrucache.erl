-module(lrucache).
-export([new/0, update/2, is_empty/1, get_lru/1]).

%%%
%%% Thanks to Heli Tuulos for this beautiful design of LRU cache:
%%% Cache is a double-linked list providing constant-time access to entries
%%% via a dictionary. The LRU item is always the head and can be retrieved
%%% with get_lru(). Accessing an item moves it to to the tail (update()).
%%%
%%% This implementation is based on the dict module to avoid copying
%%% of large binary keys, as would happen with ets.
%%%

new() -> {gb_trees:empty(), nil, nil}.

is_empty({_, nil, nil}) -> true;
is_empty(_) -> false.

% empty cache
get_lru({_, nil, _}) -> nil;

% one key
get_lru({_, H, H}) -> {H, new()};

% normal case 
get_lru({D, H, T}) ->
        {Prev, _} = gb_trees:get(H, D),
        {Prev0, _} = gb_trees:get(Prev, D),
        {H, {gb_trees:enter(Prev, {Prev0, nil},
                gb_trees:delete(H, D)), Prev, T}}.
                
update(Key, {D, _, _} = LRU) ->
        update(Key, gb_trees:lookup(Key, D), LRU).

% first key in the cache
update(Key, none, {D, nil, nil}) ->
        {gb_trees:enter(Key, {nil, nil}, D), Key, Key};

% new key 
update(Key, none, LRU) -> add_tail(Key, LRU);

% update tail
update(_, {value, {nil, _}}, LRU) -> LRU;

% update head
update(Key, {value, {Prev, nil}}, {D, _, T}) ->
        {Prev0, _} = gb_trees:get(Prev, D),
        add_tail(Key, {gb_trees:enter(Prev, {Prev0, nil}, D), Key, T});

% existing key in the middle
update(Key, {value, {Prev, Next}}, {D, H, T}) ->
        {Prev0, _} = gb_trees:get(Prev, D),
        {_, Next0} = gb_trees:get(Next, D),
        D0 = gb_trees:enter(Prev, {Prev0, Next}, D),
        add_tail(Key, {gb_trees:enter(Next, {Prev, Next0}, D0), H, T}).
        
add_tail(Key, {D, H, T}) ->
        {nil, Next} = gb_trees:get(T, D),
        D0 = gb_trees:enter(T, {Key, Next}, D),
        {gb_trees:enter(Key, {nil, T}, D0), H, Key}. 
