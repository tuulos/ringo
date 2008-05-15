
-define(RDONLY, 8#00400).
-define(KEY_MAX, 4096).
-define(VAL_INTERNAL_MAX, 4096). 
-define(MAGIC_HEAD, 16#47da66b5).
-define(MAGIC_TAIL, 16#acc50f5d).
-define(MAGIC_HEAD_B, <<16#47da66b5:32/little>>).
-define(MAGIC_TAIL_B, <<16#acc50f5d:32/little>>).
-define(EXT_FLAG, 1).
-define(IBLOCK_FLAG, 2).
-define(FLAGS, [{external, 1}, {iblock, 2}]).
-define(FLAG_UP(Flags, Flag), Flags band Flag =/= 0).

-define(NUM_MERKLE_LEAVES, 8192).

-record(domain, {this, owner, home, host, id, db, size, full, num_entries,
        sync_tree, sync_ids, sync_inbox, sync_outbox, dbname, stats, info,
        nextnode, prevnode, extproc, index, max_repl_entries,
        domain_chunk_max}).
