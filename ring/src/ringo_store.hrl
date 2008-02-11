
-define(RDONLY, 8#00400).
-define(KEY_MAX, 4096).
-define(VAL_INTERNAL_MAX, 4096). 
-define(MAGIC_HEAD, 16#47da66b5).
-define(MAGIC_TAIL, 16#acc50f5d).
-define(MAGIC_HEAD_B, <<16#47da66b5:32/little>>).
-define(MAGIC_TAIL_B, <<16#acc50f5d:32/little>>).
-define(FLAGS, [{external, 1}, {overwrite, 2}]).

-define(NUM_MERKLE_LEAVES, 8192).

-record(domain, {this, owner, home, host, id, z, db, size, full,
        sync_tree, sync_ids, sync_inbox, sync_outbox, dbname}).
