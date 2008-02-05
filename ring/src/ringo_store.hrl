
-define(MAGIC_HEAD, 16#47da66b5).
-define(MAGIC_TAIL, 16#acc50f5d).
-define(MAGIC_HEAD_B, <<16#47da66b5:32/little>>).
-define(MAGIC_TAIL_B, <<16#acc50f5d:32/little>>).
-define(FLAGS, [{external, 1}]).
