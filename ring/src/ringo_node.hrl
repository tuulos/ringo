
-define(MAX_RING_SIZE, 50000).

-record(rnode, {myid, previd, nextid, prevnode, nextnode, route, home}).
