import tempfile, os, os.path, subprocess, md5, time, sys, random, threading
import ringogw

home_dir = tempfile.mkdtemp("", "ringotest-") + '/'
node_id = 0
conn = None

os.environ['RINGOHOST'] = 'RINGODBG'
os.environ['RESYNC_INTERVAL'] = '10000'
os.environ['CHECK_EXT_INTERVAL'] = '10000'
os.environ['DOMAIN_CHUNK_MAX'] = str(100 * 1024**2)

def new_node(id = None):
        global node_id
        if id == None:
                id = md5.md5("test-%d" % node_id).hexdigest()
                node_id += 1
        path = home_dir + id
        if not os.path.exists(path):
                os.mkdir(path)
        p = subprocess.Popen(["start_ringo.sh", path],
                stdin = subprocess.PIPE, stdout = subprocess.PIPE,
                        stderr = subprocess.PIPE, env = os.environ)
        return id, p

def kill_node(id):
        subprocess.call(["pkill", "-f", id])

def domain_id(name, chunk):
        return md5.md5("%d %s" % (chunk, name)).hexdigest().upper()

def make_domain_id(did):
        return hex(did)[2:-1]

def check_entries(r, nrepl, nentries, check_size = True):
        if r[0] != 200:
                return False
        if len(r[1][3]) != nrepl:
                return False

        owner_root = owner_size = -2
        roots = []
        for node in r[1][3]:
                if 'error' in node:
                        continue
                num = node["num_entries"]
                if nentries != None and\
                        (num == "undefined" or int(num) != nentries):
                        return False
                root = -1
                if "synctree_root" in node:
                        root = node["synctree_root"][0][1]
                        roots.append((root, node["size"]))
                if node["owner"]:
                        owner_root = root
                        owner_size = node["size"]

        if len(roots) != nrepl:
                return False

        # check that root hashes match
        return [(r, s) for r, s in roots if r != owner_root\
                or (check_size and s != owner_size)] == []
               

def _check_results(reply, num):
        if reply[0] != 200:
                return False
        nodes = reply[1]
        l = len([node for node in nodes if node['ok']])
        return len(nodes) == l == num

def _test_ring(n, num = None, nodeids = []):
        if num == None:
                check = lambda x: _check_results(x, n)
        else:
                check = lambda x: _check_results(x, num)

        if nodeids:
                n = len(nodeids)

        print "Launching nodes",
        res = []
        for i in range(n):
                time.sleep(1)
                if nodeids:
                        res.append(new_node(nodeids[i]))
                else:
                        res.append(new_node())
                sys.stdout.write(".")
        print
        t = time.time()
        if _wait_until("/mon/ring/nodes", check, 30):
                print "Ring converged in %d seconds" % (time.time() - t)
                return True
        return False

def _wait_until(req, check, timeout):
        print "Checking results",
        for i in range(timeout):
                time.sleep(1)
                r = ringo.request(req)
                if check(r):
                        print
                        return True
                sys.stdout.write(".")
        print
        return False

def _put_entries(name, nitems, retries = 0):
        t = time.time()
        for i in range(nitems):
                ringo.put(name, "item-%d" % i, "testitem-%d" % i,
                        retries = retries)
        print "%d items put in %dms" % (nitems, (time.time() - t) * 1000)

def _test_repl(name, n, nrepl, nitems, create_ring = True):
        if create_ring and not _test_ring(n):
                return False
        node, domainid = ringo.create(name, nrepl)
        _put_entries(name, nitems)
        return _wait_until("/mon/domains/domain?id=0x" + domainid,
                lambda x: check_entries(x, n, nitems), 50)

def _check_extfiles(node, domainid, num):
        files = os.listdir("%s/%s/rdomain-%s/" % (home_dir, node, domainid))
        return len([f for f in files if f.startswith("value")]) == num

def _cache_test(**kwargs):
        def check_values(key, ret):
                for i, v in enumerate(ret):
                        if v != key + "-pie-%d" % i:
                                raise "Invalid results: Key <%s>: %s"\
                                        % (key, r)

        if not _test_ring(1):
                return False
        
        node, domainid = ringo.create("cachetest", 5, **kwargs)
        print "Putting 100500 items.."
        t = time.time()
        head = []
        tail = []
        for i in range(105):
                key = "head-%d" % i
                head.append(key)
                for j in range(100):
                        ringo.put("cachetest", key, key + "-pie-%d" % j)
        for i in range(50500):
                key = "tail-%d" % i
                tail.append(key)
                ringo.put("cachetest", key, key + "-pie-0")

        print "items put in %dms" % ((time.time() - t) * 1000)

        print "Retrieving all keys and checking values.."
        t = time.time()
        for key in head + tail:
                check_values(key, ringo.get("cachetest", key))
        print "Get took %dms" % ((time.time() - t) * 1000)
        
        print "Getting 10000 keys in sequential order"
        s = random.sample(head, 100) + random.sample(tail, 10)
        t = time.time()
        for i in range(10):
                random.shuffle(s)
                for key in s:
                        for j in range(10):
                                check_values(key, ringo.get("cachetest", key))
        print "Get took %dms" % ((time.time() - t) * 1000)

        print "Getting 10000 keys in random order"
        t = time.time()
        for i in range(10000):
                key = random.choice(tail)
                check_values(key, ringo.get("cachetest", key))
        print "Get took %dms" % ((time.time() - t) * 1000)
        return True

# make a ring, check that converges
def test01_ring10():
        return _test_ring(10)

# make a large ring, check that converges
def test02_ring100():
        return _test_ring(100)

# make a ring in two phases, check that converges
def test03_ring_newnodes():
        print "Launching first batch of nodes"
        if not _test_ring(10):
                return False
        time.sleep(10)
        print "Launching second batch of nodes"
        return _test_ring(10, 20)

# make a ring, kill random nodes, check that heals
def test04_ring_randomkill():
        res = []
        print "Launching nodes",
        for i in range(50):
                time.sleep(1)
                id, proc = new_node()
                res.append(id)
                sys.stdout.write(".")
        print
        time.sleep(10)
        for id in random.sample(res, 23):
                print "Kill", id
                kill_node(id)
        t = time.time()
        if _wait_until("/mon/ring/nodes", 
                        lambda x: _check_results(x, 27), 60):
                print "Ring healed in %d seconds" % (time.time() - t)
                return True
        return False


# create, put, check that succeeds
def test05_replication1():
        return _test_repl("test_replication1", 1, 1, 100)

# create, put, check that succeeds, replicates
def test06_replication50():
        return _test_repl("test_replication50", 50, 50, 100)

# create, put, add new node (replica), check that resyncs ok
def test07_addreplica(first_owner = True):
        if first_owner:
                name = "addreplicas_test"
        else:
                name = "addowner_test"
        check1 = lambda x: _check_results(x, 1)
        check2 = lambda x: _check_results(x, 2)
        did = domain_id(name, 0)
        # a node id that is guaranteed to become owner for the domain
        owner_id = real_owner = make_domain_id(int(did, 16) - 1)
        # a node id that is guaranteed to become replica for the domain
        repl_id = make_domain_id(int(did, 16) + 1)
        
        if not first_owner:
                tmp = repl_id
                repl_id = owner_id
                owner_id = tmp

        new_node(owner_id)
        print "Waiting for ring to converge:"
        if not _wait_until("/mon/ring/nodes", check1, 30):
                print "Ring didn't converge"
                return False
        print "Creating and populating the domain:"
        if not _test_repl(name, 1, 2, 50, False):
                print "Couldn't create and populate the domain"
                return False
        
        if first_owner:
                print "Adding a new replica:"
        else:
                print "Adding a new owner:"

        new_node(repl_id)
        if not _wait_until("/mon/ring/nodes", check2, 30):
                print "Ring didn't converge"
                return False
       
        _put_entries(name, 50)

        print "Waiting for resync (timeout 300s, be patient):"
        if not _wait_until("/mon/domains/domain?id=0x" + did,
                lambda x: check_entries(x, 2, 100), 300):
                print "Resync didn't finish in time"
                return False
        
        re = ringo.request("/mon/domains/domain?id=0x" + did)
        repl = re[1][3]
        
        if real_owner in [r['node'] for r in repl if r['owner']][0]:
                print "Owner matches"
                return True
        else:
                print "Invalid owner for domain %s (should be %s), got: %s" %\
                        (did, owner_id, repl)
                return False

# create, put, add new node (owner), check that resyncs ok and owner is
# transferred correctly
def test08_addowner():
        return test07_addreplica(False)


# create, put, kill owner, put, reincarnate owner, check that succeeds 
# and resyncs 
def test09_killowner():
        if not _test_ring(10):
                return False

        print "Create and populate domain"
        node, domainid = ringo.create("killowner", 5)
        _put_entries("killowner", 50)
        
        kill_id = node.split('@')[0].split("-")[1]
        print "Kill owner", kill_id
        kill_node(kill_id)
       
        print "Put 50 entries:"
        _put_entries("killowner", 50, retries = 10)
        if not _wait_until("/mon/domains/domain?id=0x" + domainid,
                        lambda x: check_entries(x, 6, 100), 300):
                print "Resync didn't finish in time"
                return False
       
        print "Owner reincarnates"
        new_node(kill_id)
        
        print "Put 50 entries:"
        _put_entries("killowner", 50, retries = 10)
        
        if _wait_until("/mon/domains/domain?id=0x" + domainid,
                        lambda x: check_entries(x, 7, 150), 300):
                return True
        else:
                print "Resync didn't finish in time"
                return False

# create owner and a replica that has a distant id. Put items. Kill owner,
# add items to the distant node. Reincarnate owner and add new nodes between
# the owner and the distant node. Check that resyncs ok. 
#
# NB: This test doesn't quite test what it should: Polling status from the 
# distant node activates it, which causes resync to active as well. In a more
# realistic case the owner would have to active the distant node by itself with
# the global resync process.
def test10_distantsync():
        did = domain_id("distantsync", 0)

        distant_id, p = new_node(make_domain_id(int(did, 16) - 20))
        time.sleep(1)
        owner_id, p = new_node(did)

        print "Owner is", owner_id
        print "Distant node is", distant_id
        if not _wait_until("/mon/ring/nodes",
                        lambda x: _check_results(x, 2), 30):
                print "Ring didn't converge"
                return False

        print "Create and populate domain"
        if not _test_repl("distantsync", 2, 3, 60, create_ring = False):
                print "Couldn't create and populate the domain"
                return False
                
        print "Kill owner", owner_id
        kill_node(owner_id)
        
        print "Put 30 entries:"
        _put_entries("distantsync", 30, retries = 10)

        print "Creating more node, reincarnating owner"
        if not _test_ring(0, 40, [make_domain_id(int(did, 16) + i)
                        for i in range(-19, 20)]):
                return False
        
        print "Putting 10 entries to the new owner"
        _put_entries("distantsync", 10)

        print "Waiting for everything to resync"
        if _wait_until("/mon/domains/domain?id=0x" + did,
                        lambda x: check_entries(x, 5, 100), 300):
                return True
        else:
                print "Resync didn't finish in time"
                return False

# This test checks that code updates can be perfomed smoothly in the
# ring. This happens by restarting nodes one by one. Restarting shouldn't
# distrupt concurrent put operations, except some re-requests may be
# needed.
def test11_simcodeupdate():
        names = ["simcodeupdate1", "simcodeupdate2"]
        def pput():
                rgo = ringogw.Ringo(sys.argv[1])
                for i in range(10):
                        k = "item-%d" % i
                        v = "testitem-%d" % i
                        for name in names:
                                rgo.put(name, k, v, retries = 10)
                                        
        did1 = int(domain_id(names[0], 0), 16)
        did2 = int(domain_id(names[1], 0), 16)
        mi = min(did1, did2)
        ids = [make_domain_id(mi + i) for i in range(10)]
        
        if not _test_ring(0, 10, ids):
                return False
        
        print "Creating domains.."

        for name in names:
                ringo.create(name, 6)

        print "Restarting nodes one at time:"
        for id in ids:
                print "Restart", id
                t = threading.Thread(target = pput).start()
                kill_node(id)
                new_node(id)
                # if the pause is too small, say 1sec, there's a danger
                # that replicas aren't yet fully propagated for the previous
                # requests and killing a node might make a replica jump over
                # the zombie and make a new replica domain. In the extreme
                # case the domain is propagated to all the nodes in the ring.
                time.sleep(7)
        
        print "All the nodes restarted."
        print "NB: Test may sometimes fail due to a wrong number of replicas,"
        print "typically 7 instead of 8. This is ok."

        # actually the number of replicas may be off by one here, if a put
        # requests hits a node while its down. So don't worry if the test fails
        # due to a wrong number of replicas.
        if not _wait_until("/mon/domains/domain?id=0x" + make_domain_id(did1),
                        lambda x: check_entries(x, 8, 100), 300):
                return False
        if not _wait_until("/mon/domains/domain?id=0x" + make_domain_id(did2),
                        lambda x: check_entries(x, 8, 100), 300):
                return False
        return True

# Check that putting large (1M) entries to external files works in
# replication and resyncing.
def test12_extsync():
        if not _test_ring(5):
                return False
        
        node, domainid = ringo.create("extsync", 6)
                
        v = "!" * 1024**2
        print "Putting ten 1M values"
        for i in range(10):
                ringo.put("extsync", "fub-%d" % i, v, verbose = True)
        if not _wait_until("/mon/domains/domain?id=0x" + domainid,
                        lambda x: x[0] == 200, 30):
                return False
        
        replicas = [n['node'].split('-')[1].split('@')[0] for n in\
                ringo.request("/mon/domains/domain?id=0x" + domainid)[1][3]]

        for repl in replicas:
                if not _check_extfiles(repl, domainid, 10):
                        print "Ext files not found on node", repl
                        return False
        print "Ext files written ok to all replicas"

        print "Deleting some files on node", replicas[0]
        print "to check that check_external works:" 
        files = os.listdir("%s/%s/rdomain-%s/" %
                        (home_dir, replicas[0], domainid))
        for rem in random.sample(
                        [f for f in files if f.startswith("value")], 5):
                print "Deleting", rem
                os.remove("%s/%s/rdomain-%s/%s" %
                        (home_dir, replicas[0], domainid, rem))
        
        newid, p = new_node()
        print "Creating a new node", newid
        if not _wait_until("/mon/ring/nodes",
                lambda x: _check_results(x, 6), 60):
                return False
        
        print "Putting an extra item (should go to the new node as well)"
        ringo.put("extsync", "extra", v, verbose = True)

        if not _wait_until("/mon/domains/domain?id=0x" + domainid,
                        lambda x: check_entries(x, 6, 11), 300):
                return False
        
        for repl in replicas + [newid]:
                if not _check_extfiles(newid, domainid, 11):
                        print "All ext files not found on node", repl 
                        return False
        return True

# Simple get test: Get a single value without chunked transfer
def test13_singleget():
        if not _test_ring(1):
                return False
        
        node, domainid = ringo.create("basicget", 5)        
        for i in range(5):
                ringo.put("basicget", "muppet-%d" % i, "nufnuf-%d" % i)

        for i in range(5):
                r = ringo.get("basicget", "muppet-%d" % i, single = True)
                if r != "nufnuf-%d" % i:
                        print "Invalid reply", r
                        return False
        return True

# Get multiple values: Test chunked transfer and entry_callback
def test14_multiget():
        def check_reply(entry, out):
                if entry != "nufnuf-%d" % len(out): 
                        raise "Invalid reply", entry
                out.append(entry)

        if not _test_ring(1):
                return False
        
        node, domainid = ringo.create("multiget", 5)
        print "Putting 1000 items.."
        for i in range(1000):
                ringo.put("multiget", "bork", "nufnuf-%d" % i)
        
        print "Getting 1000 items.."
        out = ringo.get("multiget", "bork", entry_callback = check_reply, verbose = True)
        if len(out) != 1000:
                raise "Invalid number of replies: %d" % len(out)
        return True

# Test that iblock cache works correctly with many iblocks
def test15_iblockcache():
        return _cache_test()
        
# Test that key cache works correctly with many iblocks
def test16_keycache():
        return _cache_test(keycache = True)

# Test that interleaved puts and gets work correctly with both the caches
def test17_putget(**kwargs):

        keycache = 'keycache' in kwargs
        if keycache:
                print "Testing with key cache"
        else:
                print "Testing with iblock cache"
                if not _test_ring(5):
                        return False
        
        dname = "putgettest-%s" % keycache
        node, domainid = ringo.create(dname, 5, **kwargs)
        values = ["zing-%d-%s" % (i, keycache) for i in range(2)]

        print "Putting and getting 15050 keys.."

        for i in range(1505):
                key = "k-%d-%s" % (i, keycache)
                for j in range(10):
                        for value in values:
                                r = ringo.put(dname, key, value)        
                        r = ringo.get(dname, key) 
                        c = values * (j + 1)
                        if r != c:
                                print key, j
                                raise "Invalid reply %s (%d) expected %s (%d)"\
                                        % (r, len(r), c, len(c))
        print "Results ok"

        if keycache:
                return True
        else:
                return test17_putget(keycache = True)

def single_get_check(dname, N):
        print "Check get.."
        for i in range(N):
                r = ringo.get(dname, "abc-%d" % i)
                if r != ['def-%d' % i]:
                        raise "Invalid reply to key %s: %s" %\
                                ("abc-%d" % i, r)
        print "Get ok"

# Test that missing or corrupted iblocks are re-generated correctly during
# index initialization. 
def test18_regeniblocks():
        N = 50500
        if not _test_ring(5):
                return False
        node, domainid = ringo.create("regentest", 5)
        print "Putting %d entries.." % N
        for i in range(N):
                ringo.put("regentest", "abc-%d" % i, "def-%d" % i)
        print "Entries put"
        single_get_check("regentest", N)

        kill_id = node.split('@')[0].split("-")[1]
        print "Kill owner", kill_id
        kill_node(kill_id)

        path = "%s/%s/rdomain-%s/" % (home_dir, kill_id, domainid)
        ifiles = sorted([(int(x.split('-')[1]), x)\
                        for x in os.listdir(path) if x.startswith("iblock")])
        if len(ifiles) != N / 10000:
                print "Incorrect number of iblocks: %d expected %d"\
                        % (len(ifiles), N / 10000)
                print "iblocks", ifiles
                return False
        iblocks = [] 
        print "Removing some iblocks"
        for i, iblock in enumerate(ifiles):
                fname = "%s/%s" % (path, iblock[1])
                f = file(fname).read()
                iblocks.append((iblock[1], md5.md5(f).hexdigest()))
                if i % 2:
                        print "Deleting", iblock[1]
                        os.remove(fname)
        
        print "Reincarnating owner"
        new_node(kill_id)
        if not _wait_until("/mon/ring/nodes",
                        lambda x: _check_results(x, 5), 60):
                return False
        single_get_check("regentest", N)
        print "Checking iblocks"
        for iblock, checksum in iblocks:
                fname = "%s/%s" % (path, iblock)
                f = file(fname).read()
                if checksum != md5.md5(f).hexdigest():
                        print "Checksums don't match for iblock", iblock
                        return False
        print "Checksums match"
        return True

# Test that gets are properly redirected when a new, empty owner is spawned.
# Note that especially interesting is the case when the new owner has resynced
# some of the entries, but not all, and GETs still need to be redirected to get
# the most comprehensive results.
def test19_redirget():
        N = 20500
        did = domain_id("redirget", 0)
        owner_id = real_owner = make_domain_id(int(did, 16) - 1)
        ids = [make_domain_id(int(did, 16) + i) for i in range(1, 10)]
       
        print "Create 9 replica nodes first.."
        if not _test_ring(0, 9, ids):
                return False

        print "Putting %d entries.." % N
        node, domainid = ringo.create("redirget", 5)
        for i in range(N):
                ringo.put("redirget", "abc-%d" % i, "def-%d" % i)
        single_get_check("redirget", N)
        print "Entries went to", node

        print "Creating owner node:", owner_id
        new_node(owner_id)
        if not _wait_until("/mon/ring/nodes",
                lambda x: _check_results(x, 10), 30):
                print "Ring didn't converge"
                return False
        
        single_get_check("redirget", N)
        
        # Owner and replicas will have different sizes here, thus check_size =
        # False. Iblocks are included in the chunk size, but owner doesn't have
        # them. Check_external() ignores iblocks, so they won't get to the owner
        # automatically so the owner size differs from the replica size.
        print "Waiting for resync.."
        if not _wait_until("/mon/domains/domain?id=0x" + domainid,
                        lambda x: check_entries(x, 7, N, check_size = False),\
                                300):
                print "Resync failed"
                return False

        print "Killing replicas.."
        for id in ids:
                kill_node(id)

        print "Waiting for ring to recover.."
        if not _wait_until("/mon/ring/nodes",
                lambda x: _check_results(x, 1), 60):
                return False

        single_get_check("redirget", N)
        return True

def _check_chunks(x, num_chunks):
        if x[0] != 200:
                return False
        if len(x[1]) != num_chunks:
                return False
        return True
        
# Basic chunking test: Put so many items that the maximum chunk size is
# exceeded multiple times. Check that the number of chunk is correct and
# all the items are retrieved ok.
def test20_manychunks():
        chunk_size = 500 * 1024
        N = (chunk_size / len("abc-0def-0")) * 2
        orig_max = os.environ['DOMAIN_CHUNK_MAX']
        os.environ['DOMAIN_CHUNK_MAX'] = str(chunk_size)
        if not _test_ring(1):
                return False
        node, domainid = ringo.create("manychunks", 5)
        
        print "Putting %d items" % N
        t = time.time()
        for i in range(N):
                ringo.put("manychunks", "abc-%d" % i, "def-%d" % i)
        print "Put took %dms" % ((time.time() - t) * 1000)
        
        # There's nothing special in 12 chunks. It just seems that it's
        # the correct number for this chunk size and these entries. If
        # the on-disk entry format changes, this number is likely to 
        # change too.
        if not _wait_until("/mon/domains/node?name=" + node,
                lambda x: _check_chunks(x, 12), 30):
                print "Couldn't find 12 chunks"
                return False
        
        print "Got a correct number of chunks"
        t = time.time()
        single_get_check("manychunks", N)
        print "Get took %dms" % ((time.time() - t) * 1000)
        os.environ['DOMAIN_CHUNK_MAX'] = orig_max
        return True

# Test chunks with replicas. Put many items, as in the manychunks test.
# Check that replicas are in sync and have the same size with the owner.
def test21_chunkrepl():
        chunk_size = 500 * 1024
        N = (chunk_size / len("abc-0def-0")) * 2
        orig_max = os.environ['DOMAIN_CHUNK_MAX']
        os.environ['DOMAIN_CHUNK_MAX'] = str(chunk_size)
        if not _test_ring(10):
                return False
        node, domainid = ringo.create("chunkrepl", 5)
        print "Putting %d items with replicas" % N
        t = time.time()
        for i in range(N):
                ringo.put("chunkrepl", "abc-%d" % i, "def-%d" % i)
        print "Put took %dms" % ((time.time() - t) * 1000)
        
        code, reply = ringo.request("/mon/domains/domain?id=0x" + domainid)
        orig_size = reply[3][0]['size']

        for i in range(12):
                chunkid = domain_id("chunkrepl", 0)
                print "Checking chunk", i
                # Make sure that all replicas for this chunk are of equal size
                if not _wait_until("/mon/domains/domain?id=0x" + chunkid,
                                lambda x: check_entries(x, 6, None), 300):
                        print "Resync didn't finish in time"
                        return False
                # Check that the size is the same for all the replicas, except
                # the last one
                if i < 11:
                        code, reply = ringo.request("/mon/domains/domain?id=0x" + chunkid)
                        chunk_size = reply[3][0]['size']
                        if chunk_size != orig_size:
                                print "Chunk %d has incorrect size %d, should be %d" %\
                                        (i, chunk_size, orig_size)
                                return False
                print "Chunk ok"
        t = time.time()
        single_get_check("chunkrepl", N)
        print "Get took %dms" % ((time.time() - t) * 1000)
        os.environ['DOMAIN_CHUNK_MAX'] = orig_max
        return True        


# Check that chunk size is re-computed correctly after a node is
# re-instantiated.
def test22_chunksizes():
        chunk_size = 500 * 1024
        orig_max = os.environ['DOMAIN_CHUNK_MAX']
        os.environ['DOMAIN_CHUNK_MAX'] = str(chunk_size)
        if not _test_ring(1):
                return False
        node, domainid = ringo.create("chunksizes", 5)
        print "Filling about 60% of the chunk.."
        for i in range(5000):
                ringo.put("chunksizes", "abc-%d" % i, "def-%d" % i)
        
        if not _wait_until("/mon/domains/domain?id=0x" + domainid,
                        lambda x: check_entries(x, 1, 5000), 300):
                print "Put failed"
                return False
        
        code, reply = ringo.request("/mon/domains/domain?id=0x" + domainid)
        orig_size = reply[3][0]['size']

        print "Kill node.."
        kill_node(domainid)
        time.sleep(1)
        print "Reinstantiate it.."
        new_node(domainid)
        print "Waiting for ring to recover.."
        if not _wait_until("/mon/ring/nodes",
                lambda x: _check_results(x, 1), 60):
                return False
        
        code, reply = ringo.request("/mon/domains/domain?id=0x" + domainid)
        new_size = reply[3][0]['size']
        if orig_size != new_size:
                print "Chunk size was %d bytes before and after reinstation "\
                      "%d bytes. No good." % (orig_size, new_size)
                return False
        print "Sizes match. Great!"

        print "Putting more items.."
        for i in range(5000, 10000):
                ringo.put("chunksizes", "abc-%d" % i, "def-%d" % i)
        print "Checking chunks.."
        if not _wait_until("/mon/domains/node?name=" + node,
                lambda x: _check_chunks(x, 2), 30):
                print "Couldn't find two chunks"
                return False
        single_get_check("chunksizes", 10000)
        os.environ['DOMAIN_CHUNK_MAX'] = orig_max
        return True

# Make node A, put 90% entries, kill A. make node B put 90%, restart A.
# After resyncing owner should be 180% full and only one chunk should 
# exist (note that percentages are totally approximate). The idea is 
# anyway that resyncing should work with closed domains.
def test23_resyncfull():
        def make_and_put(node_id, start):
                ringo.request("/mon/ring/reset")
                ringo.request("/mon/domains/reset")
                new_node(node_id)
                if not _wait_until("/mon/ring/nodes",
                        lambda x: _check_results(x, 1), 60):
                        return False
                print "Create domain"
                node, domainid = ringo.create("resyncfull", 2)
                print "Filling about 70% of the chunk.."
                for i in range(start, start + 8000):
                        ringo.put("resyncfull", "abc-%d" % i, "def-%d" % i)
                if not _wait_until("/mon/domains/domain?id=0x" + domainid,
                                lambda x: check_entries(x, 1, 8000), 300):
                        print "Put failed"
                        return False
                return True

        chunk_size = 500 * 1024
        orig_max = os.environ['DOMAIN_CHUNK_MAX']
        os.environ['DOMAIN_CHUNK_MAX'] = str(chunk_size)

        did = domain_id("resyncfull", 0)
        owner_id = make_domain_id(int(did, 16) - 1)
        another_id = make_domain_id(int(did, 16) + 1)
        print "Instantiating node A:", owner_id
        if not make_and_put(owner_id, 0):
                return False
        print "Kill node A"
        kill_node(owner_id)
        print "Instantiating node B:", another_id
        if not make_and_put(another_id, 8000):
                return False
        print "Reinstantiating node A"
        new_node(owner_id)
        # This just wakes up the domain
        code, reply = ringo.request("/mon/domains/domain?id=0x" + did)
        print "Waiting for resync.."
        if not _wait_until("/mon/domains/domain?id=0x" + did,
                        lambda x: check_entries(x, 2, 16000), 300):
                print "Resync failed"
                return False
        
        code, reply = ringo.request("/mon/domains/domain?id=0x" + did)
        if reply[3][0]['full'] == False:
                print "Chunk should be closed"
                return False
        
        single_get_check("resyncfull", 16000)
        os.environ['DOMAIN_CHUNK_MAX'] = orig_max
        return True

# X put, exceed chunk limit, check that new chunk is created. Check get.
# X put with replicas, exceed chunk limit, wait to converge, check that sizes
#   match
# X put to 50% chunk limit, kill node, put 50%, check that new chunk is 
#   created, kill node, put 50%, check that two chunks exist (big values too)
        
        
tests = sorted([f for f in globals().keys() if f.startswith("test")])

if len(sys.argv) > 2 and sys.argv[2] == '?':
        print "Available tests:\n", "\n".join([t[7:] for t in tests])
        sys.exit(1)

ringo = ringogw.Ringo(sys.argv[1])

for f in tests:
        prefix, testname = f.split("_", 1)
        if len(sys.argv) > 2 and testname not in sys.argv[2:]:
                continue
        kill_node("ringotest")
        ringo.request("/mon/ring/reset")
        ringo.request("/mon/domains/reset")
        time.sleep(1)
        print "*** Starting", testname
        if globals()[f]():
                print "+++ Test", testname, "successful"
        else:
                print "--- Test", testname, "failed"
                sys.exit(1)






