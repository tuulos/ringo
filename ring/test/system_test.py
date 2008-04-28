import tempfile, os, subprocess, md5, time, sys, random
import ringogw

home_dir = tempfile.mkdtemp("", "ringotest-") + '/'
node_id = 0
conn = None

def new_node(id = None):
        global node_id
        if id == None:
                id = md5.md5("test-%d" % node_id).hexdigest()
                node_id += 1
        path = home_dir + id
        os.mkdir(path)
        p = subprocess.Popen(["start_ringo.sh", path],
                stdin = subprocess.PIPE, stdout = subprocess.PIPE,
                        stderr = subprocess.PIPE)
        return id, p

def kill_node(id):
        subprocess.call(["pkill", "-f", id])

def domain_id(name, chunk):
        return md5.md5("%d %s" % (chunk, name)).hexdigest()

def check_reply(reply):
        if reply[0] != 200 or reply[1][0] != 'ok':
                raise Exception("Invalid reply (code: %d): %s" %\
                        (reply[0], reply[1]))
        return reply[1][2:]

def check_entries(r, nrepl, nentries):
        if r[0] != 200:
                return False
        if len(r[1][3]) != nrepl:
                return False

        owner_root = -2
        roots = []
        for node in r[1][3]:
                num = node["num_entries"]
                #print "NUM", num, nentries
                if num == "undefined" or int(num) != nentries:
                        return False
                root = -1
                if "synctree_root" in node:
                        root = node["synctree_root"][0][1]
                        roots.append(root)
                if node["owner"]:
                        owner_root = root

        #print "ROOT", owner_root, "ROOTS", roots
        if len(roots) != nrepl:
                return False

        # check that root hashes match
        return [r for r in roots if r != owner_root] == []

def _check_results(reply, num):
        if reply[0] != 200:
                return False
        nodes = reply[1]
        l = len([node for node in nodes if node['ok']])
        return len(nodes) == l == num

def _test_ring(n, num = None):
        if num == None:
                check = lambda x: _check_results(x, n)
        else:
                check = lambda x: _check_results(x, num)

        print "Launching nodes",
        res = []
        for i in range(n):
                time.sleep(1)
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
                r = ringogw.request(req)
                if check(r):
                        print
                        return True
                sys.stdout.write(".")
        print
        return False

def test01_ring10():
        return _test_ring(10)

def test02_ring100():
        return _test_ring(100)

def test03_ring_newnodes():
        print "Launching first batch of nodes"
        if not _test_ring(10):
                return False
        time.sleep(10)
        print "Launching second batch of nodes"
        return _test_ring(10, 20)

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

def _put_entries(name, nitems):
        t = time.time()
        for i in range(nitems):
                check_reply(ringogw.request("/mon/data/%s/item-%d" % (name, i),
                        "testitem-%d" % i))
        print "%d items put in %dms" % (nitems, (time.time() - t) * 1000)

def _test_repl(name, n, nrepl, nitems, create_ring = True):
        if create_ring and not _test_ring(n):
                return False
        node, domainid = check_reply(ringogw.request(
                "/mon/data/%s?create&nrepl=%d" % (name, nrepl), ""))[0]
        _put_entries(name, nitems)
        return _wait_until("/mon/domains/domain?id=0x" + domainid,
                lambda x: check_entries(x, n, nitems), 50)

def test05_replication1():
        return _test_repl("test_replication1", 1, 1, 100)

def test06_replication50():
        return _test_repl("test_replication50", 50, 50, 100)

def test07_addreplica(first_owner = True):
        if first_owner:
                name = "addreplicas_test"
        else:
                name = "addowner_test"
        check1 = lambda x: _check_results(x, 1)
        check2 = lambda x: _check_results(x, 2)
        did = domain_id(name, 0)
        # a node id that is guaranteed to become owner for the domain
        owner_id = real_owner = hex(int(did, 16) - 1)[2:-1]
        # a node id that is guaranteed to become replica for the domain
        repl_id = hex(int(did, 16) + 1)[2:-1]
        
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
        
        re = ringogw.request("/mon/domains/domain?id=0x" + did)
        repl = re[1][3]
        
        if real_owner in [r['node'] for r in repl if r['owner']][0]:
                print "Owner matches"
                return True
        else:
                print "Invalid owner for domain %s (should be %s), got: %s" %\
                        (did, owner_id, repl)
                return False


def test08_addowner():
        return test07_addreplica(False)

        
# X 1. (1 domain) create, put -> check that succeeds, number of entries
# X 2. (2 domains) create, put -> succeeds, replicates, #entries
# X 3. (10 domains) create put -> succeeds, replicates, #entries
# 4. (1 domain) create, put, add new domain (replica), check that replicates
# 5. (1 domain) create, put, add new domain (new owner), check that owner moves
#        correctly and replicates
# 6. (2 domains) create, put, kill owner, put, check that works
# 7. (100 domains) create N domains, put entries, killing random domains at the
#        same time, check that all entries available in the end
# - same with large, external entries
# - testcase that simulates code update: crash nodes one by one in sequence,
#   should cause minimal / none downtime

ringogw.sethost(sys.argv[1])
for f in sorted([f for f in globals().keys() if f.startswith("test")]):
        prefix, testname = f.split("_", 1)
        if len(sys.argv) > 2 and testname not in sys.argv[2:]:
                continue
        kill_node("ringotest")
        ringogw.request("/mon/ring/reset")
        ringogw.request("/mon/domains/reset")
        time.sleep(1)
        print "*** Starting", testname
        if globals()[f]():
                print "+++ Test", testname, "successful"
        else:
                print "--- Test", testname, "failed"
                sys.exit(1)






