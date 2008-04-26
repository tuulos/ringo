import tempfile, os, subprocess, md5, time, sys, random
import ringogw

home_dir = tempfile.mkdtemp("", "ringotest-") + '/'
node_id = 0
conn = None

def new_node():
        global node_id
        id = md5.md5("test-%d" % node_id).hexdigest()
        path = home_dir + id
        node_id += 1
        os.mkdir(path)
        p = subprocess.Popen(["start_ringo.sh", path],
                stdin = subprocess.PIPE, stdout = subprocess.PIPE,
                        stderr = subprocess.PIPE)
        return id, p

def kill_node(id):
        subprocess.call(["pkill", "-f", id])

#def ringogw(req, data = None):
#        global conn
#        if conn == None:
#                print "PIIP"
#                conn = httplib.HTTPConnection(sys.argv[1])
#        try:
#                t = time.time()
#                if data == None:
#                        conn.request("GET", req)
#                else:
#                        conn.request("POST", req, data)
#                r = conn.getresponse()
#                resp = r.read()
#                print "Got response in %dms" % ((time.time() - t) * 1000)
#                return cjson.decode(resp)
#        except httplib.BadStatusLine:
#                conn = None
#                return ringogw(req, data)

def check_reply(reply):
        if reply[0] != 'ok':
                raise Exception("Invalid reply: %s" % reply)
        return reply[2:]

def _check_results(nodes, num):
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

def test_ring10():
        return _test_ring(10)

def test_ring100():
        return _test_ring(100)

def test_ring_newnodes():
        print "Launching first batch of nodes"
        if not _test_ring(10):
                return False
        time.sleep(10)
        print "Launching second batch of nodes"
        return _test_ring(10, 20)

def test_ring_randomkill():
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

def test_basicput():
        id, proc = new_node()
        print "Waiting for the ring to settle down..."
        time.sleep(5)
        check_reply(ringogw.request("/mon/data/basicput?create", ""))
        t = time.time()
        for i in range(100):
                check_reply(ringogw.request("/mon/data/basicput/item-%d" % i,
                        "testitem-%d" % i))
        print "100 items put in %dms" % ((time.time() - t) * 1000)
        
        return True



        
# 1. (1 domain) create, put -> check that succeeds, number of entries
# 2. (2 domains) create, put -> succeeds, replicates, #entries
# 3. (10 domains) create put -> succeeds, replicates, #entries
# 4. (1 domain) create, put, add new domain (replica), check that replicates
# 5. (1 domain) create, put, add new domain (new owner), check that owner moves
#        correctly and replicates
# 6. (2 domains) create, put, kill owner, put, check that works
# 7. (100 domains) create N domains, put entries, killing random domains at the
#        same time, check that all entries available in the end
# - same with large, external entries
        
ringogw.sethost(sys.argv[1])
g = globals()
for f in (f for f in g.keys() if f.startswith("test_")):
        if len(sys.argv) > 2 and f not in sys.argv[2:]:
                continue
        testname = f[5:]
        kill_node("ringotest")
        ringogw.request("/mon/ring/reset")
        ringogw.request("/mon/domains/reset")
        time.sleep(1)
        print "*** Starting", testname
        if g[f]():
                print "+++ Test", testname, "successful"
        else:
                print "--- Test", testname, "failed"
                sys.exit(1)






