import tempfile, os, subprocess, md5, time, cjson, sys, httplib

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

def ringomon(req):
        global conn
        if conn == None:
                conn = httplib.HTTPConnection(sys.argv[1])
        try:
                conn.request("GET", req)
                r = conn.getresponse()
                resp = r.read()
                return cjson.decode(resp)
        except httplib.BadStatusLine:
                conn = None
                return ringomon(req)

def _test_ring(n):
        def check_results(nodes):
                l = len([node for node in nodes if node['ok']])
                return len(nodes) == l == n

        print "Launching nodes",
        res = []
        for i in range(n):
                time.sleep(1)
                res.append(new_node())
                sys.stdout.write(".")
        print
        t = time.time()
        if _wait_until("/mon/ring/nodes", check_results, 30):
                print "Ring converged in %d seconds" % (time.time() - t)
                return True
        return False


def _wait_until(req, check, timeout):
        print "Checking results",
        for i in range(timeout):
                time.sleep(1)
                r = ringomon(req)
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

g = globals()
for f in (f for f in g.keys() if f.startswith("test_")):
        kill_node("ringotest")
        ringomon("/mon/ring/reset")
        ringomon("/mon/domains/reset")
        time.sleep(1)
        print "*** Starting", f
        if g[f]():
                print "+++ Test", f, "successful"
        else:
                print "--- Test", f, "failed"
                sys.exit(1)






