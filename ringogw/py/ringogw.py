
import cjson, pycurl, cStringIO, time, re

multi_head_re = re.compile("(\d+) (.*?) ")

host = ""
curl = None

class ReplyException(Exception):
        pass

class DecodeRaw:
        def __init__(self):
                self.buf = cStringIO.StringIO()
        def write(self, data):
                self.buf.write(data)
        def output(self):
                return self.buf.getvalue()

class DecodeJson:
        def __init__(self):
                self.buf = cStringIO.StringIO()
        def write(self, data):
                self.buf.write(data)
        def output(self):
                return cjson.decode(self.buf.getvalue())

class DecodeMulti:
        def __init__(self, cb = None):
                if not cb:
                        cb = lambda e, out: out.append(e)
                self.cb = cb
                self.buf = ""
                self.ret = "ok"
                self.out = []
                self.entrylen = None

        def write(self, data):
                self.buf += data
                while True:
                        # State 1: Read header, if available
                        if self.entrylen == None:
                                m = multi_head_re.match(self.buf)
                                if m:
                                        sze, ret = m.groups()
                                        self.entrylen = int(sze)
                                        if ret != 'ok':
                                                self.ret = ret
                                        self.buf = self.buf[m.end():]
                                else:
                                        # Wait for more data
                                        break
                        # State 2: Read body, if available
                        elif len(self.buf) >= self.entrylen:
                                r = self.cb(self.buf[:self.entrylen], self.out)
                                self.buf = self.buf[self.entrylen:]
                                self.entrylen = None
                        else:
                                # Wait for more data
                                break

        def output(self):
                if self.buf:
                        raise ReplyException("%d extra bytes in the stream" %
                                len(self.buf))
                else:
                        return self.ret, self.out

def sethost(url):
        global host
        if not url.startswith("http://"):
                host = "http://" + url
        host = url

def check_reply(reply):
        if reply[0] != 200 or reply[1][0] != 'ok':
                e = ReplyException("Invalid reply (code: %d): %s" %\
                        (reply[0], reply[1]))
                e.retcode = reply[0]
                e.retvalue = reply[1]
                raise e 
        return reply[1][1:]

def create(domain, nrepl, **kwargs):
        kwargs['decoder'] = DecodeJson
        return check_reply(request("/mon/data/%s?create&nrepl=%d" %
                (domain, nrepl), "", **kwargs))[0]

def put(domain, key, value, **kwargs):
        kwargs['decoder'] = DecodeJson
        return check_reply(request("/mon/data/%s/%s" % (domain, key),
                value, **kwargs))
        
def get(domain, key, **kwargs):
        url = "/mon/data/%s/%s" % (domain, key)
        kwargs['data'] = None
        if 'single' in kwargs:
                kwargs['decoder'] = DecodeRaw
                del kwargs['single']
                code, val = request(url + "?single", **kwargs)
                if code != 200:
                        e = ReplyException("Invalid reply (code: %d)" % code)
                        e.retcode = code
                        raise e
                return val
        else:
                if 'entry_callback' in kwargs:
                        cb = kwargs['entry_callback']
                        kwargs['decoder'] = lambda: DecodeMulti(cb)
                        del kwargs['entry_callback']
                else:
                        kwargs['decoder'] = DecodeMulti
                return check_reply(request(url, **kwargs))[0]


# Although Curl's documentation suggests re-using a curl object for
# multiple requests, doing it seems to slow things down considerably.
# The first request is fast but requests after that are not (except 
# some requests occasionally with the lighttpd/scgi combination). Weird.
# TODO: Check whether TCP_NODELAY could help.
def request(url, data = None, verbose = False,
            keep_alive = False, retries = 0, decoder = DecodeJson):
        global curl
        if url.startswith("http://"):
                purl = url
        else:
                purl = host + url

        if not (curl and keep_alive):
                curl = pycurl.Curl()
        curl.setopt(curl.URL, purl)
        if data == None:
                curl.setopt(curl.HTTPGET, 1)
        else:
                curl.setopt(curl.POST, 1)
                curl.setopt(curl.POSTFIELDS, data)
                curl.setopt(curl.HTTPHEADER, ["Expect:"])
        dec = decoder()
        curl.setopt(curl.WRITEFUNCTION, dec.write)
        try:
                curl.perform()
        except pycurl.error, x:
                if verbose:
                        print "Pycurl.error:", x
                return x

        code = curl.getinfo(curl.HTTP_CODE)
        if verbose:
                print "Request took %.2fms" %\
                        (curl.getinfo(curl.TOTAL_TIME) * 1000.0)
        
        if not keep_alive:
                curl = None

        # Request timeout
        if code == 408 and retries > 0:
                time.sleep(0.1)
                return request(url, data, verbose,
                        keep_alive, retries - 1)
        else: 
                return code, dec.output()

        

                




