
import cjson, pycurl, cStringIO, time

host = ""
curl = None

def sethost(url):
        global host
        if not url.startswith("http://"):
                host = "http://" + url
        host = url

# Although Curl's documentation suggests re-using a curl object for
# multiple requests, doing it seems to slow things down considerably.
# The first request is fast but requests after that are not (except 
# some requests occasionally with the lighttpd/scgi combination). Weird.
def request(url, data = None, verbose = False,
            keep_alive = False, retries = 0):
        global curl
        if not url.startswith("http://"):
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
        buf = cStringIO.StringIO()
        curl.setopt(curl.WRITEFUNCTION, buf.write)
        try:
                curl.perform()
        except pycurl.error, x:
                if verbose:
                        print "Pycurl.error:", x
                return x

        b = buf.getvalue()
        # FIXME: Add automatic re-requesting if return code is
        # "request timeout"
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
                return code, cjson.decode(b)

        

                




