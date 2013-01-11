
Ringo - Distributed key/value storage for immutable data
--------------------------------------------------------

Ringo is an experimental, distributed, replicating key-value store based
on consistent hashing and immutable data. Unlike many general-purpose
databases, Ringo is designed for a specific use case: For archiving
small (less than 4KB) or medium-size data items (<100MB) in real-time
so that the data can survive K - 1 disk breaks, where K is the desired
number of replicas, without any downtime, in a manner that scales to
terabytes of data. In addition to storing, Ringo should be able to
retrieve individual or small sets of data items with low latencies
(<10ms) and provide a convenient on-disk format for bulk data access.

WARNING: Ringo should not be used yet as a primary storage for critical
data. Due to the fact that Ringo treats all data immutable, data
corruption or loss should be improbable. However, Ringo needs more
testing in real-world settings before we can be reasonably sure that it
works as advertised. To make this happen, feel free to try it out, adapt 
it to your needs, and report your experiences!

If you find Ringo interesting, you might also want to check out a paper
about Amazon's Dynamo

http://s3.amazonaws.com/AllThingsDistributed/sosp/amazon-dynamo-sosp2007.pdf

and another Dynamo-like system, Scalaris, which is also implemented in
Erlang: http://code.google.com/p/scalaris/


Requirements
''''''''''''

Backend system (required):

- Erlang R12B or newer
- C toolchain and autotools (for building bfile)
- Lighttpd or other web server that supports SCGI

Test harness / Python interface (optional):

- pkill command (Debian package procps)
- Python
- Python module pycurl (Debian package python-pycurl)
- Python module cjson (Debian package python-cjson)


Directories
'''''''''''

doc/         Documentation

ring/        Ringo backend
ring/bfile   High-performance replacement for the Erlang's standard file 
             module
ring/src     Backend sources
ring/test    Test harness

ringogw/     Ringo web frontend
ringogw/src  Frontend sources
ringogw/web  Web interface
ringogw/py   Python interfaces for the Ringo frontend and for the Disco 
             map/reduce framework


Compiling
'''''''''

cd ringo
./compile.sh


Starting Ringo
''''''''''''''

First you need to initialize a number of virtual nodes - at least one is
required. A virtual node is defined by an empty directory whose name is
a random 128-bit integer in hexadecimal notation.

A convenience script, create_node.sh, is provided that can be used to
setup a virtual node. For instance,

ringo/create_node.sh trurl /data/ringo

sets up a new virtual node on the host trurl to the directory
/data/ringo. The script uses ssh to log in to the host. It asks for a
password unless key-based ssh authentication is properly set up (which
is recommended). You may run this script on different nodes as many
times as you like.

You need to list all hostnames that may possibly host a Ringo node
in a file called ~/.hosts.erlang. For further information about this
file, see an Erlang manual page at "man 3erl net_adm". For instance the
following command creates the required file for the virtual node that
was initialized above:

echo "'trurl'." > ~/.hosts.erlang

After a number of virtual nodes have been initialized, Ringo may be
started. Again, a simple script is provided that starts up all virtual
nodes on a specified host. For instance,

ringo/start_nodes.sh trurl /data/ringo

starts up all nodes on the host trurl. After a while, the ring should
be up and running.

The web frontend provides a convenient way to monitor status of the 
system. An example configuration file for the Lighttpd web server is
provided at ringo/ringogw/lighttpd.conf that communicates with the
frontend process over SCGI. The following script starts up the 
web server and the frontend process:

ringo/ringogw/start_ringogw.sh

Now you should see the status page at http://localhost:15000. On the
status page, you can click nodes on the leftmost panel to see domains
that they contain. You can click a domain on the middle panel to see its
replicas. By clicking a replica, you can see its status. You might need
to wait for 10 seconds or so, and reload the page, to see new nodes and
domains appear.


Usage
'''''

You can create domain, put keys and get keys from Ringo using simple
HTTP requests. Assuming that you have started a ring as instructed
above, you can create a new domain called "foobar" with the following
POST request. Here curl is used to make a request but any other HTTP
client would work as well:

curl -d "" http://localhost:15000/mon/data/foobar?create

You can put a new key-value pair to the domain with following POST
request:

curl -d "testvalue" http://localhost:15000/mon/data/foobar/testkey

and retrieve the value given the key with a GET request as follows:

curl http://localhost:15000/mon/data/foobar/testkey

this returns all values assigned with the key "testkey". If only one
value is required, the parameter ?single can be used:

curl http://localhost:15000/mon/data/foobar/testkey?single

A Python class Ringo is provided at ringo/ringogw/py/ringogw.py that
encapsulates the above HTTP requests in Python function calls.

An experimental interface for Disco, an an open-source implementation
of the Map/Reduce framework (http://discoproject.org), can be found at
ringo/ringogw/py/ringodisco.py. This function, which implements the
Disco's map reader interface, makes it possible to use data stored in
Ringo as input to a Disco job. The function provides a particularly
efficient way of accessing data directly from Ringo's live data files.


Running the Test Harness
''''''''''''''''''''''''

Ringo comes with a set of tests that cover all main features of the
system. You need to start the ringogw web frontend process to run
the tests, as shown above. After the frontend has started, you can
run the tests with the following command:

cd ringo/ring/; ./test-system.sh


Contact information
'''''''''''''''''''

Bug reports, patches, comments etc. are welcome! Contact person is 
Ville Tuulos who can be reached at

ville.h.tuulos -a- nokia.com

or on the IRC channel #discoproject at Freenode.

