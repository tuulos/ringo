export PATH=$PATH:. 
export PYTHONPATH=../ringogw/py
rm -Rf /tmp/ringotest-*

if ! wget -O - localhost:15000/mon/ring/reset >/dev/null 2>/dev/null
then
        echo "Ringogw doesn't seem to be running at http://localhost:15000"
else
        python -u test/system_test.py localhost:15000 $@
fi
