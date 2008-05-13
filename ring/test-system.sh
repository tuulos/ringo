export PATH=$PATH:. 
export PYTHONPATH=../ringogw/py
rm -Rf /tmp/ringotest-*
python -u test/system_test.py localhost:15000 $@
