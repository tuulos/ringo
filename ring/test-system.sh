export PATH=$PATH:. 
export PYTHONPATH=../ringogw/py
python -u test/system_test.py localhost:15000 $1
