
echo
echo "Compiling tests.."
erlc +native +"{hipe, [o3]}" -I src/ -o test test/*.erl
erlc +native +"{hipe, [o3]}" -o ebin src/ringo_writer.erl src/ringo_reader.erl
echo

rm -Rf test/test_data
mkdir test/test_data
cd test

echo "*** Write test ***"
erl -pa ../ebin -noshell -sname hip-$i -run test_readwrite write_test 100000
echo

echo "*** Read test ***"
erl -pa ../ebin -noshell -sname hip-$i -run test_readwrite read_test 100000
echo

echo "*** Corrupt test (should complain about 657. entry) ***"
dd if=/dev/urandom conv=notrunc of=test_data/data seek=43345 bs=1 count=2000 2>/dev/null
erl -pa ../ebin -noshell -sname hip-$i -run test_readwrite read_test 100000
echo

cd ..
echo "ok"
