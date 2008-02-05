
echo
echo "Compiling tests.."
#erlc +native +"{hipe, [o3]}" -I src/ -o test test/*.erl
#erlc +native +"{hipe, [o3]}" -o ebin src/ringo_writer.erl src/ringo_reader.erl
erlc -I src/ -o test test/*.erl
erlc -o ebin src/ringo_writer.erl src/ringo_reader.erl\
             src/trunc_io.erl src/ringo_sync.erl
echo

rm -Rf test/test_data
mkdir test/test_data
cd test


if [[ -z $1 || $1 == "rw" ]]; then
echo "*** Write test ***"
erl -pa ../ebin -noshell -run test_readwrite write_test 100000
echo
echo "*** Read test ***"
erl -pa ../ebin -noshell -run test_readwrite read_test 100000
echo "Writing more entries.."
erl -pa ../ebin -noshell -run test_readwrite write_test 100000
erl -pa ../ebin -noshell -run test_readwrite read_test 200000
echo
fi


if [[ -z $1 || $1 == "corrupt" ]]; then
echo "*** Corrupt test (should complain about 656. entry) ***"
if [[ $1 == "corrupt" ]]; then
        erl -pa ../ebin -noshell -run test_readwrite write_test 100000
fi
dd if=/dev/urandom conv=notrunc of=test_data/data seek=43345 bs=1 count=2000 2>/dev/null
erl -pa ../ebin -noshell -run test_readwrite read_test 100000
echo
fi

if [[ -z $1 || $1 == "extfile" ]]; then
echo "*** Extfile test ***"
echo "Writing.."
erl -pa ../ebin -noshell -run test_readwrite extfile_write_test 1000
echo "Reading.."
erl -pa ../ebin -noshell -run test_readwrite extfile_read_test
echo
fi

if [[ -z $1 || $1 == "basicsync" ]]; then
echo "*** Basic sync-tree test ***"
erl -pa ../ebin -noshell -run test_sync basic_tree_test 100000
echo
fi

cd ..
echo "ok"
