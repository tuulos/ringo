#!/bin/bash

function run {
        erl -pa ../ebin -noshell -run $@
        echo
}

echo
echo "Compiling tests.."
#erlc +native +"{hipe, [o3]}" -I src/ -o test test/*.erl
#erlc +native +"{hipe, [o3]}" -o ebin src/ringo_writer.erl src/ringo_reader.erl
erlc -I src/ -o test test/*.erl
erlc -o ebin src/ringo_writer.erl src/ringo_reader.erl\
             src/trunc_io.erl src/ringo_sync.erl\
             src/bin_util.erl
echo

rm -Rf test/test_data
mkdir test/test_data
cd test


if [[ -z $1 || $1 == "rw" ]]; then
echo "*** Write test ***"
run test_readwrite write_test 100000
echo "*** Read test ***"
run test_readwrite read_test 100000
echo "Writing more entries.."
run test_readwrite write_test 100000
run test_readwrite read_test 200000
fi


if [[ -z $1 || $1 == "corrupt" ]]; then
echo "*** Corrupt test (should complain about 656. entry) ***"
if [[ $1 == "corrupt" ]]; then
        run test_readwrite write_test 100000
fi
dd if=/dev/urandom conv=notrunc of=test_data/data\
        seek=43345 bs=1 count=2000 2>/dev/null
run test_readwrite read_test 100000
fi

if [[ -z $1 || $1 == "extfile" ]]; then
echo "*** Extfile test ***"
echo "Writing.."
run test_readwrite extfile_write_test 1000
echo "Reading.."
run test_readwrite extfile_read_test
fi

if [[ -z $1 || $1 == "sync" ]]; then
echo "*** Basic sync-tree test ***"
run test_sync basic_tree_test 100000
echo "*** ID-list test ***"
run test_sync idlist_test
echo "*** Diff test ***"
run test_sync diff_test 1000
fi


cd ..
echo "ok"
