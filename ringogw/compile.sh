#erlc  +native +"{hipe, [o3]}" -I ../ring/src -o ebin src/*.erl
#erlc -o ebin src/mochi_dispatch.erl

erlc -I ../ring/src -o ebin src/*.erl
erl -pa ebin -noshell -run make_boot write_scripts
