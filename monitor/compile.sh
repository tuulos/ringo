erlc +native +"{hipe, [o3]}" -o ebin src/*.erl
#erlc -o ebin src/*.erl
erl -pa ebin -noshell -run make_boot write_scripts
