erl -noshell -pa bfile/ebin -pa ebin -run bfile load_driver -run ringo_debug $1 $2 -run erlang halt
