
PATH=.:$PATH erl +K true -smp on -sname ringomon -setcookie ringobingo -pa ../ring/ebin -pa ebin -pa src -boot ringomon -ringomon scgi_port 15001 -kernel error_logger '{file, "ringomon.log"}' -eval "[handle_ring, handle_chunks, handle_chunkstat]"
