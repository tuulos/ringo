
MODE=mochi
PORT=15000

if [ ! -z $SCGI ]
then
	MODE=scgi
	PORT=15001
	echo "SCGI mode"
fi

PATH=.:$PATH erl +K true -smp on -sname ringomon -setcookie ringobingo -pa ../mochiweb/ebin -pa ../ring/ebin -pa ebin -pa src -boot ringomon -ringomon httpmode $MODE -ringomon port $PORT -ringomon docroot \"web\" -ringomon dynroot \"mon\" -kernel error_logger '{file, "ringomon.log"}' -eval "[handle_ring, handle_domains, handle_chunkstat]"
