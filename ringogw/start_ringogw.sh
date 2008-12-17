
MODE=mochi
PORT=15000
SCGI=1

if [ ! -z $SCGI ]
then
	MODE=scgi
	PORT=15001
	echo "SCGI mode"
fi

export HEART_COMMAND="$0 $@"

PATH=$PATH:/usr/sbin lighttpd -f lighttpd.conf -D &

PATH=.:$PATH erl -heart -noshell -detached +K true -smp on -sname ringogw -setcookie ringobingo -pa ../mochiweb/ebin -pa ../ring/ebin -pa ebin -pa src -boot ringogw -ringogw httpmode $MODE -ringogw port $PORT -ringogw docroot \"web\" -ringogw dynroot \"mon\" -kernel error_logger '{file, "ringogw.log"}' -eval "[handle_ring, handle_domains, handle_data]"

