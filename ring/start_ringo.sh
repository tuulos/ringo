#!/bin/bash

if [ -z $1 ] 
then
	echo "Usage: start_ringo.sh [ringo_home]"
        exit 1
fi
if [ -z $RINGOSHELL ]
then
	SHELL="-noshell -detached -heart"
fi

export HEART_COMMAND="$0 $@"

RINGO=`dirname $0`
ID=`basename $1`
export ERL_CRASH_DUMP="ringo-$ID.erl_crash.dump"

if [ `pgrep -f "ringo-$ID"` ]
then
	echo "Node ringo-$ID already running"
	exit 1
fi

echo "Launching Ringo node [$ID]"
erl $SHELL -setcookie ringobingo +K true -smp on -pa $RINGO/bfile/ebin \
    -pa $RINGO/ebin -kernel error_logger "{file, \"ringo-$ID.log\"}"\
    -boot ringo -sname "ringo-$ID" -ringo ringo_home "\"$1\""
