#!/bin/bash

if [ -z $1 ]; then
	echo "Usage: start_nodes.sh [host] [ringo_data]"
	exit 1
fi

if [ ! -e ~/.hosts.erlang ]; then
        echo -e "\nYou don't seem to have an existing ~/.hosts.erlang file."
        echo "This file is required and it should contain a list of all possible hostnames"
        echo -e "that can contain active nodes in the ring. At the bare minimum, you can say:\n"
        echo -e "echo \"'localhost'.\" > ~/.hosts.erlang\n"
        echo "if you have only one node that is your localhost. See 'man 3erl net_adm' for"
        echo -e "further information about the .hosts.erlang file.\n"
        exit 1
fi

if [ -z $RIBGO_ROOT ]; then
        RINGO_ROOT=$(cd `dirname $0`; pwd)
fi
echo "RINGO_ROOT is $RINGO_ROOT"

for id in `ssh $1 "ls -1 $2"`
do
	echo "Starting ringo-$id"
	ssh $1 "$RINGO_ROOT/ring/start_ringo.sh $2/$id"
	sleep 1
done

