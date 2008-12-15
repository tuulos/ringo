#!/bin/bash

if [ -z $1 ]; then
	echo "Usage: start_nodes.sh [host] [ringo_data]"
	exit 1
fi

if [ -z $RIBGO_ROOT ]; then
        RINGO_ROOT=$(cd `dirname $0`; pwd)
fi
echo "RINGO_ROOT is $RINGO_ROOT"

for id in `ssh $1 "ls -1 $3"`
do
	echo "Starting ringo-$id"
	ssh $1 "$RINGO_ROOT/ring/start_ringo.sh $3/$id"
	sleep 1
done

