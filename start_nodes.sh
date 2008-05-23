#!/bin/bash

if [ -z $1 ]; then
	echo "Usage: start_nodes.sh [host] [ringo_root]"
	exit 1
fi

for id in `ssh $1 "ls -1 $2"`
do
	echo "Starting ringo-$id"
	ssh $1 "ringo/ring/start_ringo.sh $2/$id"
	sleep 1
done

