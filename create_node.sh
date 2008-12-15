#!/bin/bash

if [ -z $1 ]; then
	echo "Usage: create_node.sh [host] [ringo_data]"
	exit 1
fi

node=`dd if=/dev/urandom bs=1024 count=1 2>/dev/null | md5sum | cut -d ' ' -f 1`
ssh $1 "mkdir -p $2/$node"

