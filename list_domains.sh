#!/bin/bash

if [ -z $1 ]; then
	echo "Usage: list_domains.sh [node] [ringo_data]"
	exit 1
fi

for f in `ssh $1 find $2 -iname 'rdomain-*'`; do
	T=`basename $f`
	echo ${T:8}
done
