#!/bin/bash

if [ -z $1 ]; then
	echo "Usage: delete_domain.sh [node] [ringo_data] [domainid]"
	exit 1
fi

ssh $1 "find $2 -iname 'rdomain-$3' -exec rm -Rf \{\} \;"
