#!/bin/bash

if [ -z $1 ] 
then 
        echo "Usage: start_ringo.sh [ringo_home]"
        exit 1
fi

ID=`basename $1`
echo "Launching Ringo node [$ID]"
erl +A 128 -setcookie ringobingo +K true -smp on -pa ebin -pa src\
    -boot ringo -sname "ringo-$ID" -ringo ringo_home "\"$1\""
