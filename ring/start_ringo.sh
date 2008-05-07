#!/bin/bash

if [ -z $1 ] 
then 
        echo "Usage: start_ringo.sh [ringo_home]"
        exit 1
fi

ID=`basename $1`
echo "Launching Ringo node [$ID]"
erl -setcookie ringobingo +K true -smp on -pa bfile/ebin -pa ebin -pa src\
    -kernel error_logger "{file, \"ringo-$ID.log\"}"\
    -boot ringo -sname "ringo-$ID" -ringo ringo_home "\"$1\""
