#!/bin/bash

set -eu

# xargs is used to trim leading spaces
num_tests=`ls tests/*.exp | wc -l | xargs` 
current=1

for f in `ls tests/*.exp`
do
    echo -ne "[$current/$num_tests] $f...\t"
    expect $f >/dev/null
    echo -e "\033[1;32m PASSED\033[0m"
    current=$((current+1))
done
