#!/usr/bin/env bash

type=$1
if [ "$1" == "cf" ] ; then
    find src tests bindings -iname '*.h' -o -iname '*.c' | xargs clang-format -i
    if [ -n "$(git diff)" ] ; then
        echo "clang-format check failed"
        exit -1
    fi
fi

exit 0
