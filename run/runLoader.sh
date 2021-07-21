#!/usr/bin/env bash

if [ $# -lt 1 ] ; then
    echo "usage: $(basename $0) PROPS_FILE [ARGS]" >&2
    exit 2
fi

source funcs.sh $1
shift

# Set myCP according to the database type.
setCP || exit 1

myOPTS="-Dprop=$PROPS"
myOPTS="$myOPTS -DloadDataFile=./sql.internal/loadData.sql -DtpccFile=./sql.internal/tpcc.sql"
java -cp "$myCP" $myOPTS icu.windea.benchmarksql.LoadData $*
