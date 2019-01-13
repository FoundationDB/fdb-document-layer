#!/bin/bash

set -ex

/var/fdb/scripts/fdb.bash &
FDB_PID=$!

sleep 1
fdbcli --exec "configure new single memory;"

wait ${FDB_PID}
