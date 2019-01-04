#! /bin/bash

function setup_cluster_file() {
	FDB_CLUSTER_FILE=${FDB_CLUSTER_FILE:-/etc/foundationdb/fdb.cluster}
	mkdir -p $(dirname $FDB_CLUSTER_FILE)

	if [[ -n "$FDB_CLUSTER_FILE_CONTENTS" ]]; then
		echo "$FDB_CLUSTER_FILE_CONTENTS" > $FDB_CLUSTER_FILE
    fi

    if [ ! -f ${FDB_CLUSTER_FILE} ]; then
        echo "Failed to locate cluster file at $FDB_CLUSTER_FILE" 1>&2
        exit 1
    fi
}


function setup_public_ip() {
	if [[ "$FDB_NETWORKING_MODE" == "host" ]]; then
		public_ip=127.0.0.1
	elif [[ "$FDB_NETWORKING_MODE" == "container" ]]; then
		public_ip=$(grep `hostname` /etc/hosts | sed -e "s/\s *`hostname`.*//")
	else
		echo "Unknown FDB Networking mode \"$FDB_NETWORKING_MODE\"" 1>&2
		exit 1
	fi

	PUBLIC_IP=$public_ip
}

setup_public_ip
setup_cluster_file

echo "Starting FDB Document Layer on $PUBLIC_IP:$FDB_DOC_PORT"
fdbdoc --listen_address $PUBLIC_IP:$FDB_DOC_PORT --logdir /var/fdb/logs
