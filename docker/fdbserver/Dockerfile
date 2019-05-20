ARG FDB_VERSION=6.1.7

FROM foundationdb/foundationdb:$FDB_VERSION
LABEL version=0.0.3

COPY local_fdb.bash /var/fdb/scripts/

CMD /var/fdb/scripts/local_fdb.bash
