ARG FDB_VERSION=6.0.15

FROM foundationdb/foundationdb:$FDB_VERSION
LABEL version=0.0.2

COPY local_fdb.bash /var/fdb/scripts/

CMD /var/fdb/scripts/local_fdb.bash
