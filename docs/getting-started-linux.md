---
Disclaimer: MongoDB is a registered trademark of MongoDB, Inc.
---

# Getting Started on Linux

## First steps

* Validate system requirements
    * 64-bit Linux operating system that works with .deb or .rpm packages.
    * 4GB RAM (per Document Layer instance)
* Download the FoundationDB and the Document Layer packages for your system from [Downloads](https://www.foundationdb.org/download).

## Setting up the Key-Value Store

The Document Layer requires the FoundationDB Key-Value Store to store
its data, so you'll first need to [set up the FoundationDB Key-Value
Store](https://apple.github.io/foundationdb/getting-started-linux.html).
For both development and production use, the Document Layer must be
installed on a system running the FoundationDB client library (version
6.0 or greater) and containing the [cluster
file](https://apple.github.io/foundationdb/administration.html#foundationdb-cluster-file)
of a running FoundationDB system. Once the Document Layer has been
installed, permissions on the cluster file must be appropriately set.

## Reserved keyspace for the Document Layer

The Document Layer stores all of its data in a directory managed by the
[Directory
Layer](https://apple.github.io/foundationdb/developer-guide.html#directories).
This means that the Document Layer will not conflict with any other
appilcation or layer using Key-Value Store
[tuples](https://apple.github.io/foundationdb/data-modeling.html#tuples),
[subspaces](https://apple.github.io/foundationdb/developer-guide.html#subspaces),
or
[directories](https://apple.github.io/foundationdb/developer-guide.html#directories)
in their default configurations. The name of the top-level directory
used by the Document Layer can be specified in the [configuration file](configuration.md)
and defaults to `document`. This allows multiple Document Layer
deployments to co-exist on a single FoundationDB cluster.

## Installing the Document Layer

To install on **Ubuntu** use the dpkg command:

```
$ sudo dpkg -i fdb-document-layer_1.5.1-1_amd64.deb
```

To install on **RHEL/CentOS 6** use the rpm command:

```
$ sudo rpm -Uvh fdb-document-layer-1.5.1-1.el6.x86_64.rpm
```

And for **RHEL/CentOS 7** be sure to use the version 7 specific package:

```
$ sudo rpm -Uvh fdb-document-layer-1.5.1-1.el7.x86_64.rpm
```

By default, the Document Layer uses the loopback IP (127.0.0.1). In this
configuration, all parts of FoundationDB, including client applications,
must run on the same machine and communicate via 127.0.0.1, not via
external IPs.

### Cluster file specification and permissions

The Document Layer packages install a [configuration file](configuration.md)
that specifies the `foundationdb` user and group for
the `fdbdocmonitor` process. If these system users are not already
present, the installation will create them. The configuration file also
specifies the default [cluster
file](https://apple.github.io/foundationdb/administration.html#cluster-files)
to be located at `/etc/foundationdb/fdb.cluster`. After installation, you can update this location if needed.

The cluster file must be writable by the user running the Document Layer
process, as specified in the configuration file. This is because the
cluster file is automatically updated when a coordination change occurs.
For a default configuration, cluster file owner and group should be set
to `foundationdb` with mode `0664`.

## Connecting to the Document Layer

You can test that the Document Layer is working by connecting to it with
the `mongo` CLI distributed with MongoDB® (e.g.
`mongo 127.0.0.1:27016`). The default port on which the Document Layer
listens is `27016`, but the port can be changed in the [configuration
file](configuration.md) if needed. Be sure to insert and read back a small
document to ensure that the Document Layer is successfully communicating
with the Key-Value Store.

When you have successfully connected to the Document Layer from the `mongo` CLI, you will see a warning saying "This is not MongoDB®." This message confirms that you have connected to the Document Layer on the correct port and not another MongoDB® database.

## Importing data from MongoDB®

As Document Layer is compatible with the MongoDB® API, it can be used with the `mongodump`,
`mongorestore`, `mongoimport`, and `mongoexport` tools distributed with
MongoDB®.

## Client support

The Document Layer has been tested with all of the officially supported
drivers provided by MongoDB®. It is expected to work with any MongoDB®
client or driver, although minor tweaking may be required in some cases
to use additional features.
