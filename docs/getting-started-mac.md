---
Disclaimer: MongoDB is a registered trademark of MongoDB, Inc.
---

# Getting Started on a macOS

## First steps

* Validate that your system has
    * x86-64 processor architecture
    * 4 GB RAM (per Document Layer instance)
    * macOS 10.7 or newer
* Download the FoundationDB and the Document Layer packages for your system from [Downloads](https://www.foundationdb.org/download/).


## Setting up the Key-Value Store

The Document Layer requires the FoundationDB Key-Value Store to store
its data, so you'll first need to [set up the FoundationDB Key-Value
Store](https://apple.github.io/foundationdb/getting-started-mac.html).
For both development and production use, the Document Layer must be
installed on a system running the FoundationDB client library (version
6.0 or greater) and containing [the cluster
file](https://apple.github.io/foundationdb/administration.html#cluster-files)
of a running FoundationDB system.

## Reserved keyspace for the Document Layer

The Document Layer stores all of its data in a directory managed by the
[Directory Layer](https://apple.github.io/foundationdb/developer-guide.html#directories).
This means that the Document Layer will not conflict with any other
application or layer using Key-Value Store
[tuples](https://apple.github.io/foundationdb/data-modeling.html#tuples),
[subspaces](https://apple.github.io/foundationdb/developer-guide.html#subspaces),
or
[directories](https://apple.github.io/foundationdb/developer-guide.html#directories)
in their default configurations. The name of the top-level directory
used by the Document Layer can be specified in the [configuration file
](configuration.md) and defaults to `document`. This allows multiple Document Layer
deployments to co-exist on a single FoundationDB cluster.

## Installing the Document Layer

To begin installation, double-click on
FoundationDB-Document-Layer-1.6.0.pkg. Follow the instructions in the
installer.

### Cluster file specification

The FoundationDB Document Layer package installs a [configuration file](configuration.md) 
that specifies the default [cluster
file](https://apple.github.io/foundationdb/administration.html#cluster-files)
to be located at `/usr/local/etc/foundationdb/fdb.cluster`. After installation, you
can update this location if needed.

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

As Document Layer is compatible with the MongoDB® API, it can be used with the
`mongodump`,
`mongorestore`, `mongoimport`, and `mongoexport` tools distributed with
MongoDB®.

## Client support

The Document Layer has been tested with all of the officially supported
drivers provided by MongoDB®. It is expected to work with any MongoDB®
client or driver, although minor tweaking may be required in some cases
to use additional features.
