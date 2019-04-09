---
Disclaimer: MongoDB is a registered trademark of MongoDB, Inc.
---

# FoundationDB Document Layer

Welcome to the documentation for the FoundationDB Document Layer.

FoundationDB Document Layer is a stateless microserver that exposes a document-oriented database API. The Document Layer speaks the MongoDB® wire protocol, allowing the use of the MongoDB® API via existing MongoDB® client bindings. All persistent data are stored in the FoundationDB Key-Value Store.

The Document Layer implements a subset of the MongoDB® API (v 3.0.0) with some [differences](known-differences.md). This subset is mainly focused on CRUD operations and indexes. The Document Layer works with all the latest official MongoDB® drivers.

NOTE: [mongo-go-driver](https://github.com/mongodb/mongo-go-driver) assumes server is atleast 3.2. If you use it against Document Layer it fails on `find` commands. This should be fixed with [#11](https://github.com/FoundationDB/fdb-document-layer/issues/11).

As the Document Layer is built on top of FoundationDB, it inherits the strong guarantees of FoundationDB. Causal
consistency and strong consistency are the default mode of operation.
Indexes are always consistent with the inserts. Shard keys are not
needed as data distribution is taken care by FoundationDB backend
automatically.

You'll find the information here about the architecture of the Document
Layer and how to develop applications on top of it.

* [Getting started on macOS](getting-started-mac.md)
* [Getting started on Linux](getting-started-linux.md)
* [Architecture](architecture.md)
* [Developer Guide](developer-guide.md)
* [Data Modeling](data-modeling.md)
* [Known Differences](known-differences.md)
* [Configuration](configuration.md)
* [Administration](administration.md)
