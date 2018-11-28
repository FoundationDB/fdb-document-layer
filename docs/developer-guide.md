---
Disclaimer: MongoDB is a registered trademark of MongoDB, Inc.
---

# Developer Guide

This guide describes the use of distinctive features of the Document
Layer. Except where noted here or in [Known Differences section](known-differences.md), all features
of officially supported MongoDB® clients work with the Document Layer as
well.

## Query planning

The Document Layer strives to have the decisions made by its query
planner be as transparent and as easy to understand for developers as
possible. In particular, we have provided ways for developers to hint to
the Document Layer's query planner which index should be used to speed
up a query if more than one is available.

In the case of simple indexes, the first available indexed field in a
conjunction will always be used for the index scan, and the remaining
fields will be transformed into a filter.

For example, if indexes exist on the fields `b` and `c`, the following
query:

```
{ $and: [ {a: 1}, {b: 1}, {c: 1} ] }
```

will be transformed into an index lookup on `{b: 1}`, followed by a
filter on the remaining terms (in this case `{$and: [ {a: 1}, {c: 1} ]}`
). If a developer had prior knowledge that cardinality of c is higher than
cardinality of b then the above query should be rewritten as:

```
{ $and: [ {a: 1}, {c: 1}, {b: 1} ] }
```

In the case of compound indexes, the planner will greedily choose terms
from the beginning of a conjunction that match prefixes of available
indexes and then try to extend the selected index to a compound index if
available.

For example, if two compound indexes exist, one on the fields (`b`,
`c`), and one on the fields (`a`, `b`, `c`), then the query:

```
{ '$and':[{b:1}, {a:1}, {c:1}] }
```

will use the shorter compound index. While this might seem undesirable,
our primary concern is to give developers complete control over the
planner's behavior. In future releases, we will continue adding features
to the planner and optimizer, while always striving to preserve
transparency into and control over its operations.

For now, we don't honor `$hint` provided with the query and silently ignore it.
Hints can only be provided through the order of predicates. We are planning on
changing it very soon.

## Latency and concurrency

Like the FoundationDB Key-Value Store, the Document Layer achieves its
maximum performance with a highly concurrent workload. Multiple client
processes are one way of achieving this, but in many situations, it is
advantageous for each individual client to open multiple connections to
the Document Layer and round-robin requests across them. Clients using
MongoDB® drivers that automatically instantiate connection pools may
need to manually increase pool size to saturate the Document Layer and
Key-Value Store server processes.

In order to minimize the effects of read latencies, we recommend the use
of a MongoDB® driver with support for the asynchronous operation.
Whenever possible, reads should be issued in parallel rather than
sequentially.

## Write concern

MongoDB® has introduced new kind of
[writes](https://docs.mongodb.com/manual/release-notes/2.6-compatibility/#write-method-acknowledgements)
in 2.6.0 which has become the default way of doing writes with all the
latest drivers. These writes are acknowledged from the server making
`getLastError` obsolete. Client can specify [write
concern](https://docs.mongodb.com/manual/reference/write-concern/) to
describe level acknowledgment needed. Document layer supports the new
style of writes. It provides strongest write guarantees irrespective of
write concern requested by the client.

## Read concern and Read preference

Document Layer exposes itself as a standalone MongoDB® server. And the
write acknowledgement is sent back to client only after it is pesisted on all
FoundationDB replicas. So, read concern and read preference are irrelavant and
reads are always returned with fully consistent results.

## Index builds

The Document Layer can perform index builds in either the background or
the foreground. Unlike in MongoDB®, there is no downside to either of
these options -- backgrounded builds are no less space efficient than
otherwise, and foregrounded builds do not lock any part of the database.
We recommend foregrounded builds as a best practice since that will
result in the connection returning a success message or an error when
the build is complete.

Backgrounded index builds are supported as a compatibility option, and
the current status of a backgrounded build can be determined by
performing a read on the `system.indexes` virtual collection. The
Document Layer has added several fields to the index metadata --
including `status` (which should be one of `building`, `ready`, or
`error`). Indexes in the `building` state will also have a `currently
processing document` field, which will contain the `_id` field of the
document which was most recently added to the index. This field is
updated approximately every 100 milliseconds.

If the Document Layer instance which was performing an index build is
killed or otherwise unable to communicate with the cluster, it is
possible for the index metadata to show the index in the `building`
state when in fact the index build will never complete. An easy way to
diagnose this condition is the `currently processing document` field --
if its value does not change for a long time, this is evidence that the
build was being performed by a now-dead or uncommunicative instance. At
this time, user intervention is required in the form of dropping the
half-built index and attempting the index construction again. A future
version of the Document Layer will provide fully parallelized and
fault-tolerant index builds.

While under normal conditions the `currently processing document` field
is updated every 100 milliseconds, it is possible for it to take longer
if the database is under very high load or if the index build is
experiencing significant contention (for instance, a massively parallel,
write-heavy workload on the collection that you are attempting to
index). Do not assume that a "hung build" as described above has
occurred unless you also have evidence of a process or communications
failure.


## The findAndModify command

MongoDB® offers a specialized command that scans a collection for
documents matching a specified criterion, performs an update on the
first such document encountered and returns a projection of either the
pre- or the post-modification document to the user.

The `findAndModify` command is supported by the Document Layer. However,
in the present release, we have disabled use of sorting with the
`findAndModify` command.
