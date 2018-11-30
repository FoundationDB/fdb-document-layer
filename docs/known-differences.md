---
Disclaimer: MongoDB is a registered trademark of MongoDB, Inc.
---

# Known Differences

The FoundationDB Document Layer differs from MongoDB® in several ways. We've
implemented a number of bug fixes, affordances for new behaviors, and
some new features.

We're prioritizing unimplemented features based on user
feedback. If you’re using a feature on this list, please let us know by
raising a GitHub issue.

## Improvements

#### No locking for conflicting writes

The Document Layer employs the optimistic concurrency of Key-Value
Store. As a result, write operations of any sort do not take locks
on the database. Instead, if two operations concurrently attempt to
modify the same field of the same document, one of them will fail,
and can be retried by the client. (Most operations will
automatically retry for a configurable number of times.)

#### Online index builds

Non-backgrounded index builds do not lock the database, and
backgrounded ones are fast and space efficient.

Online index builds in the Document Layer
perform the exact same operations whether backgrounded or not -- the
only difference made by the `background` parameter in an
`createIndex` operation is whether the command returns to the client
and unblocks the connection before the index is ready. In either
case, the ongoing index build does not block concurrent database
operations, including write operations and other metadata
operations, and the index is fully consistent as soon as it is ready
for use.

#### Irrelevant commands removed

Many database commands, including all commands concerning
sharding and replication, are not applicable to the Document
Layer and are therefore unimplemented.

#### $explain format

The format of the information returned by the `$explain` operation
is not final, and may change without warning in a future version. It
is also substantially different from the format of explanations
returned by MongoDB®.

#### Multikey compound indexes

Multikey compound indexes in the Document Layer permit the
document to have array values for more than one of the indexed
fields. Care should be taken with this feature, however, as
updates to such a document may generate a number of index
updates equal to the Cartesian product of the lengths of all of
the indexed arrays. Currently, the Document Layer will not
permit the insertion or updating of a document which generates
more than `1000` updates to a single index in this way. In a
future release, we expect this limit to become configurable.


## Unimplemented features

Document Layer's feature set is focused on CRUD operations, indexes and
transactions. Some of the important features missing are listed here.

#### Aggregation framework
The Document Layer does not implement the MongoDB® aggregation
pipeline and operators.

#### Sessions
MongoDB® has introduced sessions in v3.6. The Document Layer doesn't support
sessions yet. Sessions in MongoDB® enable better consistency guarantees.
It's important to note that, even though the Document Layer doesn't support
sessions, it has better consistency guarantees by default due to FoundationDB
backend.

#### Oplog (Change Streams)
The Document Layer does not perform async replication and so
does not need the Oplog. Future releases may emulate the Oplog
to aid migration of applications that directly examine it.

#### Tailable cursors and capped collections
The Document Layer does not support tailable cursors or capped
collections.

#### Geospatial queries
The Document Layer does not implement any geospatial query
operators.

#### Projection operators
While the Document Layer does support custom projections of
query results, it does not support any projection operators.
Only literal projection documents (inclusive or exclusive) may
be used.

#### Evaluation operators
The Document Layer does not support the `$text` or `$where`
query operators.

#### Sparse indexes
The Document Layer does not support indexes which only contain
entries for documents that have the indexed field.

#### Non-multikey indexes
All indexes in the Document Layer permit multiple entries for a
given document if the indexed field on that document contains an
array.

#### Mixed-order compound indexes
Compound indexes in the Document Layer must be either ascending
or descending on all of their fields.

#### Auth/auth
The Document Layer does not support MongoDB® authentication,
role-based access control, auditing, or transport encryption,
although transport-layer encryption is supported between
FoundationDB clients and the Key-Value Store.

We are working on adding TLS very soon.

#### Deprecated BSON types
The Document Layer does not support the deprecated BSON binary
format (binary subtype 2) or the MongoDB® internal timestamp type.

#### Database references
The Document Layer does not support `DBRefs`.

#### Positional operator
The Document Layer does not support the `'$'` positional
operator in updates or projections.

#### findAndModify command
The Document Layer has disabled the `sort` parameter to the
`findAndModify` command. For more information, see the 
[Developer Guide](developer-guide.md#the-findandmodify-command).

#### $push and $pull operators
The Document Layer does not support the `$position` modifier to
the `$push` operator. The `$sort` modifier to the `$push`
operator is only available if both the `$each` modifier and the
`$slice` modifier have been used. Finally, the `$pull` operator
only accepts literal values rather than general query objects.

#### Database commands
MongoDB® supports a wide range of [administrative
commands](http://docs.mongodb.org/manual/reference/command/#database-operations)
targeted at DBAs and DevOps personnel rather than developers.
Where appropriate, the Document Layer [supports these commands](administration.md#mongodb-administrative-commands) as well or will implement them in the
future.

## Behavioral differences

#### Numeric field names

Use of array-like numeric field names are not supported in non-array
BSON documents. The Document Layer does not support the use of array-like
numeric field names (e.g. "1") in non-array BSON documents.

#### listDatabases command

`listDatabases` will always return a size of 1000000 bytes for a
database that contains any data.

#### Ordering of fields

The Document Layer ignores the provided order of fields in a BSON
document.

The Document Layer ignores the provided order of fields in a
BSON document. BSON documents consisting of the same elements in
a different order are considered to be equivalent.

One side effect of this is that unlike in MongoDB®, you may not
have documents with compound `_id` fields with identical elements
in a different order, (e.g. attempting to insert both
`{_id:{foo: 1, bar: 1}}` and `{_id:{bar: 1, foo: 1}}` into a
collection will result in an error).


#### Nested $elemMatch predicates

A query document composed of two or more nested `$elemMatch`
predicates may behave differently.


A query document composed of two or more nested `$elemMatch`
predicates, e.g., `{'a': {'$elemMatch': {'$elemMatch':
{'$gt': 0}}}}`, may behave differently than it would if executed
in MongoDB®.

#### Slow query log

The Document Layer logs all operations that perform full collection
scans on non-system collections.

Rather than log all operations that take more than a certain
amount of time, the Document Layer logs all operations that
perform full collection scans on non-system collections. In this
way, we hope to warn developers about operations that may
become slow when more data is
added to a collection.

## Protocol differences

#### Exhaust Mode Queries

In Exhaust Mode, the server generates RequestID values such that
they avoid colliding with client-generated RequestID values.

As with MongoDB®, queries in Exhaust mode will be responded to
with 1 or more replies. All of the replies will contain a
different server-generated RequestID which increases by 1 or
more for each subsequent reply. The first reply will also
contain a ResponseTo value equal to the RequestID of the
client's original query, while subsequent replies will have
ResponseTo set to the generated RequestID of the previous reply.

The Document Layer will generate RequestIDs that are always
increasing within a session. However, they are not neccessarily
sequential and they are at least 1,000,000 request units ahead
of the highest RequestID seen from the client during the session
in which the ID is generated. This avoids collision between
Client and Server generated RequestIDs.
