---
Disclaimer: MongoDB is a registered trademark of MongoDB, Inc.
---

# Transactions

This guide describes the use of transactions in the Document Layer.
Transactions are one of the distinctive features of the FoundationDB
Key-Value Store, and the Document Layer transactions inherit many of the
capabilities and limitations of FoundationDB.

## What is a transaction?

Transactions are a tool for concurrency control allowing multiple
clients to read and write data with strong guarantees about how the
clients can affect each other. The Document Layer supports serializable,
interactive, multi-document transactions across a distributed cluster.

Informally, a transaction is a group of operations that are performed
together with certain properties that ensure their reliability and
logical independence from other operations.

Using Python with PyMongo, a simple transaction using the
`@transactional` decorator looks like this:

```python
@transactional
def example(db):
    cust = db.customers.find_one()
    item = db.items.find_one()
    db.orders.insert({u'name': cust[u'name'], u'item_id': item[u'item_id']})
    db.totals.update({'_id': 1}, {'$inc': {u'orders': 1}})
    
example(db)
```

Once a call to `example()` returns, it's as if all the database
operations inside it were performed on the database with no intervening
writes from outside the transaction. If `example()` fails to return (due
to a connection failure, an exception raised in the code, etc.) then
it's either as if all the database operations were successfully
performed (as before), or *none* of them were.

This behavior ensures the following properties, known collectively as
"ACID":

  - **Atomicity**: Either all of the writes in the transaction happen,
    or none of them happen.
  - **Consistency**: If each individual transaction maintains a database
    invariant (e.g., a relationship between documents in two
    collections), then the invariant is maintained even when multiple
    transactions are modifying the database concurrently.
  - **Isolation**: It is as if transactions executed one at a time
    (serializability).
  - **Durability**: Once a transaction succeeds, its writes will not be
    lost despite any failures or network partitions.

An additional important property, though technically not part of ACID,
is also guaranteed:

  - **Causality**: A transaction is guaranteed to see the effects of all
    other transactions committed before it begins.

FoundationDB implements these properties using multiversion concurrency
control (MVCC) for reads and optimistic concurrency for writes. As a
result, neither reads nor writes are blocked by other readers or
writers. Instead, conflicting transactions will fail at commit time and
will usually be retried by the client.

In particular, all reads in a transaction take place from a snapshot of
the database. From the perspective of the transaction, this snapshot is
not modified by the writes of other concurrent transactions. When the
transaction is ready to be committed, the Document Layer submits the transaction's
reads and writes to the FoundationDB cluster. The FoundationDB cluster checks
there are no conflicts with any previously committed transaction
(i.e., that no value read within the transaction has been modified by another
transaction since the transaction began). If there is a conflict, the
transaction is rejected, and an error is returned to the client.
Rejected transactions are usually retried by the client. Accepted
transactions are written to disk on multiple cluster nodes and then
reported as accepted to the client.

Note that read-only transactions are not submitted to the cluster and
therefore can never fail due to a conflict. This means that if a
transaction reads stale data (e.g., if another transaction concurrently
modifies a document that the read-only transaction reads), the user
is *not* notified and the transaction is not rejected.

## Differences from MongoDB®

Note that because the FoundationDB Key-Value Store requires that all
operations be conducted within the context of a transaction, all Document
Layer operations also must occur either in an explicit or implicit
transaction. If no explicit transaction is given, then the Document
Layer will create an implicit transaction for each operation. This
means that whereas there is additional overhead in MongoDB® in
using multi-document transactions, in the Document Layer, the overhead
of a single-document operation and a multi-document transaction
are essentially the same. This means that in many cases, because of
the latency required to get a [read version](https://apple.github.io/foundationdb/developer-guide.html#latency)
at the beginning of each Key-Value Store operation, it may be more
efficient to batch multiple operation into one Document Layer transaction
than to issue each operation separately. Doing so allows the Document
Layer to amortize the per-transaction overhead across each operation
within your transaction.

At an API level, the Document Layer adds transactions to the
MongoDB® v3.0.0 API by introducing transaction related commands.
It expects all operations for a given transaction to be issued
between a `beginTransaction` and a `commitTransaction` on a single
connection. This differs from the way transactions were added in
the MongoDB® v4.0.0 API where transaction state is tracked through
the use of a `session`. In particular, this approach does not
allow for multiple transactions to be multiplexed across the same
connection. It also requires that once a connection is opened that
there is strong affinity between the client and the Document Layer
server, which may not be the case in some network configurations.
For that reason, the current recommendation is that users should
only use explicit transactions if they can guarantee that a
client will execute all operations against the same server within
a single connection. For example, this is safe if the Document Layer
server is deployed as a sidecar process with client code so that
each client communicates with exactly one Document Layer server.

In MongoDB® v4.0.0, transactions are available only for replica sets
and not for sharded configurations. These configuration parameters
are not used by the Document Layer, and transactions are available
under all configurations. This is because FoundationDB allows for
arbitrary transactions across its entire keyspace in all configurations.

Document Layer transactions inherit all limitations of
FoundationDB Key-Value Store transactions. Notably, this means that
all Document Layer transactions must complete within
[five seconds](https://apple.github.io/foundationdb/known-limitations.html#long-running-transactions).
Transactions are also limited to writing only 10 MB of data,
and users are encouraged to keep their transactions within 1 MB for
performance reasons. Note that this size includes both writing any
documents as well as writing or updating any indexes. In general,
the sizes of documents *read* within the transaction *do not count*
against the size limit, though boundary keys of any ranges read
*do* count. For more information, consult the FoundationDB Key-Value
Store documentation on
[large transactions](https://apple.github.io/foundationdb/known-limitations.html#large-transactions).


## Basic transaction commands

Transactions are exposed to the client via commands specific to the
Document Layer. These commands are used to write a [retry
loop](#retry-loops) for a given client. They are as follows:

- The `beginTransaction` command is used to initiate a transaction.
    Before returning, this command will wait for all prior writes to
    complete. All further database operations issued on that
    connection (prior to the next `rollbackTransaction` or
    `commitTransaction` command) will execute atomically, in isolation
    from other database operations, and with guaranteed durability.
    
    >    `beginTransaction` takes an optional `retry` flag. For a
    >     given transaction, you should set `retry=True` for all calls
    >     to `beginTransaction` after the initial one. You call
    >     `beginTransaction` more than once when a previous call to
    >     `commitTransaction` has resulted in an error. Setting
    >     `retry=True` will cause `beginTransaction` to check the
    >     error value to determine if it is appropriate to retry the
    >     transaction.

- The `rollbackTransaction` command is used to cancel an outstanding
     transaction on a connection without committing any of its changes.
     All writes since the last `beginTransaction` command are reverted.
 
- The `commitTransaction` command attempts to commit the outstanding
     transaction on that connection. This command will not return until
     either all of the modifications since the previous
     `beginTransaction` command have been durably logged to disk or the
     commit has failed.

MongoDB® clients do not have shortcuts for the above commands. They are
therefore normally run using
[runCommand](http://docs.mongodb.org/manual/reference/method/db.runCommand/)
for the `mongo` shell or its equivalent in your preferred client.

## Retry loops

The Document Layer employs optimistic concurrency and may therefore
reject a transaction, most often due to a conflict with another
transaction. As a result, transactions should normally be executed
within a *retry loop* to ensure a successful commit.

A retry loop executes the operations within a transaction and then
attempts to commit the transaction, catching any exception that may be
raised. In the event of a "retryable" exception (such as a conflict),
the loop will repeat until it successfully commits, or a non-retryable
exception (such as a timeout) is caught.


> Retry loops should be implement *once* for any given client and then
used as needed in application code.


For example, as illustrated below for PyMongo, the retry loop can be
conveniently implemented in Python as a decorator, which can then be easily
applied to any function containing database operations. Here's an implementation
of the `@transactional` decorator, ready to use with PyMongo:

```python
import inspect
import pymongo as pm
    
def transactional(func):
    index = inspect.getargspec(func).args.index("db")
    def func_wrapper(*args, **kwargs):
        db = args[index]
        committed = False
        db.command("beginTransaction")
        while not committed:
            try:
                ret = func(*args, **kwargs)
                db.command("commitTransaction")
                committed = True
            except pm.errors.OperationFailure as e:
                print e.details
                db.command("beginTransaction", retry=True)
        return ret
    return func_wrapper
```        

`@transactional` requires the function it decorates to have a formal
parameter `db` that takes the database on which to operate.

## Usage guidance

You should design your data model and queries to minimize conflicts,
allowing transactions to succeed even when the database is under load.

Transactions are not limited to basic CRUD operations. They may also
include metadata operations, such as adding or removing indexes, or
dropping collections. Index creation within a transaction will ignore
the `background` flag.

### Idempotence

In some cases, `commitTransaction` may throw an exception even though
the transaction has, in fact, succeeded in the database. This
circumstance is usually the result of a communication problem between
the client and the cluster. In this case, a standard [retry loop](#retry-loops),
such as the one given for PyMongo, will retry the transaction, potentially
executing it twice.

You should therefore consider the *idempotence* of your transactions. A
transaction is idempotent if it has the same effect when committed twice
as when committed once.

Some transactions are already idempotent, in which case there is nothing
more to worry about. If a transaction is not idempotent, you should
consider whether the differing result of a second execution is
acceptable. Most often, you will want to make the transaction idempotent
using the following techniques:

  - Avoid generating IDs within the retry loop. Instead, create them
    prior to the loop and pass them in. For example, if your transaction
    records an account deposit with a deposit ID, generate the deposit
    ID outside of the loop.
  - Within the retry loop, check for the completion of one of the
    transaction's unique side effects to determine if the whole
    transaction has previously completed. If the transaction doesn't
    naturally have such a side effect, you can create one by setting a
    unique field.

The following example illustrates both techniques. Together, they make a
transaction idempotent that otherwise would not have been:

```python
# Increase account balance and record the deposit with a unique deposit_id
@transactional
def deposit(db, acct_id, deposit_id, amount):
    
    # If the record exists, then the deposit already succeeded and we quit
    account = db.accounts.find_one({u'_id': acct_id})
    if u'deposit_id' in account and account[u'deposit_id'] == deposit_id:
        return
    
    # The previous check ensures that the update is executed only once
    db.accounts.update({u'_id': acct_id},
                       {'$set': {u'deposit_id': deposit_id},
                        '$inc': {u'balance': amount}})
```

## Configuration guidance

Transactions are tracked on a per-connection basis. Therefore, care must
be taken when using a MongoDB® client or driver that automatically
round-robins operations across a connection pool. To ensure correct
behavior, the safest approach is to deactivate such features and manage
connections manually.

Transactions can take retry limits and timeouts. These are specified via
parameters in the [configuration file](configuration.md#fdbdoc-section).
In particular:

  - `implicit-transaction-max-retries` sets the maximum number of times
    that transactions will be retried. The default value is 3. If set to -1,
    will disable the retry limit.
  - `implicit-transaction-timeout` sets a timeout in milliseconds for
    transactions. The default value is 7000. If set to 0, it will disable all
    timeouts.
