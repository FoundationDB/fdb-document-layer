---
Disclaimer: MongoDB is a registered trademark of MongoDB, Inc.
---

# Data Modeling with Transactions

This guide describes data modeling techniques that take advantage of
transactions in the Document Layer. For an introductory overview of
transactions, see [transactions](transactions.md).

## Background

The Document Layer implements a subset of the MongoDB® API (v 3.0.0)
with a few [known differences](known-differences.md). As a result,
most of the data modeling techniques used with MongoDB® are
applicapble to the Document Layer as well. However, the Document
Layer adds transactions in a way that differs from the way transactions
were added to the MongoDB® v 4.0 API, which means that client code
written to use multi-document transactions may need to be modified
to work with the Document Layer.

Transactions are a tool for concurrency control that allows multiple
clients to read and write data with strong guarantees about how the
clients can affect each other. Transactions provide new capabilities for
data modeling because they can operate on multiple documents with full
[ACID](https://en.wikipedia.org/wiki/ACID_(computer_science)) guarantees.

Note that regardless of whether or not the user specifies an explicit
transaction, all Document Layer operations must occur within the context
of some transaction of the underlying FoundationDB Key-Value Store. If
the user does not supply one explicitly, then the Document Layer will
create one transaction per operation. This means that there is very
little overhead for using a multi-document transaction compared to a
single-document operation. As a result, the Document Layer is very
amenable to data models that require such transactions.

## Embedding data

Data modeling is about selecting a representation for the entities and
relationships relevant to an application. In relational databases,
schemas are usually [normalized](https://en.wikipedia.org/wiki/Database_normalization),
so that each entity is stored in its own table (with a primary key),
and relationships between entities are represented by references (foreign keys).
Normalization seeks to eliminate any redundant storage of data. However,
relational queries often involve data from more than one table and so must
employ joins.

In contrast to the "flat" structure of relational tables, JSON-like
documents have a recursive structure that makes them amenable to
[denormalized](https://en.wikipedia.org/wiki/Denormalization) models.
In the context of documents, denormalization just means that data for one
entity is stored within the document of another. The simplest form of
denormalization is *embedding*, in which we use no references at all and
simply embed a "child" entity as subdocument within its "parent".

For example, suppose you have a very simple task tracking system in
which each person in a company is assigned a number of tasks, but no two
people share tasks. You might embed the tasks within the person documents
with something like:

```python
    {
        '_id': ObjectId("AK54OK89RE87"),
        'name': 'Susan Johnson',
        'handle': '@sjohnson',
        'email': 'susan.johnson@example.com',
        'tasks': [
            {
                'title': "New performance tests",
                'due_date':  datetime.datetime(2015, 2, 10, 0, 0, 0, 0),
            },
            {
                'title': "Update website",
                'due_date':  datetime.datetime(2015, 2, 10, 0, 0, 0, 0),
            }
        ]
    }
```

Embedding is a fairly flexible technique. Multiple categories of child
entities can be embedded (e.g., persons may have equipment assigned to
them in addition to tasks), and children can have embedded entities of
their own. This approach works well as long the children are accessed
only via the parent and the number of children remains small.

## Beyond embedding

Of course, it's common to encounter cases in which the children must be
accessible on their own or the number of children grows large. In these
cases, embedding ceases to be an effective approach.

For example, in a task tracking system, you might want to find all tasks
that match a date-based criterion regardless of the person they are
assigned to. Having the tasks embedded within the person documents is
not ideal for such queries. More fundamentally, tasks are often assigned
to more than one person or to no one.

The implication is that, rather than a *one-to-many* relationship, you
may need to model persons and task as having a *many-to-many*
relationship. You can model this richer relationship by replacing the
embedded data with references to independent documents. In this case, by
using a more normalized schema, your schema might actually approach
something more like the schema employed in a relational database. For
example, you might model persons with documents like:

```python
    {
        '_id': ObjectId("AK54OK89RE87"),
        'name': 'Susan Johnson',
        'handle': '@sjohnson',
        'email': 'susan.johnson@example.com',
        'tasks': [
            ObjectId("AU89TE42IK84"), 
            ObjectId("AN85UN94ED36"),
            ObjectId("AP05YB64RF85") 
        ]
    }
```

and tasks with documents like:

```python
    {
        '_id': ObjectId("AU89TE42IK84"), 
        'title': "New performance tests",
        'due_date':  datetime.datetime(2015, 2, 10, 0, 0, 0, 0),
        'members': [
            ObjectId("AK54OK89RE87"),
            ObjectId("AO72TB96HN85"),
            ObjectId("AO93LU45NU95")
        ]
    }
```

This model has the advantage that tasks can be accessed independently of
persons. You can therefore access persons via tasks, as well as tasks
via persons, whereas the embedding model only facilitated the latter.
For example, you can can easily find the members (i.e., persons assigned
to) a given task as follows:

```python
    from bson.objectid import ObjectId
    
    def find_members(db, task_id):
        return db.tasks.find_one({'_id': ObjectId(task_id)}, {'members': 1})
```

## Transactions for multi-document operations

The `find_members` function is a simple example that reads from only a
*single* document. In order to change a task assignment, you'll need to
update *two* documents, one for the person and one for the task. The
Document Layer's transactions make it easy to perform multi-document
operations in an atomic and isolated manner.

Here are a few examples using the [@transactional](transactions.md#retry-loops)
decorator with PyMongo. You can add a new member to a task as follows:

```python
    @transactional
    def add_member(db, person_id, task_id):
        db.persons.update({u'_id': ObjectId(person_id)},
                          {'$addToSet': {u'tasks': ObjectId(task_id)}})
        db.tasks.update({u'_id': ObjectId(task_id)},
                        {'$addToSet': {u'members': ObjectId(person_id)}}) 
```

The `@transactional` decorator implements a retry loop, which may, on
occasion, execute the transaction more than once. You should therefore
check that this transactional function is [idempotent](transactions.md#idempotence),
producing the same result if executed multiple times as if executed once.

In this case, the idempotence of `add_member()` is guaranteed by the
`$addToSet` operator, which never adds more than a single copy of an
element to an array. It would therefore be a bug to use the `$push`
operator in this function instead of `$addToSet`, as `$push` may add
redundant copies of an element.

Removing a member from a task is similar:

```python
    @transactional
    def remove_member(db, person_id, task_id):
        db.persons.update({u'_id': ObjectId(person_id)},
                          {'$pullAll': {u'tasks': [ObjectId(task_id)]}})
        db.tasks.update({u'_id': ObjectId(task_id)},
                        {'$pullAll': {u'members': [ObjectId(person_id)]}})
```

Here, idempotence is guaranteed because an element can only be removed
from an array once. `$pullAll` will simply have no effect on a
subsequent execution.

Finally, you can wholesale reassign a task by removing and adding lists
of persons, all in a single transaction:

```python
    @transactional
    def reassign_task(db, task_id, old_person_ids, new_person_ids):
    
        # get lists of the object ids
        remove_objects = map(ObjectId, old_person_ids)
        add_objects = map(ObjectId, new_person_ids)
    
        # remove task from old persons
        db.persons.update({u'_id': {'$in': remove_objects}},
                          {'$pullAll': {u'tasks': [ObjectId(task_id)]}},
                          multi=True)
    
        # add task to new persons
        db.persons.update({u'_id': {'$in': add_objects}},
                          {'$addToSet': {u'tasks': ObjectId(task_id)}},
                          multi=True)
    
        # remove old persons from task
        db.tasks.update({u'_id': ObjectId(task_id)},
                        {'$pullAll': {u'members': remove_objects}})
    
        # add new persons to task
        db.tasks.update({u'_id': ObjectId(task_id)},
                        {'$addToSet': {u'members': add_objects}})
```

This transactional function is idempotent for the same reasons as
`add_member()` and `remove_member()`.

In summary, all data modeling techniques available in MongoDB® remain
available in the Document Layer. However, transactions provide a new
capability by coordinating operations across multiple documents with
guarantees of atomicity and isolation. This capability makes the use of
references between documents much safer.
