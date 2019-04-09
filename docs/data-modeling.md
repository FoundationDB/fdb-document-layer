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
applicable to the Document Layer as well.

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
