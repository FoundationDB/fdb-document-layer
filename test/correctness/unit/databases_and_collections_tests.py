#
# databases_and_collections_tests.py
#
# This source file is part of the FoundationDB open source project
#
# Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# MongoDB is a registered trademark of MongoDB, Inc.
#

import pprint

import pymongo

import util
from mongo_model import MongoCollection

pprint = pprint.PrettyPrinter().pformat


def error_report(msg, names1, names2):
    print util.alert("%s" % msg, "fail")
    print util.indent(util.alert("Collection1: %s" % names1, "fail"))
    print util.indent(util.alert("Collection2: %s" % names2, "fail"))


# def test_1(coll1):
#     passed = True
#
#     # Blank slate
#     conn = coll1.database.client
#     databases = conn.list_database_names()
#
#     for d in databases:
#         conn.drop_database(d)
#
#     conn['db1']['coll1'].insert({'a': 1})
#     conn['db1']['coll2'].insert({'a': 1})
#     conn['db1']['colll'].insert({'a': 1})
#     conn['db1']['a'].insert({'a': 1})
#
#     conn['db2']['coll2'].insert({'a': 1})
#     conn['db2']['coll3'].insert({'a': 1})
#     conn['db2']['coll1'].insert({'a': 1})
#
#     conn['db2a']['coll1'].insert({'a': 1, 'b': 2})
#
#     databases = conn.list_database_names()
#
#     if not sorted(databases) == ['db1', 'db2', 'db2a']:
#         passed = False
#         error_report("Output of show databases was incorrect", databases, ['db1', 'db2', 'db2a'])
#
#     collections = conn['db1'].collection_names()
#
#     if not sorted(collections) == ['a', 'coll1', 'coll2', 'colll', 'system.indexes']:
#         passed = False
#         error_report("Output of show collections was incorrect", collections,
#                      ['a', 'coll1', 'coll2', 'colll', 'system.indexes'])
#
#     collections = conn['db2'].collection_names()
#
#     if not sorted(collections) == ['coll1', 'coll2', 'coll3', 'system.indexes']:
#         passed = False
#         error_report("Output of show collections was incorrect", collections,
#                      ['coll1', 'coll2', 'coll3', 'system.indexes'])
#
#     return passed

def test_simple_coll_index(collection):
    # Create an index
    collection.create_index('a', name='idx_a')

    # Insert bunch of documents
    docs = []
    for i in range(1, 50):
        docs.append({'a': i, 'b': str(i)})
    collection.insert_many(docs)

    # Create another index
    collection.create_index('b')

    # Query and check results
    if collection.find({'a': {'$gt': 10, '$lt': 21}}).count() != 10:
        print "Expected records 10 not received"
        return False

    # Drop an index
    collection.drop_index('idx_a')

    # Query and check results
    if collection.find({'a': {'$gt': 10, '$lt': 21}}).count() != 10:
        print "Expected records 10 not received"
        return False

    # Drop all indexes
    collection.drop_indexes()

    # Query and check results
    if collection.find({'a': {'$gt': 10, '$lt': 21}}).count() != 10:
        print "Expected records 10 not received"
        return False

    return True


tests = [globals()[attr] for attr in dir() if attr.startswith('test_')]


def test_all(collection, _):
    okay = True
    for t in tests:
        okay = t(collection) and okay
    return okay
