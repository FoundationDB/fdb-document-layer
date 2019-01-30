#
# collStats_tests.py
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

from pymongo.errors import OperationFailure

import random
import itertools
import pprint
import sys
import util
from util import MongoModelException


def _generate_unique_int(seen):
    tmp = random.randint(0, sys.maxint)
    while tmp in seen:
        tmp = random.randint(0, sys.maxint)
    seen.add(tmp)
    return tmp



def collStats_test(test_name, collection, operation, expected_return, query=None):
    def transform(elm):
        if isinstance(elm, list):
            return tuple(elm)
        else:
            return elm

    # clean up the collection
    collection.delete_many({})
    collection.drop_indexes()

    # run operation
    operation(collection)

    # get count
    actual_return = collection.database.command('collstats', collection.name)['count']

    if actual_return == expected_return:
        print "{} is OK".format(test_name)
        return True
    else:
        print "{} failed. Expected: {}; Actual: {}".format(test_name, expected_return, actual_return)
        return False


def test_insert_only():
    number_of_records = random.randint(1, 100)

    def operation(collection):
        for _ in range(0, number_of_records):
            collection.insert({'a': 1})

    return ("[Insert only]", operation, number_of_records, None)

def test_insert_then_delete_one():
    number_of_records = random.randint(1, 100)

    def operation(collection):
        for i in range(0, number_of_records):
            collection.insert({'a': 1, 'b': i})
            i += 1
        collection.delete_one({'b':0})

    return ("[Insert then delete one]", operation, number_of_records-1, None)

def test_insert_then_delete_many():
    number_of_records = random.randint(1, 100)

    def operation(collection):
        for i in range(1, number_of_records+1):
            collection.insert({'a': 1, 'b': i%2})

        collection.delete_many({'b':0})

    return ("[Insert then delete many]", operation, number_of_records - number_of_records/2, None)


tests = [locals()[attr] for attr in dir() if attr.startswith('test_')]


def test(collection, t):
    (test_name, operation, expected_return, query) = t
    okay = collStats_test(test_name, collection, operation, expected_return, query)
    if okay:
        print util.alert('PASS', 'okgreen')
        return True
    print util.alert('FAIL', 'fail')
    return False


#### `test_all()` is needed by the testing framework


def test_all(collection1, collection2):
    print "Distinct tests only use first collection specified"
    okay = True
    for t in tests:
        okay = test(collection1, t()) and okay
    return okay
