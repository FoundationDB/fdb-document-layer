#
# unique_index_tests.py
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
import pymongo
import sys
import util
from util import MongoModelException


def _wrap(func, args, kwargs, expected_failure, msg_on_error):
    try:
        func(*args, **kwargs)
        if expected_failure:
            print msg_on_error
            return False
        else:
            return True
    except Exception as e:
        if not expected_failure or "Duplicated value not allowed by unique index" not in str(e):
            print msg_on_error
            return False
        else:
            return True


def test_insert_single_field_unique_index(collection):
    test_name = "test_insert_single_field_unique_index"
    random_key = "key-{}".format(random.randint(0, sys.maxint))
    random_key_2 = "{}-2".format(random_key)
    collection.create_index([(random_key, pymongo.ASCENDING)], unique=True)
    if not _wrap(collection.insert, ({
            random_key: 1
    }, ), {}, False, "{} non-duplicated non-null failed".format(test_name)):
        return False
    if not _wrap(collection.insert, ({random_key: 1}, ), {}, True, "{} duplicated non-null failed".format(test_name)):
        return False

    if not _wrap(collection.insert, ({
            random_key_2: 1
    }, ), {}, False, "{} non-duplicated null failed".format(test_name)):
        return False
    if not _wrap(collection.insert, ({random_key_2: 2}, ), {}, True, "{} duplicated null failed".format(test_name)):
        return False
    print "{} is OK".format(test_name)
    return True


def test_insert_compound_unique_index(collection):
    test_name = "test_insert_compound_unique_index"
    random_key = "key-{}".format(random.randint(0, sys.maxint))
    random_key_2 = "{}-2".format(random_key)
    random_key_3 = "{}-3".format(random_key)
    collection.create_index([(random_key, pymongo.ASCENDING), (random_key_2, pymongo.ASCENDING)], unique=True)
    if not _wrap(collection.insert, ({
            random_key: 1,
            random_key_2: 1
    }, ), {}, False, "{} non-duplicated non-null failed".format(test_name)):
        return False
    if not _wrap(collection.insert, ({
            random_key: 1,
            random_key_2: 1
    }, ), {}, True, "{} duplicated non-null failed".format(test_name)):
        return False
    if not _wrap(collection.insert, ({
            random_key_3: 1
    }, ), {}, False, "{} non-duplicated all-null failed".format(test_name)):
        return False
    if not _wrap(collection.insert, ({random_key_3: 2}, ), {}, True, "{} duplicated all-null failed".format(test_name)):
        return False
    if not _wrap(collection.insert, ({
            random_key: 1
    }, ), {}, False, "{} non-duplicated partial-null failed".format(test_name)):
        return False
    if not _wrap(collection.insert, ({
            random_key: 1,
            random_key_3: 4
    }, ), {}, True, "{} duplicated partial-null failed".format(test_name)):
        return False
    print "{} is OK".format(test_name)
    return True


def test_update_single_field_unique_index(collection):
    test_name = "test_update_single_field_unique_index"
    random_key = "key-{}".format(random.randint(0, sys.maxint))
    collection.create_index([(random_key, pymongo.ASCENDING)], unique=True)
    id1 = random.randint(0, sys.maxint)
    id2 = random.randint(0, sys.maxint)
    if not _wrap(collection.insert, ({
            random_key: 1,
            "_id": id1
    }, ), {}, False, "{} non-duplicated non-null insert failed".format(test_name)):
        return False
    if not _wrap(collection.insert, ({
            random_key: 2,
            "_id": id2
    }, ), {}, False, "{} non-duplicated non-null insert failed".format(test_name)):
        return False
    if not _wrap(collection.update_one, (
        {
            "_id": id2
        },
        {
            "$set": {
                random_key: 1
            }
        },
    ), {}, True, "{} duplicated non-null update failed".format(test_name)):
        return False
    print "{} is OK".format(test_name)
    return True


def test_update_compound_unique_index(collection):
    test_name = "test_update_compound_unique_index"
    random_key = "key-{}".format(random.randint(0, sys.maxint))
    random_key_2 = "{}-2".format(random_key)
    if not _wrap(collection.create_index, ([(random_key, pymongo.ASCENDING), (random_key_2, pymongo.ASCENDING)], ),
                 {'unique': True}, False, "{} non-duplicated non-null index creation failed".format(test_name)):
        return False
    id1 = random.randint(0, sys.maxint)
    id2 = random.randint(0, sys.maxint)
    if not _wrap(collection.insert, ({
            random_key: 1,
            random_key_2: 1,
            "_id": id1
    }, ), {}, False, "{} non-duplicated non-null insert failed".format(test_name)):
        return False
    if not _wrap(collection.insert, ({
            random_key: 2,
            random_key_2: 1,
            "_id": id2
    }, ), {}, False, "{} non-duplicated non-null insert failed".format(test_name)):
        return False
    if not _wrap(collection.update_one, (
        {
            "_id": id2
        },
        {
            "$set": {
                random_key: 1
            }
        },
    ), {}, True, "{} duplicated non-null update failed".format(test_name)):
        return False
    print "{} is OK".format(test_name)
    return True


def test_update_single_field_unique_index_with_same_value(collection):
    test_name = "test_update_single_field_unique_index_with_same_value"
    random_key = "key-{}".format(random.randint(0, sys.maxint))
    collection.create_index([(random_key, pymongo.ASCENDING)], unique=True)
    id1 = random.randint(0, sys.maxint)
    id2 = random.randint(0, sys.maxint)
    if not _wrap(collection.insert, ({
            random_key: 1,
            "_id": id1
    }, ), {}, False, "{} non-duplicated non-null insert failed".format(test_name)):
        return False
    if not _wrap(collection.insert, ({
            random_key: 2,
            "_id": id2
    }, ), {}, False, "{} non-duplicated non-null insert failed".format(test_name)):
        return False
    if not _wrap(collection.update_one, (
        {
            "_id": id2
        },
        {
            "$set": {
                random_key: 2
            }
        },
    ), {}, False, "{} same value update failed".format(test_name)):
        return False
    print "{} is OK".format(test_name)
    return True


def test_update_compound_unique_index_with_same_value(collection):
    test_name = "test_update_compound_unique_index_with_same_value"
    random_key = "key-{}".format(random.randint(0, sys.maxint))
    random_key_2 = "{}-2".format(random_key)
    if not _wrap(collection.create_index, ([(random_key, pymongo.ASCENDING), (random_key_2, pymongo.ASCENDING)], ),
                 {'unique': True}, False, "{} non-duplicated non-null index creation failed".format(test_name)):
        return False
    id1 = random.randint(0, sys.maxint)
    id2 = random.randint(0, sys.maxint)
    if not _wrap(collection.insert, ({
            random_key: 1,
            random_key_2: 1,
            "_id": id1
    }, ), {}, False, "{} non-duplicated non-null insert failed".format(test_name)):
        return False
    if not _wrap(collection.insert, ({
            random_key: 2,
            random_key_2: 1,
            "_id": id2
    }, ), {}, False, "{} non-duplicated non-null insert failed".format(test_name)):
        return False
    if not _wrap(collection.update_one, (
        {
            "_id": id2
        },
        {
            "$set": {
                random_key: 2
            }
        },
    ), {}, False, "{} same value update failed".format(test_name)):
        return False
    print "{} is OK".format(test_name)
    return True


def test_create_single_field_unique_index(collection):
    test_name = "test_create_single_field_unique_index"
    random_key = "key-{}".format(random.randint(0, sys.maxint))
    random_key_2 = "{}-2".format(random_key)
    if not _wrap(collection.insert, ({
            random_key: 1
    }, ), {}, False, "{} duplicated non-null pre-index insert failed".format(test_name)):
        return False
    if not _wrap(collection.insert, ({
            random_key: 1
    }, ), {}, False, "{} duplicated non-null pre-index insert failed".format(test_name)):
        return False
    if not _wrap(collection.create_index, ([(random_key, pymongo.ASCENDING)], ), {'unique': True}, True,
                 "{} duplicated non-null index creation failed".format(test_name)):
        return False
    if not _wrap(collection.create_index, ([(random_key_2, pymongo.ASCENDING)], ), {'unique': True}, True,
                 "{} duplicated null index creation failed".format(test_name)):
        return False
    print "{} is OK".format(test_name)
    return True


def test_create_compound_unique_index(collection):
    test_name = 'test_create_compound_unique_index'
    random_key = "key-{}".format(random.randint(0, sys.maxint))
    random_key_2 = "{}-2".format(random_key)
    if not _wrap(collection.insert, ({
            random_key: 1,
            random_key_2: 1
    }, ), {}, False, "{} duplicated non-null pre-index insert failed".format(test_name)):
        return False
    if not _wrap(collection.insert, ({
            random_key: 1,
            random_key_2: 1
    }, ), {}, False, "{} duplicated non-null pre-index insert failed".format(test_name)):
        return False
    if not _wrap(collection.create_index, ([(random_key, pymongo.ASCENDING), (random_key_2, pymongo.ASCENDING)], ),
                 {'unique': True}, True, "{} duplicated non-null index creation failed".format(test_name)):
        return False
    print "{} is OK".format(test_name)
    return True


def test_unique_index_backgroud_build_request(collection):
    test_name = 'test_unique_index_backgroud_build_request'
    random_key = "key-{}".format(random.randint(0, sys.maxint))
    try:
        collection.create_index([(random_key, pymongo.ASCENDING)], unique=True, background=True)
        print "{} did not get the expected error".format(test_name)
        return False
    except Exception as e:
        if "tried to create unique indexes in background" not in str(e):
            print "{} did not get the expected error".format(test_name)
            return False
    print "{} is OK".format(test_name)
    return True


tests = [locals()[attr] for attr in dir() if attr.startswith("test_")]

#### `test_all()` is needed by the testing framework


def test_all(collection1, collection2):
    print "Unique index tests only use first collection specified"
    okay = True
    # since unique index applies to the whole collection, and it counts null values, we need a brand new collection
    # to run tests.
    client = collection1.database.client
    tmp_db = client["unique_index_tests_tmp_db"]
    tmp_collection = None
    for t in tests:
        # clean up the collection before every test
        if tmp_collection is None:
            tmp_collection = tmp_db["unique_index_tests_tmp_collection"]
        else:
            tmp_collection.drop()
            tmp_collection = tmp_db["unique_index_tests_tmp_collection"]
        okay = t(tmp_collection) and okay
        if okay:
            print util.alert("PASS", "okgreen")
        else:
            print util.alert("FAIL", "fail")
    tmp_collection.drop()
    client.drop_database("unique_index_tests_tmp_db")
    return okay
