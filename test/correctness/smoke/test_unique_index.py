#
# test_unique_index.py
#
# This source file is part of the FoundationDB open source project
#
# Copyright 2013-2019 Apple Inc. and the FoundationDB project authors
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

import random
import sys

import pymongo


def _wrap(func, args, kwargs, expected_failure):
    try:
        func(*args, **kwargs)
        return not expected_failure
    except Exception as e:
        if not expected_failure or "Duplicated value not allowed by unique index" not in str(e):
            return False
        else:
            return True


def test_insert_single_field_unique_index(fixture_collection):
    collection = fixture_collection
    random_key = "key-{}".format(random.randint(0, sys.maxsize))
    random_key_2 = "{}-2".format(random_key)
    collection.create_index([(random_key, pymongo.ASCENDING)], unique=True)

    assert _wrap(collection.insert_one, ({random_key: 1},), {}, False), "non-duplicated non-null failed"
    assert _wrap(collection.insert_one, ({random_key: 1},), {}, True), "duplicated non-null failed"
    assert _wrap(collection.insert_one, ({random_key_2: 1},), {}, False), "non-duplicated null failed"
    assert _wrap(collection.insert_one, ({random_key_2: 2},), {}, True), "duplicated null failed"


def test_insert_compound_unique_index(fixture_collection):
    collection = fixture_collection
    random_key = "key-{}".format(random.randint(0, sys.maxsize))
    random_key_2 = "{}-2".format(random_key)
    random_key_3 = "{}-3".format(random_key)
    collection.create_index([(random_key, pymongo.ASCENDING), (random_key_2, pymongo.ASCENDING)], unique=True)

    assert _wrap(collection.insert_one, ({random_key: 1, random_key_2: 1},), {}, False), "non-duplicated non-null failed"
    assert _wrap(collection.insert_one, ({random_key: 1, random_key_2: 1},), {}, True), "duplicated non-null failed"
    assert _wrap(collection.insert_one, ({random_key_3: 1},), {}, False), "non-duplicated all-null failed"
    assert _wrap(collection.insert_one, ({random_key_3: 2},), {}, True), "duplicated all-null failed"
    assert _wrap(collection.insert_one, ({random_key: 1},), {}, False), "non-duplicated partial-null failed"
    assert _wrap(collection.insert_one, ({random_key: 1, random_key_3: 4},), {}, True), "duplicated partial-null failed"


def test_update_single_field_unique_index(fixture_collection):
    collection = fixture_collection
    random_key = "key-{}".format(random.randint(0, sys.maxsize))
    collection.create_index([(random_key, pymongo.ASCENDING)], unique=True)
    id1 = random.randint(0, sys.maxsize)
    id2 = random.randint(0, sys.maxsize)

    assert _wrap(collection.insert_one, ({random_key: 1, "_id": id1},), {}, False), "non-duplicated non-null insert failed"
    assert _wrap(collection.insert_one, ({random_key: 2, "_id": id2},), {}, False), "non-duplicated non-null insert failed"
    assert _wrap(collection.update_one, (
        {
            "_id": id2
        },
        {
            "$set": {
                random_key: 1
            }
        },
    ), {}, True), "duplicated non-null update failed"


def test_update_compound_unique_index(fixture_collection):
    collection = fixture_collection

    random_key = "key-{}".format(random.randint(0, sys.maxsize))
    random_key_2 = "{}-2".format(random_key)
    assert _wrap(collection.create_index, ([(random_key, pymongo.ASCENDING), (random_key_2, pymongo.ASCENDING)],),
                 {'unique': True}, False), "non-duplicated non-null index creation failed"

    id1 = random.randint(0, sys.maxsize)
    id2 = random.randint(0, sys.maxsize)
    assert _wrap(collection.insert_one, ({
                                         random_key: 1,
                                         random_key_2: 1,
                                         "_id": id1
                                     },), {}, False), "non-duplicated non-null insert failed"
    assert _wrap(collection.insert_one, ({
                                         random_key: 2,
                                         random_key_2: 1,
                                         "_id": id2
                                     },), {}, False), "non-duplicated non-null insert failed"
    assert _wrap(collection.update_one, (
        {
            "_id": id2
        },
        {
            "$set": {
                random_key: 1
            }
        },
    ), {}, True), "duplicated non-null update failed"


def test_update_single_field_unique_index_with_same_value(fixture_collection):
    collection = fixture_collection
    random_key = "key-{}".format(random.randint(0, sys.maxsize))
    collection.create_index([(random_key, pymongo.ASCENDING)], unique=True)
    id1 = random.randint(0, sys.maxsize)
    id2 = random.randint(0, sys.maxsize)
    assert _wrap(collection.insert_one, ({
                                         random_key: 1,
                                         "_id": id1
                                     },), {}, False), "non-duplicated non-null insert failed"
    assert _wrap(collection.insert_one, ({
                                         random_key: 2,
                                         "_id": id2
                                     },), {}, False), "non-duplicated non-null insert failed"
    assert _wrap(collection.update_one, (
        {
            "_id": id2
        },
        {
            "$set": {
                random_key: 2
            }
        },
    ), {}, False), "same value update failed"


def test_update_compound_unique_index_with_same_value(fixture_collection):
    collection = fixture_collection
    random_key = "key-{}".format(random.randint(0, sys.maxsize))
    random_key_2 = "{}-2".format(random_key)

    assert _wrap(collection.create_index, ([(random_key, pymongo.ASCENDING), (random_key_2, pymongo.ASCENDING)],),
                 {'unique': True}, False), "non-duplicated non-null index creation failed"

    id1 = random.randint(0, sys.maxsize)
    id2 = random.randint(0, sys.maxsize)
    assert _wrap(collection.insert_one, ({
                                         random_key: 1,
                                         random_key_2: 1,
                                         "_id": id1
                                     },), {}, False), "non-duplicated non-null insert failed"
    assert _wrap(collection.insert_one, ({
                                         random_key: 2,
                                         random_key_2: 1,
                                         "_id": id2
                                     },), {}, False), "non-duplicated non-null insert failed"
    assert _wrap(collection.update_one, (
        {
            "_id": id2
        },
        {
            "$set": {
                random_key: 2
            }
        },
    ), {}, False), "same value update failed"


def test_create_single_field_unique_index(fixture_collection):
    collection = fixture_collection
    random_key = "key-{}".format(random.randint(0, sys.maxsize))
    random_key_2 = "{}-2".format(random_key)

    assert _wrap(collection.insert_one, ({
                                         random_key: 1
                                     },), {}, False), "duplicated non-null pre-index insert failed"
    assert _wrap(collection.insert_one, ({
                                         random_key: 1
                                     },), {}, False), "duplicated non-null pre-index insert failed"
    assert _wrap(collection.create_index, ([(random_key, pymongo.ASCENDING)],), {'unique': True}, True), \
        "duplicated non-null index creation failed"
    assert _wrap(collection.create_index, ([(random_key_2, pymongo.ASCENDING)],), {'unique': True}, True), \
        "duplicated null index creation failed"


def test_create_compound_unique_index(fixture_collection):
    collection = fixture_collection
    random_key = "key-{}".format(random.randint(0, sys.maxsize))
    random_key_2 = "{}-2".format(random_key)

    assert _wrap(collection.insert_one, ({
                                         random_key: 1,
                                         random_key_2: 1
                                     },), {}, False), "duplicated non-null pre-index insert failed"
    assert _wrap(collection.insert_one, ({
                                         random_key: 1,
                                         random_key_2: 1
                                     },), {}, False), "duplicated non-null pre-index insert failed"
    assert _wrap(collection.create_index, ([(random_key, pymongo.ASCENDING), (random_key_2, pymongo.ASCENDING)],),
                 {'unique': True}, True), "duplicated non-null index creation failed"


def test_unique_index_backgroud_build_request(fixture_collection):
    collection = fixture_collection
    random_key = "key-{}".format(random.randint(0, sys.maxsize))
    try:
        collection.create_index([(random_key, pymongo.ASCENDING)], unique=True, background=True)
        assert False, "did not get the expected error"
    except Exception as e:
        if "tried to create unique indexes in background" not in str(e):
            assert False, "did not get the expected error"
