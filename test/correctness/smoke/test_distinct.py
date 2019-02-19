#
# test_distinct.py
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

import random
import sys


def _generate_unique_int(seen):
    tmp = random.randint(0, sys.maxint)
    while tmp in seen:
        tmp = random.randint(0, sys.maxint)
    seen.add(tmp)
    return tmp


def _generate_random_duplicated_int(seen):
    tmp = random.randint(0, 1000)  # do not use sys.maxint since we may want duplication
    want_duplicated = random.randint(0, 1) > 0
    while len(seen) != 0:
        if tmp in seen and not want_duplicated:
            tmp = random.randint(0, 1000)
        elif tmp not in seen and want_duplicated:
            tmp = random.randint(0, 1000)
        else:
            break
    seen.add(tmp)
    return tmp


def distinct_test(test_name, collection, field, records, expected_return, query=None):
    def transform(elm):
        if isinstance(elm, list):
            return tuple(elm)
        else:
            return elm

    for record in records:
        # Make sure we first delete record if it exists
        collection.delete_many({"_id": record["_id"]})
        # insert
        collection.insert_one(record)
    actual_return = map(transform, collection.distinct(field, query))
    expected_return = map(transform, expected_return)
    assert len(actual_return) == len(expected_return) and set(actual_return) == set(expected_return), \
        "{} failed. Expected: {}; Actual: {}".format(test_name, expected_return, actual_return)


def test_values_with_arrays(fixture_collection):
    # test values like
    # {"k1": 1, "k2": [1,2,3]}
    # {"k1": 1, "k2": 2}
    # {"k1": 1, "k2": 5}
    # {"k1": 1, "k2": [4, [1]]}
    # when query collection.distinct("k2"), we should get [1,2,3,4,5,[1]]
    number_of_records = random.randint(1, 100)
    key = "test_key_{}".format(random.randint(0, sys.maxint))
    records = []
    ids = set()
    values = set()

    for _ in range(0, number_of_records):
        id = _generate_unique_int(ids)
        vType = random.randint(0, 2)
        if vType == 0:
            # add an array with ints
            array_size = random.randint(1, 5)
            array_value = []
            for _ in range(0, array_size):
                array_value.append(_generate_unique_int(values))
            records.append({"_id": random.randint(0, sys.maxint), key: array_value})
        elif vType == 1:
            # add an array with ints and arrays as its elements
            array_size = random.randint(1, 5)
            array_value = []
            for _ in range(0, array_size):
                if random.randint(0, 1) == 1:
                    array_value.append(_generate_unique_int(values))
                else:
                    tmp = random.randint(0, sys.maxint)
                    values.add(tuple([tmp]))
                    array_value.append([tmp])
            records.append({"_id": random.randint(0, sys.maxint), key: array_value})
        else:
            # add ints
            records.append({"_id": random.randint(0, sys.maxint), key: _generate_unique_int(values)})

    def transform(elm):
        if isinstance(elm, tuple):
            return list(elm)
        else:
            return elm

    distinct_test("[Values with arrays; No query]", fixture_collection,
                  key, records, map(transform, list(values)), None)


def test_values_no_duplicates_no_query(fixture_collection):
    number_of_records = random.randint(1, 100)
    key = "test_key_{}".format(random.randint(0, sys.maxint))
    records = []
    ids = set()
    values = set()
    for _ in range(0, number_of_records):
        id = _generate_unique_int(ids)
        value = _generate_unique_int(values)
        records.append({"_id": random.randint(0, sys.maxint), key: value})
    distinct_test("[Values with no duplicates; No query]", fixture_collection,
                  key, records, list(values), None)


def test_values_no_duplicates_with_query(fixture_collection):
    number_of_records = random.randint(1, 100)
    key = "test_key_{}".format(random.randint(0, sys.maxint))
    key2 = "test_key_1"
    records = []
    ids = set()
    values = [set(), set()]
    for _ in range(0, number_of_records):
        key2_val = random.randint(0, 1)
        id = _generate_unique_int(ids)
        value = _generate_unique_int(values[key2_val])
        records.append({"_id": random.randint(0, sys.maxint), key: value, key2: key2_val})
    distinct_test("[Values with no duplicates; With query]", fixture_collection,
                  key, records, list(values[key2_val]), {key2: key2_val})


def test_values_with_duplicates_no_query(fixture_collection):
    number_of_records = random.randint(1, 100)
    key = "test_key_{}".format(random.randint(0, sys.maxint))
    records = []
    ids = set()
    values = set()
    for _ in range(0, number_of_records):
        id = _generate_unique_int(ids)
        value = _generate_random_duplicated_int(values)
        records.append({"_id": random.randint(0, sys.maxint), key: value})
    distinct_test("[Values with duplicates; No query]",
                  fixture_collection, key, records, list(values), None)


def test_values_with_duplicates_with_query(fixture_collection):
    number_of_records = random.randint(1, 100)
    key = "test_key_{}".format(random.randint(0, sys.maxint))
    key2 = "test_key_1"
    records = []
    ids = set()
    values = [set(), set()]
    for _ in range(0, number_of_records):
        key2_val = random.randint(0, 1)
        id = _generate_unique_int(ids)
        value = _generate_random_duplicated_int(values[key2_val])
        records.append({"_id": random.randint(0, sys.maxint), key: value, key2: key2_val})

    distinct_test("[Values with duplicates; With query]", fixture_collection,
                  key, records, list(values[key2_val]), {key2: key2_val})
