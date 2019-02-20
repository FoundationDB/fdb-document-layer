#!/usr/bin/python
#
# test_find_and_modify.py
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


def test_simple_remove(fixture_collection):
    collection = fixture_collection

    collection.delete_many({})
    collection.insert_one({'A': 'Hello', 'B': 'World'})
    collection.insert_one({'A': 'Hello', 'B': 'California'})
    collection.find_one_and_delete({'A': 'Hello'})
    found = collection.count()
    assert found == 1, "Expected: 1, Found: {}".format(found)


def test_simple_update(fixture_collection):
    collection = fixture_collection

    collection.delete_many({})
    collection.insert_one({'A': 'Hello', 'B': 'World'})
    collection.insert_one({'A': 'Hello', 'B': 'California'})

    collection.find_one_and_delete({'A': 'Hello'}, remove=True)
    found = collection.count()
    assert found == 1, "Expected: 1, Found: {}".format(found)

    collection.find_one_and_update({'A': 'Hello'}, update={ '$set': {'A': 'Bye'}})
    found = collection.count({'A': 'Hello'})
    assert found == 0, "Expected: 0, Found: {}".format(found)
