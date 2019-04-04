#!/usr/bin/python
#
# test_update.py
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

from collections import OrderedDict
import pytest

@pytest.mark.xfail
def test_update_array_containing_none_value(fixture_collection):
    collection = fixture_collection

    collection.insert({'A': [1, None]})

    docCount = collection.find( {'A': {'$in': [ None]}}).count()
    assert docCount == 1, "Expect 1 documents before the update but got {}".format(docCount)

    collection.update({'A': {'$size': 2}} ,  OrderedDict([('$pop', {'A': -1})]))

    docCount = collection.find( {'A': {'$in': [ None]}}).count()
    assert docCount == 1, "Expect 1 documents after the update but got {}".format(docCount)

def test_addToSet_with_none_value(fixture_collection):
    collection = fixture_collection

    collection.insert_one({'_id':1, "B":[{'a':1}]})

    collection.update_one({'_id':1}, {'$addToSet': {'B': None}})

    updatedDoc = [i for i in collection.find( {'_id':1})][0]
    assert updatedDoc['B'] == [{'a':1}, None], "Expect field 'B' contains the None but got {}".format(updatedDoc)


