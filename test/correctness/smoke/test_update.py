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
from pymongo.errors import OperationFailure
import pytest


def test_update_array_containing_none_value(fixture_collection):
    collection = fixture_collection

    collection.insert_one({'A': [1, None]})

    docCount = collection.find( {'A': {'$in': [ None]}}).count()
    assert docCount == 1, "Expect 1 documents before the update but got {}".format(docCount)

    collection.update_one({'A': {'$size': 2}} ,  OrderedDict([('$pop', {'A': -1})]))

    docCount = collection.find( {'A': {'$in': [ None]}}).count()
    assert docCount == 1, "Expect 1 documents after the update but got {}".format(docCount)


def test_addToSet_with_none_value(fixture_collection):
    collection = fixture_collection

    collection.insert_one({'_id':1, "B":[{'a':1}]})

    collection.update_one({'_id':1}, {'$addToSet': {'B': None}})

    updatedDoc = [i for i in collection.find( {'_id':1})][0]
    assert updatedDoc['B'] == [{'a':1}, None], "Expect field 'B' contains the None but got {}".format(updatedDoc)

#156: staticValidateUpdateObject function is moved to updateDocument constructor

def test_update_exception_error_1(fixture_collection):
    collection = fixture_collection

    collection.delete_many({})

    collection.insert_one({'_id':1, "B":[{'a':1}]})
    #'_id' cannot be modified
    #In case try to update '_id' it should throw an error
    try:
        collection.update_one({'_id':1}, {'$set': {'_id': 2}})
    except OperationFailure as e:
        serverErrObj = e.details
        assert serverErrObj['code'] != None
        # 20010 : You may not modify '_id' in an update
        assert serverErrObj['code'] == 20010, "Expected:20010, Found: {}".format(serverErrObj)


def test_update_exception_error_2(fixture_collection):
    collection = fixture_collection

    collection.delete_many({})

    collection.insert_one({'_id':1, "B":"qty"})
    #'$rename' operator should not be empty
    #In case '$rename' operator is empty it should throw an error
    try:
        collection.update_one({'_id':1}, {'$rename': {}})
    except OperationFailure as e:
        serverErrObj = e.details
        assert serverErrObj['code'] != None
        # 26840 : Update operator has empty object for parameter. You must specify a field.
        assert serverErrObj['code'] == 26840, "Expected:26840, Found: {}".format(serverErrObj)
