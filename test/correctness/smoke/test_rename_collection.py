#!/usr/bin/python
#
# test_rename_collection.py
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

from pymongo.errors import OperationFailure

# Rename Collection - New name - Without Index
def test_renamecollection_1(fixture_db):
    db = fixture_db

    # Make sure we first delete collections if it exists
    db.drop_collection("src_collection")
    db.drop_collection("dest_collection")

    # Make sure we first delete record if it exists
    db.src_collection.delete_many({})

    # Insert one document
    db.src_collection.insert_one({'A': 'Hello', 'B': 'World'})

    # Query and list collection names
    found_collections = db.collection_names()
    total_collections = len(found_collections)
    assert total_collections == 2, "Expected:2, Found: {}".format(total_collections)
    assert found_collections[0] == u'src_collection', "Expected:1, Found: {}".format(found_collections)
    assert found_collections[1] == u'system.indexes', "Expected:1, Found: {}".format(found_collections)

    # Rename src_collection to dest_collection
    db.src_collection.rename("dest_collection",dropTarget=False)

    # Query and check collection names after rename
    found_collections = db.collection_names()
    total_collections = len(found_collections)
    assert total_collections == 2, "Expected:2, Found: {}".format(total_collections)
    assert found_collections[0] == u'dest_collection', "Expected:1, Found: {}".format(found_collection) 
    assert found_collections[1] == u'system.indexes', "Expected:1, Found: {}".format(found_collections)

    # Query and check results
    found = db.dest_collection.find().count()
    assert found == 1, "Expected:1, Found: {}".format(found)

    # Drop collection
    db.drop_collection("dest_collection")

# Rename Collection - Existing name -  force_rename=false - Without Index
def test_renamecollection_2(fixture_db):
    db = fixture_db

    db.src_collection.insert_one({'A': 'Hello', 'B': 'World'})
    db.dest_collection.insert_one({'A': 'Hello', 'B': 'California'})

    # Query and check error
    try:
        db.src_collection.rename("dest_collection",dropTarget=False)
    except OperationFailure as e:
        serverErrObj = e.details
        assert serverErrObj['code'] != None
        # 29977 : Collection name already exist
        assert serverErrObj['code'] == 29977, "Expected:29977, Found: {}".format(serverErrObj)

    db.drop_collection("src_collection")
    db.drop_collection("dest_collection")

# Rename Collection - Existing name - force_rename=true - Without Index
def test_renamecollection_3(fixture_db):
    db = fixture_db

    # Make sure we first delete record if it exists
    db.src_collection.delete_many({})
    db.src_collection.insert_one({'A': 'Hello', 'B': 'World'})

    db.dest_collection.delete_many({})
    db.dest_collection.insert_one({'A': 'Hello', 'B': 'California'})

    found_collections = db.collection_names()
    total_collections = len(found_collections)
    assert total_collections == 3, "Expected:3, Found: {}".format(total_collections)
    assert found_collections[0] == u'dest_collection', "Expected:1, Found: {}".format(found_collections)
    assert found_collections[1] == u'src_collection', "Expected:1, Found: {}".format(found_collections)
    assert found_collections[2] == u'system.indexes', "Expected:1, Found: {}".format(found_collections)

    # Query and force rename
    db.src_collection.rename("dest_collection",dropTarget=True)

    # Query and check collection names after rename
    found_collections = db.collection_names()
    total_collections = len(found_collections)
    assert total_collections == 2, "Expected:2, Found: {}".format(total_collections)
    assert found_collections[0] == u'dest_collection', "Expected:1, Found: {}".format(found_collections)
    assert found_collections[1] == u'system.indexes', "Expected:1, Found: {}".format(found_collections)

    #Query and check dest_collection has one document
    found = db.dest_collection.find().count()
    assert found == 1, "Expected:1, Found: {}".format(found)

    # Query and check src_collection document moved to dest_collection
    found = db.dest_collection.find({'B':'World'}).count()
    assert found == 1, "Expected:1, Found: {}".format(found)

    db.drop_collection("dest_collection")

# Rename Collection - New name - With Index
def test_renamecollection_4(fixture_db):
    db = fixture_db

    # Make sure we first delete collections if it exists
    db.drop_collection("src_collection")
    db.drop_collection("dest_collection")

    # Create an index for source collection
    db.src_collection.create_index('field_a')
    db.src_collection.insert_one({'A': 'Hello', 'B': 'California'})

    # Query and check name space in collection index
    indexes = db.src_collection.index_information()
    src_ns = indexes['field_a_1']['ns']
    src_ns_collection = src_ns.split(".")
    assert src_ns_collection[1] == u'src_collection', "Expected:1, Found: {}".format(src_ns_collection)

    db.src_collection.rename("dest_collection",dropTarget=False)

    # Query and check collection indexes updated with dest_collection
    indexes = db.dest_collection.index_information()
    dest_ns = indexes['field_a_1']['ns']
    dest_ns_collection = dest_ns.split(".")
    assert dest_ns_collection[1] == u'dest_collection', "Expected:1, Found: {}".format(dest_ns_collection)

    # Query and check source collection index
    indexes = db.src_collection.index_information()
    assert 'field_a_1' not in indexes, "Expected:0, Found: {}".format(indexes)

    # Query and check dest_collection has src_collection document
    found = db.dest_collection.find({'B': 'California'}).count()
    assert found == 1, "Expected:1, Found: {}".format(found)

    db.drop_collection("dest_collection")

# Rename Collection - Existing name -  force_rename=false - With Index
def test_renamecollection_5(fixture_db):
    db = fixture_db

    # Create an index for source collection
    db.src_collection.create_index('field_a')
    db.src_collection.insert_one({'A': 'Hello', 'B': 'World'})

    # Create an index for destination collection
    db.dest_collection.create_index('field_b')
    db.dest_collection.insert_one({'A': 'Hello', 'B': 'California'})

    # Query and check error
    try:
        db.src_collection.rename("dest_collection",dropTarget=False)
    except OperationFailure as e:
        serverErrObj = e.details
        assert serverErrObj['code'] != None
        # 29977 : Collection name already exist
        assert serverErrObj['code'] == 29977, "Expected:29977, Found: {}".format(serverErrObj)

    db.drop_collection("src_collection")
    db.drop_collection("dest_collection")

# Rename Collection - Existing name - force_rename=true - With Index
def test_renamecollection_6(fixture_db):
    db = fixture_db

    db.src_collection.delete_many({})

    # Create an index for source collection
    db.src_collection.create_index('field_a')
    db.src_collection.insert_one({'A': 'Hello', 'B': 'World'})

    # Query and get source collection id
    indexes = db.src_collection.index_information()
    source_object_id = indexes['field_a_1']['_id']

    db.dest_collection.delete_many({})

    # Create an index for destination collection
    db.dest_collection.create_index('field_b')
    db.dest_collection.insert_one({'A': 'Hello', 'B': 'California'})

    # Query and check collection names
    found_collections = db.collection_names()
    total_collections = len(found_collections)
    assert total_collections == 3, "Expected:3, Found: {}".format(total_collections)
    assert found_collections[0] == u'dest_collection', "Expected:1, Found: {}".format(found_collections)
    assert found_collections[1] == u'src_collection', "Expected:1, Found: {}".format(found_collections)
    assert found_collections[2] == u'system.indexes', "Expected:1, Found: {}".format(found_collections)

    # Query and force rename
    db.src_collection.rename("dest_collection",dropTarget=True)

    # Query and check collection names after rename
    found_collections = db.collection_names()
    total_collections = len(found_collections)
    assert total_collections == 2, "Expected:2, Found: {}".format(total_collections)
    assert found_collections[0] == u'dest_collection', "Expected:1, Found: {}".format(found_collections)
    assert found_collections[1] == u'system.indexes', "Expected:1, Found: {}".format(found_collections)

    # Query and get destination collection id
    indexes = db.dest_collection.index_information()
    destination_object_id = indexes['field_a_1']['_id']

    # After rename, check dest_collection updated with src_collection id
    assert source_object_id == destination_object_id, "Expected:1, Found: {}".format(destination_object_id)

    indexes = db.src_collection.index_information()
    assert 'field_a_1' not in indexes, "Expected:0, Found: {}".format(indexes)

    found = db.dest_collection.find({'B': 'World'}).count()
    assert found == 1, "Expected:1, Found: {}".format(found)

    db.drop_collection("dest_collection")

# Rename Collection - Check Error - force_rename=false - Without Index
def test_renamecollection_7(fixture_db):
    db = fixture_db

    # Rename and check error, when source collection doesn't exist
    try:
        db.src_collection.rename("dest_collection",dropTarget=False)
    except OperationFailure as e:
        serverErrObj = e.details
        assert serverErrObj['code'] != None
        # 29976 : Collection name does not exist
        assert serverErrObj['code'] == 29976, "Expected:29976, Found: {}".format(serverErrObj)

# Rename Collection - Existing name - Check Error - force_rename=True - With Index
def test_renamecollection_8(fixture_db):
    db = fixture_db

    # Create an index for source collection
    db.src_collection.create_index('field_a')
    db.src_collection.insert_one({'A': 'Hello', 'B': 'World'})

    # Query and check error if src and dest collection names are same
    try:
        db.src_collection.rename("src_collection",dropTarget=True)
    except OperationFailure as e:
        serverErrObj = e.details
        assert serverErrObj['code'] != None
        # 29978 : Old and New collection name cannot be same
        assert serverErrObj['code'] == 29978, "Expected:29978, Found: {}".format(serverErrObj)

    db.drop_collection("src_collection")
