def test_simple_coll_index(fixture_collection):
    collection = fixture_collection

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
    returned = collection.find({'a': {'$gt': 10, '$lt': 21}}).count()
    assert returned == 10, "Expected: 10, Received: {}".format(returned)

    # Drop an index
    collection.drop_index('idx_a')

    # Query and check results
    returned = collection.find({'a': {'$gt': 10, '$lt': 21}}).count()
    assert returned == 10, "Expected: 10, Received: {}".format(returned)

    # Drop all indexes
    collection.drop_indexes()

    # Query and check results
    returned = collection.find({'a': {'$gt': 10, '$lt': 21}}).count()
    assert returned == 10, "Expected: 10, Received: {}".format(returned)

