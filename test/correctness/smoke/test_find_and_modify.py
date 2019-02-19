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
