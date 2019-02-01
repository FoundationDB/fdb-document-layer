def test_simple_remove(collection):
    collection.insert({'A': 'Hello', 'B': 'World'})
    collection.insert({'A': 'Hello', 'B': 'California'})
    collection.find_and_modify({'A': 'Hello'}, remove=True)
    return collection.count() == 1


def test_simple_update(collection):
    collection.insert({'A': 'Hello', 'B': 'World'})
    collection.insert({'A': 'Hello', 'B': 'California'})

    collection.find_and_modify({'A': 'Hello'}, remove=True)
    if collection.count() != 1:
        return False

    collection.find_and_modify({'A': 'Hello'}, update={'A': 'Bye'})
    return collection.count({'A': 'Hello'}) == 0


tests = [locals()[attr] for attr in dir() if attr.startswith('test_')]


def test_all(collection1, collection2):
    print "findAndModify tests only use first collection specified"
    okay = True
    for t in tests:
        collection1.remove()
        okay = t(collection1) and okay
    return okay
