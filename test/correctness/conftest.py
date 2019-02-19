import pytest
import pymongo
import random

import log

logger = log.setup_logger(__name__)


@pytest.yield_fixture(scope='session')
def fixture_client():
    client = pymongo.MongoClient('127.0.0.1:27018')
    yield client


@pytest.yield_fixture(scope='session')
def fixture_db(fixture_client):
    db_name = 'db_{}'.format(random.getrandbits(64))
    db = fixture_client[db_name]
    yield db
    fixture_client.drop_database(db_name)


@pytest.yield_fixture(scope='function')
def fixture_collection(fixture_db):
    coll_name = 'coll_{}'.format(random.getrandbits(64))
    collection = fixture_db[coll_name]  # type: pymongo.collection
    yield collection
    collection.drop()
