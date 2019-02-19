import pytest
import pymongo
import random

import log

logger = log.setup_logger(__name__)


def pytest_addoption(parser):
    parser.addoption('--doclayer-port', action='store', default=27018, help="Port that Doc Layer is listening on")


@pytest.yield_fixture(scope='session')
def fixture_client(request):
    port = request.config.getoption('--doclayer-port')
    client = pymongo.MongoClient('127.0.0.1:{}'.format(port))
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
