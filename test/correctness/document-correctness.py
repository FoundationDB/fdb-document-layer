#!/usr/bin/python
#
# document-correctness.py
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

import argparse
import copy
import os.path
import random
import sys

import pymongo

import gen
import transactional_shim
import util
from mongo_model import MongoCollection
from mongo_model import MongoModel
from util import MongoModelException


def get_clients(str1, str2, ns):
    client_dict = {
        "mongo": lambda: pymongo.MongoClient(ns['mongo_host'], ns['mongo_port'], maxPoolSize=1),
        "mm": lambda: MongoModel("DocLayer"),
        "doclayer": lambda: pymongo.MongoClient(ns['doclayer_host'], ns['doclayer_port'], maxPoolSize=1)
    }
    if 'mongo' in [ns['1'], ns['2']] and 'mm' in [ns['1'], ns['2']]:
        client_dict['mm'] = lambda: MongoModel("MongoDB")

    instance_id = str(random.random())[2:] if ns['instance_id'] == 0 else str(ns['instance_id'])
    print 'Instance: ' + instance_id

    client1 = client_dict[str1]()
    client2 = client_dict[str2]()
    return (client1, client2, instance_id)


def get_clients_and_collections(ns):
    (client1, client2, instance) = get_clients(ns['1'], ns['2'], ns)
    db1 = client1['test']
    db2 = client2['test']
    collection1 = db1['correctness' + instance]
    collection2 = db2['correctness' + instance]
    transactional_shim.remove(collection1)
    transactional_shim.remove(collection2)
    return (client1, client2, collection1, collection2)


def get_collections(ns):
    (client1, client2, collection1, collection2) = get_clients_and_collections(ns)
    return (collection1, collection2)


def get_result(query, collection, projection, sort, limit, skip, exception_msg):
    try:
        cur = transactional_shim.find(collection, query, projection)

        if sort is None:
            ret = [util.deep_convert_to_unordered(i) for i in cur]
            ret.sort(cmp=util.mongo_compare_unordered_dict_items)
        elif isinstance(collection, MongoCollection):
            ret = copy.deepcopy([i for i in cur])
            for i, val in enumerate(ret):
                if '_id' in val:
                    val['_id'] = 0

            ret = util.MongoModelNondeterministicList(ret, sort, limit, skip, query, projection, collection.options)
            # print ret
        else:
            ret = [i for i in cur.sort(sort).skip(skip).limit(limit)]
            for i, val in enumerate(ret):
                if '_id' in val:
                    val['_id'] = 0
                # print '1====', i, ret[i]

        return ret

    except pymongo.errors.OperationFailure as e:
        exception_msg.append('Caught PyMongo error:\n\n'
                             '  Collection: %s\n' % str(collection) + '  Exception: %s\n' % str(e) +
                             '  Query: %s\n' % str(query) + '  Projection: %s\n' % str(projection) +
                             '  Sort: %s\n' % str(sort) + '  Limit: %r\n' % limit + '  Skip: %r\n' % skip)
    except MongoModelException as e:
        exception_msg.append('Caught Mongo Model error:\n\n'
                             '  Collection: %s\n' % str(collection) + '  Exception: %s\n' % str(e) +
                             '  Query: %s\n' % str(query) + '  Projection: %s\n' % str(projection) +
                             '  Sort: %s\n' % str(sort) + '  Limit: %r\n' % limit + '  Skip: %r\n' % skip)

    return list()


def format_result(collection, result, index):
    formatted = '{:<20} ({})'.format(collection.__module__, len(result))
    if index < len(result):
        formatted += ': %r' % result[index]

    return formatted


def doc_as_normalized_string(thing):
    if isinstance(thing, dict):
        return '{' + ', '.join(
            [doc_as_normalized_string(x) + ": " + doc_as_normalized_string(thing[x]) for x in sorted(thing)]) + '}'
    elif isinstance(thing, list):
        return '[' + ', '.join([doc_as_normalized_string(x) for x in thing]) + '}'
    elif isinstance(thing, unicode) or isinstance(thing, str):
        return "'" + thing + "'"
    return str(thing)


def diff_results(cA, rA, cB, rB):
    # For each result list create a set of normalized strings representing each of the docs
    a = set([str(doc_as_normalized_string(x)) for x in rA])
    b = set([str(doc_as_normalized_string(x)) for x in rB])

    only_a = a - b
    only_b = b - a

    if len(only_a) > 0 or len(only_b) > 0:
        print "  RESULT SET DIFFERENCES (as 'sets' so order within the returned results is not considered)"
    for x in only_a:
        print "    Only in", cA.__module__, ":", x
    for x in only_b:
        print "    Only in", cB.__module__, ":", x
    print


zero_resp_queries = 0
total_queries = 0


def check_query(query, collection1, collection2, projection=None, sort=None, limit=0, skip=0):
    util.trace('debug', '\n==================================================')
    util.trace('debug', 'query:', query)
    util.trace('debug', 'sort:', sort)
    util.trace('debug', 'limit:', limit)
    util.trace('debug', 'skip:', skip)

    exception_msg = list()

    ret1 = get_result(query, collection1, projection, sort, limit, skip, exception_msg)
    ret2 = get_result(query, collection2, projection, sort, limit, skip, exception_msg)
    if len(exception_msg) == 1:
        print '\033[91m\n', exception_msg[0], '\033[0m'
        return False

    global total_queries
    total_queries += 1
    if len(ret1) == 0 and len(ret2) == 0:
        global zero_resp_queries
        zero_resp_queries += 1
        # print 'Zero responses so far: {}/{}'.format(zero_resp_queries, total_queries)

    if isinstance(ret1, util.MongoModelNondeterministicList):
        return ret1.compare(ret2)
    elif isinstance(ret2, util.MongoModelNondeterministicList):
        return ret2.compare(ret1)

    i = 0
    try:
        for i in range(0, max(len(ret1), len(ret2))):
            assert ret1[i] == ret2[i]
        return True
    except AssertionError:
        print '\nQuery results didn\'t match at index %d!' % i
        print 'Query: %r' % query
        print 'Projection: %r' % projection
        print '\n  %s' % format_result(collection1, ret1, i)
        print '  %s\n' % format_result(collection2, ret2, i)

        diff_results(collection1, ret1, collection2, ret2)

        # for i in range(0, max(len(ret1), len(ret2))):
        #    print '\n%d: %s' % (i, format_result(collection1, ret1, i))
        #    print '%d: %s' % (i, format_result(collection2, ret2, i))

        return False
    except IndexError:
        print 'Query results didn\'t match!'
        print 'Query: %r' % query
        print 'Projection: %r' % projection

        print '\n  %s' % format_result(collection1, ret1, i)
        print '  %s\n' % format_result(collection2, ret2, i)

        diff_results(collection1, ret1, collection2, ret2)

        # for i in range(0, max(len(ret1), len(ret2))):
        #    print '\n%d: %s' % (i, formatResult(collection1, ret1, i))
        #    print '%d: %s' % (i, formatResult(collection2, ret2, i))

        return False


def test_update(collections, verbose=False):
    okay = True
    for i in range(1, 10):
        update = gen.random_update(collections[0])

        util.trace('debug', '\n========== Update No.', i, '==========')
        util.trace('debug', 'Query:', update['query'])
        util.trace('debug', 'Update:', str(update['update']))
        util.trace('debug', 'Number results from collection: ', gen.count_query_results(
            collections[0], update['query']))
        for item in transactional_shim.find(collections[0], update['query']):
            util.trace('debug', 'Find Result0:', item)

        exception = []
        exception_msg = []
        for coll in collections:
            try:
                if verbose:
                    all = [x for x in coll.find(dict())]
                    for item in transactional_shim.find(coll, update['query']):
                        print 'Before update doc:', item
                    print 'Before update coll size: ', len(all)

                transactional_shim.update(
                    coll, update['query'], update['update'], upsert=update['upsert'], multi=update['multi'])

                if verbose:
                    all = [x for x in coll.find(dict())]
                    for item in coll.find(update['query']):
                        print 'After update doc:', item
                    print 'After update coll size: ', len(all)

            except pymongo.errors.OperationFailure as e:
                exception.append(e)
                exception_msg.append(
                    util.join_n('Caught PyMongo error while attempting update: %s' % e[0],
                                'Query: %s' % update['query'], 'Update: %s' % update['update'],
                                'Upsert: {0}, Multi: {1}'.format(update['upsert'], update['multi'])))
            except MongoModelException as e:
                exception.append(e)
                exception_msg.append(
                    util.join_n('Caught MongoModel error. Offending update(', str(update['query']),
                                str(update['update']), str(update['upsert']), str(update['multi']), ')'))

        if len(exception_msg) == 1:
            print 'Update: ' + str(update['update'])
            print '\033[91m', exception[0], '\033[0m'
            print '\033[91m', exception_msg[0], '\033[0m'
            return False

        if not check_query(dict(), collections[0], collections[1]):
            print 'Update: ' + str(update['update'])
            return False

    return okay


def one_iteration(collection1, collection2, ns, seed):
    update_tests_enabled = ns['no_updates']
    sorting_tests_enabled = gen.generator_options.allow_sorts
    indexes_enabled = ns['no_indexes']
    projections_enabled = ns['no_projections']
    verbose = ns['verbose']
    num_doc = ns['num_doc']
    fname = "unknown"

    def _run_operation_(op1, op2):
        okay = True
        exceptionOne = None
        exceptionTwo = None
        func1, args1, kwargs1 = op1
        func2, args2, kwargs2 = op2
        try:
            func1(*args1, **kwargs1)
        except pymongo.errors.OperationFailure as e:
            exceptionOne = e
        except MongoModelException as e:
            exceptionOne = e
        try:
            func1(*args2, **kwargs2)
        except pymongo.errors.OperationFailure as e:
            exceptionTwo = e
        except MongoModelException as e:
            exceptionTwo = e

        if ((exceptionOne is None and exceptionTwo is None)
            or (exceptionOne is not None and exceptionTwo is not None and exceptionOne.code == exceptionTwo.code)):
            pass
        else:
            print '\033[91m Unmatched result: \033[0m'
            print '\033[91m', type(exceptionOne), ': ', str(exceptionOne), '\033[0m'
            print '\033[91m', type(exceptionTwo), ': ', str(exceptionTwo), '\033[0m'
            okay = False
            if [x for x in ignored_exceptions if x.strip() == str(exceptionOne).strip().strip('\"').strip('.')]:
                print "Ignoring EXCEPTION :", str(exceptionOne)
                okay = True
            elif [x for x in ignored_exceptions if x.strip() == str(exceptionTwo).strip().strip('\"').strip('.')]:
                print "Ignoring EXCEPTION :", str(exceptionTwo)
                okay = True
        return okay

    try:
        okay = True

        if verbose:
            util.traceLevel = 'debug'

        fname = util.save_cmd_line(util.command_line_str(ns, seed))

        if indexes_enabled:
            transactional_shim.drop_indexes(collection1)
            transactional_shim.drop_indexes(collection2)
        transactional_shim.remove(collection1)
        transactional_shim.remove(collection2)

        indexes = []
        indexes_first = gen.global_prng.choice([True, False])
        if indexes_enabled:
            for i in range(0, 5):
                index_obj = gen.random_index_spec()
                indexes.append(index_obj)

        if indexes_first:
            for i in indexes:
                # When we do enable make sure we don't enable for 50% cases. That way unique indexes will cloud everything else. Make it something like 5-10%.
                # uniqueIndex = gen.global_prng.choice([True, False])
                uniqueIndex = False
                okay = _run_operation_(
                    (transactional_shim.ensure_index, (collection1, i), {"unique":uniqueIndex}),
                    (transactional_shim.ensure_index, (collection2, i), {"unique":uniqueIndex})
                    )
                if not okay:
                    return (okay, fname, None)
        docs = []
        for i in range(0, num_doc):
            doc = gen.random_document(True)
            docs.append(doc)

        okay = _run_operation_(
            (transactional_shim.insert, (collection1, docs), {}),
            (transactional_shim.insert, (collection2, docs), {})
            )
        if not okay:
            print "Failed when doing inserts"
            return (okay, fname, None)

        if not indexes_first:
            for i in indexes:
                # When we do enable make sure we don't enable for 50% cases. That way unique indexes will cloud everything else. Make it something like 5-10%.
                # uniqueIndex = gen.global_prng.choice([True, False])
                uniqueIndex = False
                okay = _run_operation_(
                    (transactional_shim.ensure_index, (collection1, i), {"unique":uniqueIndex}),
                    (transactional_shim.ensure_index, (collection2, i), {"unique":uniqueIndex})
                    )
                if not okay:
                    print "Failed when adding index after insert"
                    return (okay, fname, None)

        okay = check_query(dict(), collection1, collection2)
        if not okay:
            return (okay, fname, None)

        if update_tests_enabled:
            if not test_update([collection1, collection2], verbose):
                okay = False
                return (okay, fname, None)

        for ii in range(1, 30):
            query = gen.random_query()
            if not sorting_tests_enabled:
                sort = None
                limit = 0
                skip = 0
            else:
                sort = gen.random_query_sort()
                limit = gen.global_prng.randint(0, 600)
                skip = gen.global_prng.randint(0, 10)

            # Always generate a projection, whether or not we use it. This allows us to run the same test in
            # either case.
            temp_projection = gen.random_projection()
            if not projections_enabled:
                projection = None
            else:
                projection = temp_projection

            okay = check_query(query, collection1, collection2, projection, sort=sort, limit=limit, skip=skip)
            if not okay:
                return (okay, fname, None)

        if not okay:
            return (okay, fname, None)

    except Exception as e:
        import traceback
        traceback.print_exc()
        return (False, fname, e)

    return (okay, fname, None)


ignored_exceptions = [
    "Multi-multikey index size exceeds maximum value",
    "key too large to index", # it's hard to estimate the exact byte size of KVS key to be inserted, ignore for now.
    "Key length exceeds limit",
    "Operation aborted because the transaction timed out",
]


def test_forever(ns):
    (client1, client2, collection1, collection2) = get_clients_and_collections(ns)

    seed = ns['seed']
    bgf_enabled = ns['buggify']
    num_iter = ns['num_iter']

    jj = 0
    okay = True

    gen.global_prng = random.Random(seed)

    # this assumes that the database name we use for testing is "test"
    client = client1 if "doclayer" == ns['1'] else (client2 if "doclayer" == ns['2'] else None)
    if client is not None:
        client.test.command("buggifyknobs", bgf_enabled)

    while okay:
        jj += 1
        if num_iter != 0 and jj > num_iter:
            break

        print '========================================================'
        print 'ID : ' + str(os.getpid()) + ' iteration : ' + str(jj)
        print '========================================================'
        (okay, fname, e) = one_iteration(collection1, collection2, ns, seed)

        if not okay:
            # print 'Seed for failing iteration: ', seed
            fname = util.rename_file(fname, ".failed")
            # print 'File for failing iteration: ', fname
            with open(fname, 'r') as fp:
                for line in fp:
                    print line
            break

        # Generate a new seed and start over
        seed = random.randint(0, sys.maxint)
        gen.global_prng = random.Random(seed)

    return okay


def start_forever_test(ns):
    gen.generator_options.test_nulls = ns['no_nulls']
    gen.generator_options.upserts_enabled = ns['no_upserts']
    gen.generator_options.numeric_fieldnames = ns['no_numeric_fieldnames']
    gen.generator_options.allow_sorts = ns['no_sort']

    util.weaken_tests(ns)

    return test_forever(ns)


def start_self_test(ns):
    from threading import Thread
    import time
    import sys

    class NullWriter(object):
        def write(self, arg):
            pass

    ns['1'] = ns['2'] = 'mm'
    (collection1, collection2) = get_collections('mm', 'mm', ns)
    (collection3, collection4) = get_collections('mm', 'mm', ns)

    collection2.options.mongo6050_enabled = False

    oldstdout = sys.stdout

    def tester_thread(c1, c2):
        test_forever(
            collection1=c1,
            collection2=c2,
            seed=random.random(),
            update_tests_enabled=True,
            sorting_tests_enabled=True,
            indexes_enabled=False,
            projections_enabled=True,
            verbose=False)

    t1 = Thread(target=tester_thread, args=(collection1, collection2))
    t1.daemon = True

    t2 = Thread(target=tester_thread, args=(collection3, collection4))
    t2.daemon = True

    sys.stdout = NullWriter()

    t1.start()

    for i in range(1, 5):
        time.sleep(1)
        if not t1.is_alive():
            sys.stdout = oldstdout
            print 'SUCCESS: Test harness found artificial bug'
            break

    sys.stdout = oldstdout

    if t1.is_alive():
        print 'FAILURE: Test harness did not find obvious artificial bug in 5 seconds'

    sys.stdout = NullWriter()

    t2.start()

    for i in range(1, 5):
        time.sleep(1)
        if not t2.is_alive():
            sys.stdout = oldstdout
            print 'FAILURE: Test of model vs. itself did not match'
            return

    sys.stdout = oldstdout

    print 'SUCCESS: Model was consistent with itself'


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-v', '--verbose', default=False, action='store_true', help='verbose')
    parser.add_argument('--mongo-host', type=str, default='localhost', help='hostname of MongoDB server')
    parser.add_argument('--mongo-port', type=int, default=27018, help='port of MongoDB server')
    parser.add_argument('--doclayer-host', type=str, default='localhost', help='hostname of document layer server')
    parser.add_argument('--doclayer-port', type=int, default=27019, help='port of document layer server')
    parser.add_argument('--max-pool-size', type=int, default=None, help='maximum number of threads in the thread pool')
    subparsers = parser.add_subparsers(help='type of test to run')

    parser_forever = subparsers.add_parser('forever', help='run comparison test until failure')
    parser_forever.add_argument('1', choices=['mongo', 'mm', 'doclayer'], help='first tester')
    parser_forever.add_argument('2', choices=['mongo', 'mm', 'doclayer'], help='second tester')
    parser_forever.add_argument(
        '-s', '--seed', type=int, default=random.randint(0, sys.maxint), help='random seed to use')
    parser_forever.add_argument('--no-updates', default=True, action='store_false', help='disable update tests')
    parser_forever.add_argument(
        '--no-sort', default=True, action='store_false', help='disable non-deterministic sort tests')
    parser_forever.add_argument(
        '--no-numeric-fieldnames',
        default=True,
        action='store_false',
        help='disable use of numeric fieldnames in subobjects')
    parser_forever.add_argument(
        '--no-nulls', default=True, action='store_false', help='disable generation of null values')
    parser_forever.add_argument(
        '--no-upserts', default=True, action='store_false', help='disable operator-operator upserts in update tests')
    parser_forever.add_argument(
        '--no-indexes', default=True, action='store_false', help='disable generation of random indexes')
    parser_forever.add_argument(
        '--no-projections', default=True, action='store_false', help='disable generation of random query projections')
    parser_forever.add_argument('--num-doc', type=int, default=300, help='number of documents in the collection')
    parser_forever.add_argument('--buggify', default=False, action='store_true', help='enable buggification')
    parser_forever.add_argument('--num-iter', type=int, default=0, help='number of iterations of this type of test')
    parser_forever.add_argument(
        '--instance-id',
        type=int,
        default=0,
        help='the instance that we would like to test with, default is 0 which means '
        'autogenerate it randomly')

    parser_self_test = subparsers.add_parser('self_test', help='test the test harness')

    parser_forever.set_defaults(func=start_forever_test)
    parser_self_test.set_defaults(func=start_self_test)

    ns = vars(parser.parse_args())

    okay = ns['func'](ns)
    sys.exit(not okay)
