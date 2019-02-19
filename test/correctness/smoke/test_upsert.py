#
# test_upsert.py
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

import itertools
import pprint

from pymongo.errors import OperationFailure

import util
from mongo_model import MongoModel
from util import MongoModelException

pprint = pprint.PrettyPrinter().pformat


# test_1 = ("Plain upsert", { 'a': 1 }, { 'b': 1 }, "")
# test_2 = (
#    "Upsert with selector operators",
#    { 'a' : { '$gt': 1 } },
#    { 'b': 1 },
#    ""
# )
# test_3 = (
#    "Upsert with update operators",
#    { 'a' : 1 },
#    { '$set': { 'a' : 2 } },
#    ""
# )


def run_and_compare(dl_collection, test):
    mm = MongoModel('DocLayer')
    mm_collection = mm['test']['test']

    (selector, update) = test

    update = util.deep_convert_to_ordered(update)

    dl_collection.delete_many({})

    def up(c, s, u):
        c.update_one(s, u, upsert=True)

    mm_err = dl_err = False
    try:
        up(mm_collection, selector, update)
    except MongoModelException:
        mm_err = True
    try:
        up(dl_collection, selector, update)
    except OperationFailure:
        dl_err = True
    assert mm_err == dl_err, "Expected both to fail or succeed but, mm: {}, dl: {}".format(mm_err, dl_err)

    mm_ret = [util.deep_convert_to_unordered(i) for i in mm_collection.find({})]
    mm_ret.sort()
    dl_ret = [util.deep_convert_to_unordered(i) for i in dl_collection.find({})]
    dl_ret.sort()

    for doc in mm_ret:
        del doc['_id']

    for doc in dl_ret:
        del doc['_id']

    assert mm_ret == dl_ret


def test_multiple_disparate_operators(fixture_collection):
    run_and_compare(fixture_collection, ({
                                             '$and': [{
                                                 '$and': [{
                                                     'a': 1
                                                 }]
                                             }],
                                             'b': {
                                                 '$gt': 1
                                             }
                                         }, {
                                             '$set': {
                                                 'c': 35
                                             }
                                         }))


def test_logical_operators(fixture_collection):
    run_and_compare(fixture_collection, ({'$and': [{'a.b.c.d': 1}, {'f': 17}]}, {'$set': {'yf': 17}}))


# {$and:[{$and:[{a:1}]}], b:{$gt: 1}}


def create_upsert_test_with_selector_operator(operator, object, depth, update):
    selector = next_selector = {}
    next_char = 'a'
    for i in range(0, depth):
        next_selector[next_char] = {}
        next_selector = next_selector[next_char]
        next_char = chr(ord(next_char) + 1)

    next_selector[operator] = object

    return selector, update


def create_upsert_with_dotted_selector_operator(operator, object, depth, update):
    selector = {}
    k = ['a']
    next_char = 'b'
    for i in range(0, depth):
        k.append(next_char)
        next_char = chr(ord(next_char) + 1)

    k_str = '.'.join(k)

    selector[k_str] = {}
    selector[k_str][operator] = object

    return selector, update


def create_upsert_dotted_selector_operator_test_with_operator_in_initial_position(operator, object, depth, update):
    selector = {}
    k = ['a']
    next_char = 'b'
    for i in range(0, depth):
        k.append(next_char)
        next_char = chr(ord(next_char) + 1)

    k_str = '.'.join(k)

    selector[operator] = {}
    selector[operator][k_str] = object

    return selector, update


def create_operator_tests(operators, depth, update):
    return [
        func(operator, object, depth, update) for name, func in globals().iteritems()
        if name.startswith('create_upsert_') for operator, object in operators
    ]


def create_operator_permutation_tests(oplist, depth, repeat, update):
    op_permutations = itertools.product(oplist, repeat=repeat)

    tests = []

    for ops in op_permutations:
        next_char = 'a'
        next_selector = selector = {}
        for i in range(0, depth):
            for op, obj in ops:
                next_selector[op] = obj
            tests.append((util.OrderedDict(selector), update))
            next_selector[next_char] = {}
            next_selector = next_selector[next_char]
            next_char = chr(ord(next_char) + 1)
    return tests


operator_types = {
    'comparison_operators': (
        ('$gt', 0),
        ('$gte', 0),
        ('$in', [17, 2, 3]),
        ('$in', [17]),
        ('$lt', 17),
        ('$lte', 17),
        ('$ne', 17),
        ('$nin', [17, 2, 3]),
        ('$nin', [17]),
        ('$eq', 17),
    ),
    'logical_operators': (
        ('$or', [{
            'a': 17
        }, {
            'b': 17
        }]),
        ('$or', [{
            '$and': [{
                'a': 17
            }, {
                'b': 17
            }]
        }]),
        ('$or', [{
            '$and': [{
                'a': 17
            }, {
                '$and': [{
                    'c': 18
                }, {
                    'd': 19
                }]
            }, {
                'b': 17
            }]
        }]),
        ('$or', [{
            '$and': [{
                'a': 17
            }, {
                'b': 17
            }]
        }, {
            'c': 17
        }]),
        ('$or', [{
            'c': 17
        }, {
            '$and': [{
                'a': 17
            }, {
                'b': 17
            }]
        }]),
        ('$or', [{
            'c': 17
        }, {
            '$and': [{
                'a': 17
            }, {
                '$and': [{
                    'd': 18
                }, {
                    'e': 19
                }]
            }, {
                'b': 17
            }]
        }]),
        ('$or', [{
            'c': 17
        }, {
            '$or': [{
                'a': 17
            }, {
                'b': 17
            }]
        }]),
        ('$and', [{
            'a': 17
        }, {
            'b': 17
        }]),
        ('$and', [{
            '$and': [{
                'a': 17
            }, {
                'b': 17
            }]
        }]),
        ('$and', [{
            'c': 17
        }, {
            '$and': [{
                'a': 17
            }, {
                'b': 17
            }]
        }]),
        ('$and', [{
            'c.d.e.f': 17
        }, {
            '$or': [{
                'a': 17
            }, {
                'b': 17
            }]
        }]),
        ('$nor', [{
            'a': 17
        }, {
            'b': 17
        }]),
    ),
    'logical_not_operator': (
        # ('$not', 17), # the documentation says `$not` requires an operator expression as its predicate
        ('a', {
            '$not': {
                '$gte': 3
            }
        }),),
    'element_operators': (
        ('$exists', True),
        ('$type', 2),
    ),
    'array_operators': (
        ('$all', [17, 2, 3]),  # these are breaking for some reason
        ('$all', []),
        ('$all', [35]),
        ('$size', 1),
        ('$size', 0),
        ('$size', None),
        ('$elemMatch', {
            '$gte': 0,
            '$lt': 10
        }),
        ('$elemMatch', {
            '$lt': 10
        }),
    )
}

updates = (
    # fields
    {
        '$set': {
            'yf': 17
        }
    },
    {
        '$inc': {
            'yf': 17
        }
    },
    {
        '$mul': {
            'yf': 2
        }
    },
    {
        '$rename': {
            'yf': 'b'
        }
    },
    {
        '$setOnInsert': {
            'yf': 17
        }
    },
    {
        '$set': {
            'yf': 17
        }
    },
    {
        '$unset': {
            'yf': ''
        }
    },
    {
        '$min': {
            'yf': -1
        }
    },
    {
        '$max': {
            'yf': 100
        }
    },

    # arrays
    {
        '$addToSet': {
            'yf': [17, 18, 19]
        }
    },
    {
        '$pop': {
            'yf': 1
        }
    },
    {
        '$pullAll': {
            'yf': [17, 18, 19]
        }
    },
    {
        '$pull': {
            'yf': 17
        }
    },
    #    {'$pushAll':{'yf':[17,18,19]}},
    {
        '$push': {
            'yf': 17
        }
    },
)


def operators_test_with_depth(dl_collection, depth):
    for update in updates:
        for operator_type, operators in operator_types.iteritems():
            for test_cfg in create_operator_tests(operators, depth, update):
                run_and_compare(dl_collection, test_cfg)


def test_operators_with_depth_0(fixture_collection):
    operators_test_with_depth(fixture_collection, 0)


def test_operators_with_depth_1(fixture_collection):
    operators_test_with_depth(fixture_collection, 1)


def test_operators_with_depth_2(fixture_collection):
    operators_test_with_depth(fixture_collection, 2)


def test_operators_with_depth_3(fixture_collection):
    operators_test_with_depth(fixture_collection, 3)


def test_operators_with_depth_4(fixture_collection):
    operators_test_with_depth(fixture_collection, 4)


def operators_permutation_test_with_depth(dl_collection, depth):
    oplist = []
    for _, operators in operator_types.iteritems():
        for op in operators:
            oplist.append(op)

    for update in updates:
        for repeat in range(1, 2):
            for test_cfg in create_operator_permutation_tests(oplist, depth, repeat, update):
                run_and_compare(dl_collection, test_cfg)


def test_operators_permutation_with_depth_0(fixture_collection):
    operators_permutation_test_with_depth(fixture_collection, 0)


def test_operators_permutation_with_depth_1(fixture_collection):
    operators_permutation_test_with_depth(fixture_collection, 1)


def test_operators_permutation_with_depth_2(fixture_collection):
    operators_permutation_test_with_depth(fixture_collection, 2)


def test_operators_permutation_with_depth_3(fixture_collection):
    operators_permutation_test_with_depth(fixture_collection, 3)


def test_operators_permutation_with_depth_4(fixture_collection):
    operators_permutation_test_with_depth(fixture_collection, 4)
