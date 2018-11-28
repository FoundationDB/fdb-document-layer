#
# upsert_tests.py
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

from pymongo.errors import OperationFailure

import itertools
import pprint
import sys
import util
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

test_multiple_disparate_operators_1 = ("Multiple disparate operators 1", {
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
}, 'mixed')

test_logical_1 = ("X", {'$and': [{'a.b.c.d': 1}, {'f': 17}]}, {'$set': {'yf': 17}}, 'mixed')

# {$and:[{$and:[{a:1}]}], b:{$gt: 1}}


def create_upsert_selector_operator_test(operator, object, depth, update, operator_type):
    selector = next_selector = {}
    next_char = 'a'
    for i in range(0, depth):
        next_selector[next_char] = {}
        next_selector = next_selector[next_char]
        next_char = chr(ord(next_char) + 1)

    next_selector[operator] = object

    return ("Upsert operator (%s) depth %s" % (operator, depth), selector, update, operator_type)


def create_upsert_dotted_selector_operator_test(operator, object, depth, update, operator_type):
    selector = {}
    k = ['a']
    next_char = 'b'
    for i in range(0, depth):
        k.append(next_char)
        next_char = chr(ord(next_char) + 1)

    k_str = '.'.join(k)

    selector[k_str] = {}
    selector[k_str][operator] = object

    return ("Upsert operator (%s) depth %s (dotted selector)" % (operator, depth), selector, update, operator_type)


def create_upsert_dotted_selector_operator_test_with_operator_in_initial_position(operator, object, depth, update,
                                                                                  operator_type):
    selector = {}
    k = ['a']
    next_char = 'b'
    for i in range(0, depth):
        k.append(next_char)
        next_char = chr(ord(next_char) + 1)

    k_str = '.'.join(k)

    selector[operator] = {}
    selector[operator][k_str] = object

    return ("Upsert operator (%s) depth %s (dotted selector)" % (operator, depth), selector, update, operator_type)


def create_operator_tests(operators, depth, update, operator_type):
    return [
        func(operator, object, depth, update, operator_type) for name, func in globals().iteritems()
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
            tests.append(("Permutation (%s) depth %s repeat %s" % (ops, depth, repeat), util.OrderedDict(selector),
                          update, 'permutation'))
            next_selector[next_char] = {}
            next_selector = next_selector[next_char]
            next_char = chr(ord(next_char) + 1)
    return tests


def test(collection1, collection2, test):
    (label, selector, update, operator_type) = test

    sys.stdout.write('\tTesting \"%s\"... ' % label)

    update = util.deep_convert_to_ordered(update)

    collection1.remove()
    collection2.remove()

    def up(c, s, u):
        c.update(s, u, upsert=True, multi=False)

    errors = []
    ret1 = ret2 = []

    for c in (collection1, collection2):
        try:
            up(c, selector, update)
        except OperationFailure as e:
            errors.append("PyMongo upsert failed! Error was: " + str(e))
        except MongoModelException as e:
            errors.append("MongoModel upsert failed! Error was: " + str(e))

    if (len(errors)) == 1:
        print util.alert('FAIL', 'fail')
        print errors[0]
        return (False, [], [], operator_type)
    elif (len(errors)) == 2:
        print util.alert('PASS', 'okblue')
        return (True, [], [], operator_type)

    ret1 = [util.deep_convert_to_unordered(i) for i in collection1.find({})]
    ret1.sort()
    ret2 = [util.deep_convert_to_unordered(i) for i in collection2.find({})]
    ret2.sort()

    def error_report(msg):
        print util.indent(msg)
        print util.indent("Selector was: %s" % pprint(dict(selector)))
        print util.indent("Update was %s" % pprint(dict(update)))
        print util.indent("Upsert from collection1: %s" % ret1)
        print util.indent("Upsert from collection2: %s" % ret2)

    passed = True

    try:
        if len(ret1) + len(ret2) == 0:
            raise ValueError("NIL")
        for i in range(0, max(len(ret1), len(ret2))):
            try:
                del ret1[i]['_id']
            except:
                pass
            try:
                del ret2[i]['_id']
            except:
                pass
            assert ret1[i] == ret2[i]
        print util.alert('PASS', 'okgreen')
    except AssertionError:
        print util.alert('FAIL', 'fail')
        error_report("Upserts didn't match!")
        passed = False
    except IndexError:
        print util.alert('FAIL', 'fail')
        error_report("One or both upserts failed!")
        passed = False
    except ValueError as e:
        print util.alert("PASS (%s)" % str(e), 'okblue')

    return (passed, ret1, ret2, operator_type)


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
        }), ),
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

tests = [locals()[attr] for attr in dir() if attr.startswith('test_')]

oplist = []
for _, operators in operator_types.iteritems():
    for op in operators:
        oplist.append(op)

for depth in range(0, 5):
    for update in updates:
        for operator_type, operators in operator_types.iteritems():
            tests = tests + create_operator_tests(operators, depth, update, operator_type)
        for repeat in range(1, 2):
            tests = tests + create_operator_permutation_tests(oplist, depth, repeat, update)


def test_all(collection1, collection2, **kwargs):
    okay = True
    number_passed = number_failed = 0
    to_csv = kwargs.get("to_csv") or "upsert_test.csv"
    if to_csv:
        mf = open(to_csv, "a+")
        mf.truncate()
        mf.write('|'.join([
            'Operator Type', 'Passed?', 'Return 1', 'Return 2', 'Selector', 'Update', 'Label', 'Collection1',
            'Collection2'
        ]) + '\r\n')
    for t in tests:
        passed, r1, r2, operator_type = test(collection1, collection2, t)
        number_passed = number_passed + 1 if passed else number_passed
        number_failed = number_failed + 1 if not passed else number_failed
        okay = passed and okay
        if to_csv:
            mf.write("|".join([
                t[3],
                str(passed),
                str(r1),
                str(r2),
                str(dict(t[1])),
                str(dict(t[2])), t[0],
                str(collection1),
                str(collection2), '\r\n'
            ]))
    if to_csv:
        mf.close()
    print util.alert("Passed: %d, Failed: %d" % (number_passed, number_failed))
    return okay
