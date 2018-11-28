#
# core_tests.py
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

import sys
from collections import OrderedDict

import util
from gen import value_operators


def operator_queries(base_query):
    q_list = [base_query]
    q_list.extend([{base_query.keys()[0]: {operator: base_query.values()[0]}} for operator in value_operators])
    return q_list


def test_array1():
    return ("Array Test #1", {'a': [{'b': 1}]}, operator_queries({"a.b": 1}))


def test_array2():
    return ("Array Test #2", {'a': [3, 'b', {'b': 1}]}, operator_queries({"a.b": 1}))


def test_array3a():
    return ("Array Test #3a", {'_id': 'Bhaskar', 'a': [[1]]}, operator_queries({"a.0": 1}))


def test_array3b():
    return ("Array Test #3b", {'a': [{'0': 1}]}, operator_queries({"a.0": 1}))


def test_array3c():
    return ("Array Test #3c", {'a': [[{'b': 1}]]}, operator_queries({"a.0.b": 1}))


def test_array3d():
    return ("Array Test #3d", {'a': [['h', {'b': 1}]]}, operator_queries({"a.0.b": 1}))


def test_array3e():
    return ("Array Test #3e", {'a': ['h', [{'b': 1}]]}, operator_queries({"a.0.b": 1}))


def test_array4():
    return ("Array Test #4", {'a': [1, {'b': [2, {'c': 1}]}]}, operator_queries({"a.b.c": 1}))


def test_array5():
    return ("Array Test #5", {'a': [1, {'0': 2}]}, operator_queries({"a.0": 1}))


def test_array5a():
    return ("Array Test #5a", {'a': [1, {'0': 2}]}, operator_queries({"a": 1}))


def test_array5b():
    return ("Array Test #5b", {'a': [1, {'0': 2}]}, operator_queries({"a.0": 2}))


def test_array5c():
    return ("Array Test #5c", {
        'a': [1, {
            '0': 'hello',
            '1': [3, {
                'h': 'hello'
            }]
        }]
    }, operator_queries({
        'a.1.h': 'hello'
    }))


def test_array6():
    return ("Array Test #6", {'a': [2, {'0': 1}]}, operator_queries({"a.0": 1}))


def test_array7():
    return ("Array Test #7", {'a': [[1]]}, operator_queries({"a": 1}))


def test_array8():
    return ("Array Test #8", {'a': [2, {"1": ['e']}]}, operator_queries({"a.1": 'e'}))


def test_array9():
    return ("Array Test #9", {'a': [2, {"1": ['e']}]}, operator_queries({"a.1.0": 'e'}))


def test_array10():
    return ("Array Test #10", {u'B': [1, 2, {u'2': [1, 6]}]}, [{'B.2.1': {'$exists': True}}])


def test_array11():
    return ("Array Test #11", {
        u'2': [2, u'a', [], {
            u'1': u'd',
            u'2': {
                u'A': {
                    u'E': u'b',
                    u'D': u'c'
                }
            }
        }]
    }, [{
        "2.2.A": {
            "$exists": True
        }
    }])


def test_array12():
    return ("Array Test #12", {u'B': [1, 2, {u'2': [1, 6]}]}, [{'B.2.1': 6}])


def test_array13():
    return ("Array Test #13", {'E': [[2, 2, [1]]]}, [{'E.0.2': 1}])


def test_array14():
    return ("Array Test #14", {'E': [2, 2, []]}, [{'E': {'$ne': []}}])


def test_array16():
    return ("Array Test #16", {'E': [2, 2, [1]]}, [{'E': [1]}])


def test_array17():
    return ("Array Test #17", {'E': [2, 2, {'a': 'b', 'c': ['d', 'e']}]}, [{'E.2.c': 'e'}])


def test_null1():
    return ("Null Test #1", {"_id": 1, "A": [{"B": 5}]}, operator_queries({"A.B": None}))


def test_null2():
    return ("Null Test #2", {"_id": 1, "A": [{}]}, operator_queries({"A.B": None}))


def test_null3():
    return ("Null Test #3", {"_id": 1, "A": []}, operator_queries({"A.B": None}))


def test_null4():
    return ("Null Test #4", {"_id": 1, "A": [{}, {"B": 5}]}, operator_queries({"A.B": None}))


def test_null5():
    return ("Null Test #5", {"_id": 1, "A": [5, {"B": 5}]}, operator_queries({"A.B": None}))


def test_null6():
    return ("Null Test #6", {'2': ['A', [{}, u'a', 0]]}, operator_queries({"2.1.D": None}))


def test_dict1():
    d = OrderedDict()
    d['C'] = 'c'
    d['D'] = 'd'
    return ("Dict Test #1", {'_id': 'A', 'B': d}, [{'B': d}])


def test_dict2():
    d = OrderedDict()
    d['C'] = 'c'
    d['D'] = 'd'
    e = OrderedDict()
    e['C'] = 'c'
    e['D'] = 'e'
    return ("Dict Test #2", {'_id': 'A', 'B': d}, [{'B': e}])


def test_dict3():
    d = OrderedDict()
    d['C'] = 'c'
    d['D'] = 'd'
    e = OrderedDict()
    e['D'] = 'd'
    e['C'] = 'c'
    return ("Dict Test #2", {'_id': 'A', 'B': d}, [{'B': e}])


tests = [globals()[f] for f in dir() if f.startswith("test_")]


def test(collection1, collection2, test):
    collection1.remove()
    collection2.remove()

    (info, doc, queries) = test()

    sys.stdout.write("Testing %s..." % info)

    doc = util.deep_convert_to_ordered(doc)

    collection1.insert(doc)
    collection2.insert(doc)

    for query in queries:

        ret1 = [util.deep_convert_to_unordered(i) for i in collection1.find(query)]
        ret1.sort()
        ret2 = [util.deep_convert_to_unordered(i) for i in collection2.find(query)]
        ret2.sort()

        try:
            for i in range(0, max(len(ret1), len(ret2))):
                assert ret1[i] == ret2[i]
            print util.alert('PASS', 'okgreen')
            return True
        except AssertionError:
            print util.alert('FAIL', 'fail')
            print "Query results didn't match!"
            print query
            print ret1[i]
            print ret2[i]
            print len(ret1)
            print len(ret2)
            return False
        except IndexError:
            print util.alert('FAIL', 'fail')
            print "Query results didn't match!"
            print query
            print len(ret1)
            print len(ret2)
            if len(ret1) < len(ret2):
                print ret2[i]
            else:
                print ret1[i]
            return False
    return True


def non_deterministic_query_test():
    # =====sort fields=====input data=====result=====expected
    data = [
        ([('x', 1)], 0, 0, {}, ['{"x":1}', '{"x":2}'], ['{"x":1}', '{"x":2}'], True),
        ([('x', -1)], 0, 0, {}, ['{"x":1}', '{"x":2}'], ['{"x":2}', '{"x":1}'], True),
        ([('x', 1)], 0, 0, {}, ['{"x":3}', '{"x":1}', '{"x":2}'], ['{"x":1}', '{"x":2}', '{"x":3}'], True),
        ([('x', 1)], 0, 0, {}, ['{"a":3}', '{"x":1}', '{"x":2}'], ['{"a":3}', '{"x":1}', '{"x":2}'], True),
        ([('x', -1)], 0, 0, {}, ['{"a":3}', '{"x":1}', '{"x":2}'], ['{"x":2}', '{"x":1}', '{"a":3}'], True),
        ([('x', 1)], 0, 0, {}, ['{"a":3}', '{"x":1, "a":1}', '{"x":1, "a":2}'],
         ['{"a":3}', '{"x":1, "a":2}', '{"x":1, "a":1}'], True),
        ([('x', 1)], 0, 0, {}, ['{"x":1, "a":1}', '{"x":1, "a":2}', '{"x":1, "a":2}'],
         ['{"x":1, "a":2}', '{"x":1, "a":1}', '{"x":1, "a":1}'], False),

        # limit & skip
        ([('x', 1)], 1, 0, {}, ['{"x":1}', '{"x":2}'], ['{"x":1}'], True),
        ([('x', 1)], 1, 0, {}, ['{"x":1}', '{"x":2}'], ['{"x":2}'], False),
        ([('x', 1)], 0, 1, {}, ['{"x":1}', '{"x":2}'], ['{"x":2}'], True),
        ([('x', 1)], 0, 1, {}, ['{"x":1}', '{"x":2}'], ['{"x":1}'], False),
        ([('x', 1)], 0, 5, {}, ['{"x":1}', '{"x":2}'], [], True),
        ([('x', 1)], 1, 1, {}, ['{"x":1}', '{"x":2}', '{"x":3}'], ['{"x":2}'], True),

        # array
        ([('x', 1)], 0, 0, {}, ['{"a":3}', '{"x":1, "a":1}', '{"x":[2,1], "a":2}'],
         ['{"a":3}', '{"x":1, "a":1}', '{"x":[2,1], "a":2}'], True),
        ([('x', 1)], 0, 0, {}, ['{"a":3}', '{"x":1, "a":1}', '{"x":[2,1], "a":2}'],
         ['{"a":3}', '{"x":[2,1], "a":2}', '{"x":1, "a":1}'], True),
        ([('x', 1)], 0, 0, {}, ['{"a":3}', '{"x":1, "a":1}', '{"x":[2,1], "a":2}'],
         ['{"a":3}', '{"x":1, "a":1}', '{"x":[2,1], "a":2}'], True),
        ([('x', -1)], 0, 0, {}, ['{"a":3}', '{"x":1, "a":1}', '{"x":[2,1], "a":2}'],
         ['{"x":1, "a":1}', '{"x":[2,1], "a":2}', '{"a":3}'], False),
        ([('x', -1)], 0, 0, {}, ['{"a":3}', '{"x":1, "a":1}', '{"x":[2,1], "a":2}'],
         ['{"x":[2,1], "a":2}', '{"x":1, "a":1}', '{"a":3}'], True),
        ([('a', 1)], 0, 0, {}, ['{"a":3}', '{"a":[1,4]}', '{"a":1}', '{"a":[1]}', '{"a":[4,1]}'], [
            '{"a":[1,4]}',
            '{"a":[4,1]}',
            '{"a":1}',
            '{"a":[1]}',
            '{"a":3}',
        ], True),
        ([('a', -1)], 0, 0, {}, ['{"a":3}', '{"a":[1,4]}', '{"a":1}', '{"a":[1]}', '{"a":[4,1]}'], [
            '{"a":3}',
            '{"a":[1,4]}',
            '{"a":[4,1]}',
            '{"a":1}',
            '{"a":[1]}',
        ], False),
        ([('a', -1)], 0, 0, {}, ['{"a":3}', '{"a":[1,4]}', '{"a":1}', '{"a":[1]}', '{"a":[4,1]}'], [
            '{"a":[4,1]}',
            '{"a":[1,4]}',
            '{"a":3}',
            '{"a":[1]}',
            '{"a":1}',
        ], True),
        ([('a', 1)], 0, 0, {}, ['{"a":3}', '{"a":[1,4]}', '{"a":[4,1]}', '{"a":1}', '{"a":[1]}', '{"b":[1]}'], [
            '{"b":[1]}',
            '{"a":[1,4]}',
            '{"a":[4,1]}',
            '{"a":1}',
            '{"a":[1]}',
            '{"a":3}',
        ], True),

        # null, [] and {}
        ([('a', 1)], 0, 0, {}, [
            '{"a":3}', '{"a":{}}', '{"a":[]}', '{"a":[1,4]}', '{"a":[4,1]}', '{"a":1}', '{"a":null}', '{"a":[1]}',
            '{"b":[1]}'
        ], [
            '{"a":[]}', '{"a":null}', '{"b":[1]}', '{"a":[1,4]}', '{"a":[4,1]}', '{"a":1}', '{"a":[1]}', '{"a":3}',
            '{"a":{}}'
        ], True),

        # two sort fields
        ([('a', 1), ('b', 1)], 0, 0, {}, ['{"a":1, "b":1}', '{"a":1, "b":3}', '{"a":1, "b":2}'],
         ['{"a":1, "b":1}', '{"a":1, "b":2}', '{"a":1, "b":3}'], True),
        ([('a', 1), ('b', 1)], 0, 0, {},
         ['{"a":2, "b":1}', '{"a":2, "b":3}', '{"a":2, "b":2}', '{"a":1, "b":6}', '{"a":1, "b":5}', '{"a":1, "b":4}'],
         ['{"a":1, "b":4}', '{"a":1, "b":5}', '{"a":1, "b":6}', '{"a":2, "b":1}', '{"a":2, "b":2}',
          '{"a":2, "b":3}'], True),
        ([('a', 1), ('b', -1)], 0, 0, {},
         ['{"a":2, "b":1}', '{"a":2, "b":3}', '{"a":2, "b":2}', '{"a":1, "b":6}', '{"a":1, "b":5}', '{"a":1, "b":4}'],
         ['{"a":1, "b":6}', '{"a":1, "b":5}', '{"a":1, "b":4}', '{"a":2, "b":3}', '{"a":2, "b":2}',
          '{"a":2, "b":1}'], True),

        # with sort path
        ([('x.a', 1)], 0, 0, {}, ['{"x":{"a":3}}', '{"x":{"a":1}}', '{"x":{"a":2}}'],
         ['{"x":{"a":1}}', '{"x":{"a":2}}', '{"x":{"a":3}}'], True),
        ([('x.a', 1)], 0, 0, {}, ['{"x":{"a":3}}', '{"x":{"a":1}}', '{"x":{"a":null}}'],
         ['{"x":{"a":null}}', '{"x":{"a":1}}', '{"x":{"a":3}}'], True),
        ([('x.a', 1)], 0, 0, {}, ['{"x":{"a":3}}', '{"x":{"b":1}}', '{"x":{"a":null}}'],
         ['{"x":{"a":null}}', '{"x":{"b":1}}', '{"x":{"a":3}}'], True),
        ([('a.b', 1)], 0, 0, {}, ['{"a":[{"b":3}]}', '{"a":[{"b":[1,6]}]}'], ['{"a":[{"b":[1,6]}]}', '{"a":[{"b":3}]}'],
         True),
        ([('a.b', 1)], 0, 0, {}, ['{"a":[{"b":3}]}', '{"a":[{"b":[1,6]}]}'], ['{"a":[{"b":3}]}', '{"a":[{"b":[1,6]}]}'],
         False),
        ([('a.b', 1)], 0, 0, {}, ['{"a":[{"b":5}]}', '{"a":[{"b":[2,1]}]}'], ['{"a":[{"b":[2,1]}]}', '{"a":[{"b":5}]}'],
         True),
        ([('a.b', 1)], 0, 0, {}, ['{"a":[{"b":5}, {"b":null}]}', '{"a":[{"b":[2,1]}]}'],
         ['{"a":[{"b":5}, {"b":null}]}', '{"a":[{"b":[2,1]}]}'], True),
        ([('a.b', 1)], 0, 0, {}, ['{"a":[{"b":3},4]}', '{"a":[{"b":[1,6]}]}'],
         ['{"a":[{"b":3},4]}', '{"a":[{"b":[1,6]}]}'], True),
        ([('a.b', 1)], 0, 0, {}, ['{"a":[{"b":3},4]}', '{"a":[{"b":[1,6]}]}'],
         ['{"a":[{"b":[1,6]}]}', '{"a":[{"b":3},4]}'], False),
        ([('a.b', 1)], 0, 0, {}, ['{"a":[{"b":6},4]}', '{"a":[{"b":[1,3]}]}'],
         ['{"a":[{"b":6},4]}', '{"a":[{"b":[1,3]}]}'], True),
        ([('a.b', -1)], 0, 0, {}, ['{"a":[{"b":6},4]}', '{"a":[{"b":[1,3]}]}'],
         ['{"a":[{"b":6},4]}', '{"a":[{"b":[1,3]}]}'], True),
        ([('a.b', 1)], 0, 0, {}, ['{"a":[{"b":[3,4]}]}', '{"a":[{"b":[1,2]}]}'],
         ['{"a":[{"b":[1,2]}]}', '{"a":[{"b":[3,4]}]}'], True),
        ([('a.b', -1)], 0, 0, {}, ['{"a":[{"b":[3,4]}]}', '{"a":[{"b":[1,2]}]}'],
         ['{"a":[{"b":[3,4]}]}', '{"a":[{"b":[1,2]}]}'], True),

        # mongoDB special cases that not pass the test
        ([('2.D', 1), (2, -1)], 0, 0, {}, ['{"2": "e"}', '{"2": ["c", {"0": "e", "C": -68, "D": "a"}]}', '{"2": "c"}'],
         ['{"2": "e"}', '{"2": ["c", {"0": "e", "C": -68, "D": "a"}]}', '{"2": "c"}'], True),
    ]

    try:
        for idx, test_item in enumerate(data):
            print "\n========== test", idx, "=========="
            (sort, limit, skip, query, input_data, result, expected) = test_item
            input_data = util.generate_list_of_ordered_dict_from_json(input_data)
            result = util.generate_list_of_ordered_dict_from_json(result)

            nd_list = util.MongoModelNondeterministicList(input_data, sort, limit, skip, query)

            # print nd_list
            if nd_list.compare(result) != expected:
                assert False
            elif not expected:
                util.trace('error', "Result doesn't match but this is what we expected!")

    except AssertionError:
        print nd_list
        util.trace('error', "nonDeterministicQueryTest results didn't match!")
        return False

    return True


def is_list_subset_test():
    # =====sort fields=====input data=====result=====expected
    data = [
        (['{"x":1}', '{"x":2}'], ['{"x":1}', '{"x":2}'], True),
        (['{"x":1}', '{"x":2}'], ['{"x":2}'], True),
        (['{"x":1}', '{"x":2}'], ['{"x":3}'], False),
        (['{"x":1}', '{"x":2}'], ['{"x":2}', '{"x":2}'], False),
    ]

    try:
        for idx, test_item in enumerate(data):
            print "\n========== isListSubset test", idx, "=========="

            (src_list, sub_list, expected) = test_item
            if util.is_list_subset(src_list, sub_list) != expected:
                assert False
            elif not expected:
                util.trace('error', "Result doesn't match but this is what we expected!")

    except AssertionError:
        print "src_list:", src_list, "sub_list:", sub_list, "expected:", expected
        util.trace('error', "isListSubset() results didn't match!")
        return False

    return True


def is_ambiguous_field_name_in_array_test():
    # =====input=====path=====ambiguous
    data = [
        (['{"a":[2,{"1":3}]}'], "a.1", True),
        (['{"a":[2,{"1":3}]}'], "a.2", False),
        (['{"a":[2,{"1":3}]}'], "a.1.1", True),
        (['{"a":[2,{"1":3}]}'], "a.1.1.1", True),
        (['{"a":{"1":[4,6,{"1":2}]}}'], "a.1.1", True),
    ]

    for idx, test_item in enumerate(data):
        print "\n========== isAmbiguousFieldNameInArray test", idx, "=========="
        (src_list, path, ambiguous) = test_item
        src_list = util.generate_list_of_ordered_dict_from_json(src_list)
        for obj in src_list:
            try:
                util.check_ambiguous_array(obj, path)
                if ambiguous:
                    util.trace('error', "isAmbiguousFieldNameInArray result didn't match!")
                    return False
            except util.MongoModelException as e:
                if not ambiguous:
                    util.trace('error', "isAmbiguousFieldNameInArray result didn't match!")
                    return False
    return True


def test_all(collection1, collection2):
    okay = True
    for t in tests:
        okay = test(collection1, collection2, t) and okay

    #   # FIXME: These unit tests were added later, and don't appear to fit the conventions of this file.
    # They're also quite spammy.
    #   if not nonDeterministicQueryTest():
    #       okay = False
    #       return okay
    #   if not isListSubsetTest():
    #       okay = False
    #       return okay
    #   if not isAmbiguousFieldNameInArrayTest():
    #       okay = False
    #       return okay
    return okay
