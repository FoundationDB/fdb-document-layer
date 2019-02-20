#
# test_core.py
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

from collections import OrderedDict

import pytest

import util
from gen import value_operators
from mongo_model import MongoModel


def operator_queries(base_query):
    q_list = [base_query]
    q_list.extend([{base_query.keys()[0]: {operator: base_query.values()[0]}} for operator in value_operators])
    return q_list


def run_and_match(dl_collection, test):
    mm = MongoModel('DocLayer')
    mm_collection = mm['test']['test']

    dl_collection.delete_many({})

    (doc, queries) = test

    doc = util.deep_convert_to_ordered(doc)

    mm_collection.insert_one(doc)
    dl_collection.insert_one(doc)

    for query in queries:
        ret1 = [util.deep_convert_to_unordered(i) for i in mm_collection.find(query)]
        ret1.sort()
        ret2 = [util.deep_convert_to_unordered(i) for i in dl_collection.find(query)]
        ret2.sort()

        assert len(ret1) == len(ret2), "Number returned docs don't match, mm: {}, dl: {}".format(len(ret1), len(ret2))
        for i in range(0, len(ret1)):
            assert ret1[i] == ret2[i], "Mismatch at {}, mm: {}, dl: {}".format(i, ret1[i], ret2[i])


def test_array1(fixture_collection):
    run_and_match(fixture_collection, ({'a': [{'b': 1}]}, operator_queries({"a.b": 1})))


def test_array2(fixture_collection):
    run_and_match(fixture_collection, ({'a': [3, 'b', {'b': 1}]}, operator_queries({"a.b": 1})))


def test_array3a(fixture_collection):
    run_and_match(fixture_collection, ({'_id': 'Bhaskar', 'a': [[1]]}, operator_queries({"a.0": 1})))


def test_array3b(fixture_collection):
    run_and_match(fixture_collection, ({'a': [{'0': 1}]}, operator_queries({"a.0": 1})))


def test_array3c(fixture_collection):
    run_and_match(fixture_collection, ({'a': [[{'b': 1}]]}, operator_queries({"a.0.b": 1})))


def test_array3d(fixture_collection):
    run_and_match(fixture_collection, ({'a': [['h', {'b': 1}]]}, operator_queries({"a.0.b": 1})))


def test_array3e(fixture_collection):
    run_and_match(fixture_collection, ({'a': ['h', [{'b': 1}]]}, operator_queries({"a.0.b": 1})))


def test_array4(fixture_collection):
    run_and_match(fixture_collection, ({'a': [1, {'b': [2, {'c': 1}]}]}, operator_queries({"a.b.c": 1})))


def test_array5(fixture_collection):
    run_and_match(fixture_collection, ({'a': [1, {'0': 2}]}, operator_queries({"a.0": 1})))


def test_array5a(fixture_collection):
    run_and_match(fixture_collection, ({'a': [1, {'0': 2}]}, operator_queries({"a": 1})))


def test_array5b(fixture_collection):
    run_and_match(fixture_collection, ({'a': [1, {'0': 2}]}, operator_queries({"a.0": 2})))


def test_array5c(fixture_collection):
    run_and_match(fixture_collection, ({
        'a': [1, {
            '0': 'hello',
            '1': [3, {
                'h': 'hello'
            }]
        }]
    }, operator_queries({
        'a.1.h': 'hello'
    })))


def test_array6(fixture_collection):
    run_and_match(fixture_collection, ({'a': [2, {'0': 1}]}, operator_queries({"a.0": 1})))


def test_array7(fixture_collection):
    run_and_match(fixture_collection, ({'a': [[1]]}, operator_queries({"a": 1})))


def test_array8(fixture_collection):
    run_and_match(fixture_collection, ({'a': [2, {"1": ['e']}]}, operator_queries({"a.1": 'e'})))


def test_array9(fixture_collection):
    run_and_match(fixture_collection, ({'a': [2, {"1": ['e']}]}, operator_queries({"a.1.0": 'e'})))


def test_array10(fixture_collection):
    run_and_match(fixture_collection, ({u'B': [1, 2, {u'2': [1, 6]}]}, [{'B.2.1': {'$exists': True}}]))


def test_array11(fixture_collection):
    run_and_match(fixture_collection, ({
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
    }]))


def test_array12(fixture_collection):
    run_and_match(fixture_collection, ({u'B': [1, 2, {u'2': [1, 6]}]}, [{'B.2.1': 6}]))


def test_array13(fixture_collection):
    run_and_match(fixture_collection, ({'E': [[2, 2, [1]]]}, [{'E.0.2': 1}]))


def test_array14(fixture_collection):
    run_and_match(fixture_collection, ({'E': [2, 2, []]}, [{'E': {'$ne': []}}]))


def test_array16(fixture_collection):
    run_and_match(fixture_collection, ({'E': [2, 2, [1]]}, [{'E': [1]}]))


def test_array17(fixture_collection):
    run_and_match(fixture_collection, ({'E': [2, 2, {'a': 'b', 'c': ['d', 'e']}]}, [{'E.2.c': 'e'}]))


def test_null1(fixture_collection):
    run_and_match(fixture_collection, ({"_id": 1, "A": [{"B": 5}]}, operator_queries({"A.B": None})))


def test_null2(fixture_collection):
    run_and_match(fixture_collection, ({"_id": 1, "A": [{}]}, operator_queries({"A.B": None})))


def test_null3(fixture_collection):
    run_and_match(fixture_collection, ({"_id": 1, "A": []}, operator_queries({"A.B": None})))


def test_null4(fixture_collection):
    run_and_match(fixture_collection, ({"_id": 1, "A": [{}, {"B": 5}]}, operator_queries({"A.B": None})))


def test_null5(fixture_collection):
    run_and_match(fixture_collection, ({"_id": 1, "A": [5, {"B": 5}]}, operator_queries({"A.B": None})))


def test_null6(fixture_collection):
    run_and_match(fixture_collection, ({'2': ['A', [{}, u'a', 0]]}, operator_queries({"2.1.D": None})))


def test_dict1(fixture_collection):
    d = OrderedDict()
    d['C'] = 'c'
    d['D'] = 'd'
    run_and_match(fixture_collection, ({'_id': 'A', 'B': d}, [{'B': d}]))


def test_dict2(fixture_collection):
    d = OrderedDict()
    d['C'] = 'c'
    d['D'] = 'd'
    e = OrderedDict()
    e['C'] = 'c'
    e['D'] = 'e'
    run_and_match(fixture_collection, ({'_id': 'A', 'B': d}, [{'B': e}]))


def test_dict3(fixture_collection):
    d = OrderedDict()
    d['C'] = 'c'
    d['D'] = 'd'
    e = OrderedDict()
    e['D'] = 'd'
    e['C'] = 'c'
    run_and_match(fixture_collection, ({'_id': 'A', 'B': d}, [{'B': e}]))


@pytest.mark.skip
def test_non_deterministic_query():
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

    for idx, test_item in enumerate(data):
        print "\n========== test", idx, "=========="
        (sort, limit, skip, query, input_data, result, expected) = test_item
        input_data = util.generate_list_of_ordered_dict_from_json(input_data)
        result = util.generate_list_of_ordered_dict_from_json(result)

        nd_list = util.MongoModelNondeterministicList(input_data, sort, limit, skip, query)

        # print nd_list
        assert nd_list.compare(result) == expected, str(nd_list)


def test_is_list_subset():
    # =====sort fields=====input data=====result=====expected
    data = [
        (['{"x":1}', '{"x":2}'], ['{"x":1}', '{"x":2}'], True),
        (['{"x":1}', '{"x":2}'], ['{"x":2}'], True),
        (['{"x":1}', '{"x":2}'], ['{"x":3}'], False),
        (['{"x":1}', '{"x":2}'], ['{"x":2}', '{"x":2}'], False),
    ]

    for idx, test_item in enumerate(data):
        (src_list, sub_list, expected) = test_item
        assert util.is_list_subset(src_list, sub_list) == expected,\
            "src_list: {}, sub_listL: {}, expected: {}".format(src_list, sub_list, expected)


def test_is_ambiguous_field_name_in_array():
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
            if ambiguous:
                with pytest.raises(util.MongoModelException):
                    util.check_ambiguous_array(obj, path)
            else:
                util.check_ambiguous_array(obj, path)
