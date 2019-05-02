#
# test_planner.py
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


class Predicates(object):
    @staticmethod
    def no_table_scan(explanation):
        this_type = explanation['type']
        if this_type == 'table scan':
            return False
        elif this_type == 'union':
            return all([Predicates.no_table_scan(p) for p in explanation['plans']])
        elif 'source_plan' in explanation:
            return Predicates.no_table_scan(explanation['source_plan'])
        else:
            return True

    @staticmethod
    def only_index_named(index_name, explanation):
        this_type = explanation['type']
        if this_type == 'index scan' and explanation['index name'] == index_name:
            return True
        elif this_type == 'union':
            return all([Predicates.only_index_named(index_name, p) for p in explanation['plans']])
        elif 'source_plan' in explanation:
            return Predicates.only_index_named(index_name, explanation['source_plan'])
        else:
            return False

    @staticmethod
    def pk_lookup(explanation):
        this_type = explanation['type']
        if this_type == 'PK lookup':
            return True
        elif this_type == 'union':
            return all([Predicates.pk_lookup(p) for p in explanation['plans']])
        elif 'source_plan' in explanation:
            return Predicates.pk_lookup(explanation['source_plan'])
        else:
            return False

    @staticmethod
    def no_filter(explanation):
        this_type = explanation['type']
        if this_type == 'filter':
            return False
        elif this_type == 'union':
            return all([Predicates.no_filter(p) for p in explanation['plans']])
        elif 'source_plan' in explanation:
            return Predicates.no_filter(explanation['source_plan'])
        else:
            return True

    @staticmethod
    def pk_lookup_no_filter(explanation):
        return Predicates.pk_lookup(explanation) and Predicates.no_filter(explanation)

    @staticmethod
    def no_table_scan_no_filter(explanation):
        return Predicates.no_table_scan(explanation) and Predicates.no_filter(explanation)


# PK Lookup Tests
def test_pk_lookup(fixture_collection):
    ret = fixture_collection.find({'_id': 1}).explain()
    assert Predicates.pk_lookup_no_filter(ret['explanation'])


def test_pk_scan(fixture_collection):
    ret = fixture_collection.find({'_id': {'$gt': 1}}).explain()
    assert Predicates.pk_lookup(ret['explanation'])


# Simple Index Tests
def test_simple_index_basic(fixture_collection):
    fixture_collection.create_index(keys=[('a', 1)], name='index')
    query = {'a': 1}
    ret = fixture_collection.find(query).explain()
    assert Predicates.no_table_scan_no_filter(ret['explanation'])


def test_simple_index_union(fixture_collection):
    fixture_collection.create_index(keys=[('a', 1)], name='index1')
    fixture_collection.create_index(keys=[('b', 1)], name='index2')
    query = {
        '$or': [{
            "a": 1
        }, {
            "b": 1
        }]
    }
    ret = fixture_collection.find(query).explain()
    assert Predicates.no_table_scan(ret['explanation'])


def test_simple_index_compound_as_simple(fixture_collection):
    fixture_collection.create_index(keys=[('a', 1), ('b', 1)], name='compound')
    query = {'a': 1}
    ret = fixture_collection.find(query).explain()
    assert Predicates.no_table_scan_no_filter(ret['explanation'])


def test_simple_index_prefer_simple_over_compound(fixture_collection):
    fixture_collection.create_index(keys=[('a', 1), ('b', 1)], name='compound')
    fixture_collection.create_index(keys=[('a', 1)], name='simple')
    query = {'a': 1}
    ret = fixture_collection.find(query).explain()
    assert Predicates.only_index_named('simple', ret['explanation'])


def test_simple_index_prefer_shorter_compound_index(fixture_collection):
    fixture_collection.create_index(keys=[('a', 1), ('b', 1), ('c', 1)], name='long')
    fixture_collection.create_index(keys=[('a', 1), ('b', 1)], name='short')
    query = {'a': 1}
    ret = fixture_collection.find(query).explain()
    assert Predicates.only_index_named('short', ret['explanation'])


def test_simple_index_greedy_plan(fixture_collection):
    fixture_collection.create_index(keys=[('a', 1)], name='one')
    fixture_collection.create_index(keys=[('b', 1)], name='two')
    query = {
        '$and': [{
            'b': 1
        }, {
            'a': 1
        }]
    }
    ret = fixture_collection.find(query).explain()
    assert Predicates.only_index_named('two', ret['explanation'])


def test_simple_index_scan(fixture_collection):
    fixture_collection.create_index(keys=[('a', 1)], name='index')
    query = {
        "a": {
            '$gt': 1
        }
    }
    ret = fixture_collection.find(query).explain()
    assert Predicates.no_table_scan(ret['explanation'])


def test_simple_index_multi_union(fixture_collection):
    fixture_collection.create_index(keys=[('d', 1)], name='index1')
    fixture_collection.create_index(keys=[('c', 1)], name='index2')
    fixture_collection.create_index(keys=[('b', 1)], name='index3')
    fixture_collection.create_index(keys=[('a', 1)], name='index4')
    query = {
        '$or': [{
            "a": {
                '$gt': 1
            }
        }, {
            "b": 1
        }, {
            "c": {
                '$lte': 1
            }
        }, {
            "d": 1
        }]
    }
    ret = fixture_collection.find(query).explain()
    assert Predicates.no_table_scan(ret['explanation'])


def test_simple_index_dotted_path(fixture_collection):
    fixture_collection.create_index(keys=[('a.b', 1)], name='simple')
    query = {'a.b': 'hello'}
    ret = fixture_collection.find(query).explain()
    assert Predicates.no_table_scan(ret['explanation'])


# Compound Index Tests
def test_compound_index_basic(fixture_collection):
    fixture_collection.create_index(keys=[('a', 1), ('b', 1)], name='compound')
    query = {
        '$and': [{
            'a': 1
        }, {
            'b': 1
        }]
    }
    ret = fixture_collection.find(query).explain()
    assert Predicates.no_table_scan_no_filter(ret['explanation'])


def test_compound_index_out_of_order(fixture_collection):
    fixture_collection.create_index(keys=[('a', 1), ('b', 1)], name='compound')
    query = {
        '$and': [{
            'b': 1
        }, {
            'a': 1
        }]
    }
    ret = fixture_collection.find(query).explain()
    assert Predicates.no_table_scan(ret['explanation'])


def test_compound_index_find_long_index(fixture_collection):
    fixture_collection.create_index(keys=[('a', 1), ('b', 1), ('c', 1), ('d', 1), ('e', 1), ('f', 1), ('g', 1)], name='long')
    query = {
        '$and': [{
            'g': 1
        }, {
            'f': 1
        }, {
            'e': 1
        }, {
            'd': 1
        }, {
            'c': 1
        }, {
            'b': 1
        }, {
            'a': 1
        }]
    }
    ret = fixture_collection.find(query).explain()
    assert Predicates.no_table_scan(ret['explanation'])


def test_compound_index_union(fixture_collection):
    fixture_collection.create_index(keys=[('a', 1), ('b', 1)], name='compound')
    fixture_collection.create_index(keys=[('d', 1), ('c', 1)], name='compound2')
    query = {
        '$or': [{
            '$and': [{
                'b': 1
            }, {
                'a': 1
            }]
        }, {
            '$and': [{
                'd': 1
            }, {
                'c': 1
            }]
        }]
    }
    ret = fixture_collection.find(query).explain()
    assert Predicates.no_table_scan(ret['explanation'])


def test_compound_index_range_at_end(fixture_collection):
    fixture_collection.create_index(keys=[('d', 1), ('b', 1), ('c', 1)], name='compound')
    fixture_collection.create_index(keys=[('d', 1)], name='simple')
    query = {
        '$and': [{
            'c': {
                '$gt': 1
            }
        }, {
            'b': 1
        }, {
            'd': 1
        }]
    }
    ret = fixture_collection.find(query).explain()
    assert Predicates.only_index_named("compound", ret['explanation'])


def test_compound_index_range_at_start(fixture_collection):
    fixture_collection.create_index(keys=[('d', 1), ('b', 1), ('c', 1)], name='compound')
    fixture_collection.create_index(keys=[('d', 1)], name='simple')
    query = {
        '$and': [{
            'd': {
                '$gt': 1
            }
        }, {
            'b': 1
        }, {
            'c': 1
        }]
    }
    ret = fixture_collection.find(query).explain()
    assert Predicates.only_index_named("simple", ret['explanation'])


def test_compound_index_range_at_middle(fixture_collection):
    fixture_collection.create_index(keys=[('d', 1), ('b', 1), ('c', 1)], name='compound')
    fixture_collection.create_index(keys=[('d', 1)], name='simple')
    query = {
        '$and': [{
            'b': {
                '$gt': 1
            }
        }, {
            'd': 1
        }, {
            'c': 1
        }]
    }
    ret = fixture_collection.find(query).explain()
    assert Predicates.only_index_named("compound", ret['explanation'])


def test_compound_index_greedy_planner(fixture_collection):
    fixture_collection.create_index(keys=[('d', 1), ('b', 1), ('c', 1)], name='long')
    fixture_collection.create_index(keys=[('b', 1), ('c', 1)], name='short')
    query = {
        '$and': [{
            'b': 1
        }, {
            'd': 1
        }, {
            'c': 1
        }]
    }
    ret = fixture_collection.find(query).explain()
    assert Predicates.only_index_named("short", ret['explanation'])


def test_compound_index_match_exact_index(fixture_collection):
    fixture_collection.create_index(keys=[('a', 1)], name='a')
    fixture_collection.create_index(keys=[('a', 1), ('b', 1)], name='ab')
    fixture_collection.create_index(keys=[('a', 1), ('b', 1), ('c', 1)], name='abc')
    fixture_collection.create_index(keys=[('a', 1), ('b', 1), ('c', 1), ('d', 1), ('e', 1)], name='abcde')
    query = {
        '$and': [{
            'a': 1
        }, {
            'b': 1
        }, {
            'c': 1
        }]
    }
    ret = fixture_collection.find(query).explain()
    assert Predicates.only_index_named("abc", ret['explanation']) and Predicates.no_table_scan_no_filter(ret['explanation'])


def test_compound_index_match_longest_prefix(fixture_collection):
    fixture_collection.create_index(keys=[('a', 1)], name='a')
    fixture_collection.create_index(keys=[('a', 1), ('b', 1)], name='ab')
    fixture_collection.create_index(keys=[('a', 1), ('b', 1), ('c', 1)], name='abc')
    fixture_collection.create_index(keys=[('a', 1), ('b', 1), ('c', 1), ('d', 1), ('e', 1)], name='abcde')
    query = {
        '$and': [{
            'a': 1
        }, {
            'b': 1
        }, {
            'c': 1
        }, {
            'd': 1
        }]
    }
    ret = fixture_collection.find(query).explain()
    assert Predicates.only_index_named("abcde", ret['explanation']) and Predicates.no_table_scan_no_filter(ret['explanation'])


def test_compound_index_match_non_contiguous_prefix(fixture_collection):
    fixture_collection.create_index(keys=[('a', 1)], name='a')
    fixture_collection.create_index(keys=[('a', 1), ('b', 1)], name='ab')
    fixture_collection.create_index(keys=[('a', 1), ('b', 1), ('c', 1)], name='abc')
    fixture_collection.create_index(keys=[('a', 1), ('b', 1), ('c', 1), ('d', 1), ('e', 1)], name='abcde')
    query = {
        '$and': [{
            'a': 1
        }, {
            'b': 1
        }, {
            'c': 1
        }, {
            'e': 1
        }]
    }
    ret = fixture_collection.find(query).explain()
    assert Predicates.only_index_named("abc", ret['explanation']) and Predicates.no_table_scan(ret['explanation'])


def test_compound_index_multi_match(fixture_collection):
    fixture_collection.create_index(keys=[('a', 1), ('b', 1), ('c', 1), ('d', 1)], name='abcd')
    fixture_collection.create_index(keys=[('a', 1), ('e', 1)], name='ae')
    fixture_collection.create_index(keys=[('a', 1), ('d', 1)], name='ad')
    fixture_collection.create_index(keys=[('d', 1), ('a', 1)], name='da')
    fixture_collection.create_index(keys=[('e', 1)], name='e')

    query = {
        '$and': [{
            'a': 1
        }, {
            'd': 1
        }]
    }
    ret = fixture_collection.find(query).explain()
    assert Predicates.only_index_named('ad', ret['explanation'])
    assert Predicates.no_table_scan(ret['explanation'])
    assert Predicates.no_filter(ret['explanation'])

    query = {
        '$and': [{
            'a': 1
        }, {
            'd': 1
        }, {
            'e': 1
        }]
    }
    ret = fixture_collection.find(query).explain()
    assert Predicates.only_index_named('ad', ret['explanation'])
    assert Predicates.no_table_scan(ret['explanation'])

    query = {
        '$and': [{
            'd': 1
        }, {
            'a': 1
        }]
    }
    ret = fixture_collection.find(query).explain()
    assert Predicates.only_index_named('da', ret['explanation'])
    assert Predicates.no_table_scan(ret['explanation'])
    assert Predicates.no_filter(ret['explanation'])

    query = {'d': 1}
    ret = fixture_collection.find(query).explain()
    assert Predicates.only_index_named('da', ret['explanation'])
    assert Predicates.no_table_scan(ret['explanation'])
