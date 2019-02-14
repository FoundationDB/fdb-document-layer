#
# planner_tests.py
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

import sys
import util


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


class Index(object):
    def __init__(self, name, keys):
        self.name = name
        self.keys = keys


# PK Lookup Tests
def test_pk_1():
    return ("PK Lookup", [], {"_id": 1}, Predicates.pk_lookup_no_filter)


def test_pk_2():
    return ("PK Scan", [], {"_id": {'$gt': 1}}, Predicates.pk_lookup)


# Simple Index Tests
def test_simple_1():
    return ("Simple Index", [Index("index", [("a", 1)])], {"a": 1}, Predicates.no_table_scan_no_filter)


def test_simple_2():
    return ("Simple Index Union", [Index("index", [("a", 1)]), Index("index2", [("b", 1)])], {
        '$or': [{
            "a": 1
        }, {
            "b": 1
        }]
    }, Predicates.no_table_scan)


def test_simple_3():
    return ("Compound Index as Simple Index", [Index("compound", [("a", 1), ("b", 1)])], {
        "a": 1
    }, Predicates.no_table_scan_no_filter)


def test_simple_4():
    return ("Prefers Simple to Compound Index", [Index("compound", [("a", 1), ("b", 1)]),
                                                 Index("simple", [("a", 1)])], {
                                                     "a": 1
                                                 }, lambda e: Predicates.only_index_named("simple", e))


def test_simple_5():
    return ("Prefers Shorter to Longer Compound Index",
            [Index("long", [("a", 1), ("b", 1), ("c", 1)]),
             Index("short", [("a", 1), ("b", 1)])], {
                 "a": 1
             }, lambda e: Predicates.only_index_named("short", e))


def test_simple_6():
    return ("Planner is greedy", [Index("one", [("a", 1)]), Index("two", [("b", 1)])], {
        '$and': [{
            'b': 1
        }, {
            'a': 1
        }]
    }, lambda e: Predicates.only_index_named("two", e))


def test_simple_7():
    return ("Simple Index Scan", [Index("index", [("a", 1)])], {"a": {'$gt': 1}}, Predicates.no_table_scan)


def test_simple_8():
    return ("Simple Index Multi Union", [
        Index("index", [("d", 1)]),
        Index("index2", [("c", 1)]),
        Index("index3", [("b", 1)]),
        Index("index4", [("a", 1)])
    ], {
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
    }, Predicates.no_table_scan)


def test_simple_9():
    return ("Simple Index on Dotted Path", [Index("index", [('a.b', 1)])], {'a.b': 'hello'}, Predicates.no_table_scan)


# Compound Index Tests
def test_compound_1():
    return ("Basic Compound Index", [Index("compound", [("a", 1), ("b", 1)])], {
        '$and': [{
            'a': 1
        }, {
            'b': 1
        }]
    }, Predicates.no_table_scan_no_filter)


def test_compound_11():
    return ("Basic Compound Index", [Index("compound", [("a", 1), ("b", 1)])], {
        '$and': [{
            'b': 1
        }, {
            'a': 1
        }]
    }, Predicates.no_table_scan)


def test_compound_2():
    return ("Planner gets to long index eventually",
            [Index("long", [("a", 1), ("b", 1), ("c", 1), ("d", 1), ("e", 1), ("f", 1), ("g", 1)])], {
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
            }, Predicates.no_table_scan)


def test_compound_3():
    return ("Compound Index Union", [Index("compound", [("a", 1), ("b", 1)]),
                                     Index("compound2", [("d", 1), ("c", 1)])], {
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
                                     }, Predicates.no_table_scan)


def test_compound_4():
    return ("Compound Index Range at End",
            [Index("compound", [("d", 1), ("b", 1), ("c", 1)]),
             Index("simple", [("d", 1)])], {
                 '$and': [{
                     'c': {
                         '$gt': 1
                     }
                 }, {
                     'b': 1
                 }, {
                     'd': 1
                 }]
             }, lambda e: Predicates.only_index_named("compound", e))


def test_compound_5():
    return ("Compound Index Range at Start",
            [Index("compound", [("d", 1), ("b", 1), ("c", 1)]),
             Index("simple", [("d", 1)])], {
                 '$and': [{
                     'd': {
                         '$gt': 1
                     }
                 }, {
                     'b': 1
                 }, {
                     'c': 1
                 }]
             }, lambda e: Predicates.only_index_named("simple", e))


def test_compound_6():
    return ("Compound Index Range at Middle",
            [Index("compound", [("d", 1), ("b", 1), ("c", 1)]),
             Index("simple", [("d", 1)])], {
                 '$and': [{
                     'b': {
                         '$gt': 1
                     }
                 }, {
                     'd': 1
                 }, {
                     'c': 1
                 }]
             }, lambda e: Predicates.only_index_named("compound", e))


def test_compound_7():
    return ("Planner is greedy to a fault",
            [Index("long", [("d", 1), ("b", 1), ("c", 1)]),
             Index("short", [("b", 1), ("c", 1)])], {
                 '$and': [{
                     'b': 1
                 }, {
                     'd': 1
                 }, {
                     'c': 1
                 }]
             }, lambda e: Predicates.only_index_named("short", e))


def test_compound_8():
    return ("Compound index matching exact index",
            [Index("a", [("a", 1)]),
             Index("ab", [("a", 1), ("b", 1)]),
             Index("abc", [("a", 1), ("b", 1), ("c", 1)]),
             Index("abcde", [("a", 1), ("b", 1), ("c", 1), ("d", 1), ("e", 1)])], {
                '$and': [{
                    'a': 1
                }, {
                    'b': 1
                }, {
                    'c': 1
                }]
            }, lambda e: Predicates.only_index_named("abc", e) and Predicates.no_table_scan_no_filter(e))


def test_compound_9():
    return ("Compound index matching longest prefix",
            [Index("a", [("a", 1)]),
             Index("ab", [("a", 1), ("b", 1)]),
             Index("abc", [("a", 1), ("b", 1), ("c", 1)]),
             Index("abcde", [("a", 1), ("b", 1), ("c", 1), ("d", 1), ("e", 1)])], {
                '$and': [{
                    'a': 1
                }, {
                    'b': 1
                }, {
                    'c': 1
                }, {
                    'd': 1
                }]
            }, lambda e: Predicates.only_index_named("abcde", e) and Predicates.no_table_scan_no_filter(e))


def test_compound_10():
    return ("Compound index matching dscontinuous index prefix",
            [Index("a", [("a", 1)]),
             Index("ab", [("a", 1), ("b", 1)]),
             Index("abc", [("a", 1), ("b", 1), ("c", 1)]),
             Index("abcde", [("a", 1), ("b", 1), ("c", 1), ("d", 1), ("e", 1)])], {
                '$and': [{
                    'a': 1
                }, {
                    'b': 1
                }, {
                    'c': 1
                }, {
                    'e': 1
                }]
            }, lambda e: Predicates.only_index_named("abc", e) and Predicates.no_table_scan(e))


tests = [globals()[f] for f in dir() if f.startswith("test_")]


def test(collection, t):
    (name, indexes, query, condition) = t
    sys.stdout.write("Testing %s..." % name)
    collection.drop_indexes()
    for index in indexes:
        collection.create_index(index.keys, name=index.name)
    explanation = collection.find(query).explain()
    # print explanation
    okay = condition(explanation['explanation'])
    if okay:
        print util.alert('PASS', 'okgreen')
        return True
    else:
        print util.alert('FAIL', 'fail')
        print explanation['explanation']
        return False


def test_all(collection1, collection2):
    print "Planner tests only use first collection specified"
    okay = True
    for t in tests:
        okay = test(collection1, t()) and okay
    return okay
