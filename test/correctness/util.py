#
# util.py
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

#!/usr/bin/python

import json
import os.path
import random
import re
import sys
import time
from bisect import bisect_left, bisect
from collections import OrderedDict
from collections import defaultdict
from datetime import datetime
from types import NoneType

import bson.timestamp
from bson import ObjectId, binary

import gen


class ModelOptions(object):
    # MongoDB bug SERVER-6050 (also implemented by document layer): {P:$elemMatch:{X}} can match {P:A} where A matches X rather than only {P:[A]}
    mongo6050_enabled = False
    # When MongoDB evaluates an $elemMatch on an array field, it treats it as an object *only* for the purpose of whether and where to impute nulls.
    elem_match_convert_to_dict = True
    # MongoDB considers two matching objects with different field orders to be different
    object_field_order_matters = True

    def __init__(self, compare_vs):
        if compare_vs == "DocLayer":
            self.mongo6050_enabled = True
            self.elem_match_convert_to_dict = False
            self.object_field_order_matters = False


# this is outlandishly unsafe and terrible
# however it should not crash anything, because we only use these for id fields which under mongo semantics are not allowed to change
# so this class causing some bizarre crash in python should be interpreted as a correctness failure in the model, and not just evidence that it's
# time to fix this horribleness
class HashableOrderedDict(OrderedDict):
    def __hash__(self):
        return self.__str__().__hash__()


def dedup(seq):
    'Remove duplicates. Preserve order first seen.  Assume orderable, but not hashable elements'
    result = []
    seen = []
    for x in seq:
        i = bisect_left(seen, x)
        if i == len(seen) or seen[i] != x:
            seen.insert(i, x)
            result.append(x)
    return result


BSON_type_codes = {
    float: 1,
    str: 2,
    dict: 3,
    OrderedDict: 3,
    HashableOrderedDict: 3,
    list: 4,
    binary.Binary: 5,
    ObjectId: 7,
    bool: 8,
    datetime: 9,
    NoneType: 10,
    int: 16,
    bson.timestamp.Timestamp: 17,
    long: 18
}


def is_literal(d):
    return True not in [k[0] == '$' for k in d.keys()]


def is_numeric_type_code(code):
    return code in [1, 16, 18]


def is_numeric(field):
    return type(field) in BSON_type_codes and is_numeric_type_code(BSON_type_codes[type(field)])


def comparable(term1, term2):
    t1 = type(term1)
    t2 = type(term2)

    if is_numeric(term1):
        t1 = int
    if is_numeric(term2):
        t2 = int

    return (t1 == t2) and t1 in [datetime, ObjectId, dict, OrderedDict, list, binary.Binary, bool, int, str, NoneType]


def compare(term1, term2):
    if type(term1) == type(
            term2
    ) == binary.Binary:  # FIXME: Abstraction breaking. Better to make our own binary class wrapping binary.Binary.
        return mongo_compare_binary(term1, term2)
    else:
        return cmp(term1, term2)


def BSON_order(term1, term2):
    if comparable(term1, term2):
        return compare(term1, term2)
    else:
        return 0  # FIXME


'''When comparing values of different BSON types, MongoDB uses the following comparison order, from lowest to highest:
    MinKey (internal type)
    Null
    Numbers (ints, longs, doubles)
    Symbol, String
    Object
    Array
    BinData
    ObjectID
    Boolean
    Date, Timestamp
    Regular Expression
    MaxKey (internal type)'''


def mongo_compare_type(term1, term2):
    t1 = type(term1)
    t2 = type(term2)

    if is_numeric(term1):
        t1 = int
    elif t1 == unicode:
        t1 = str

    if is_numeric(term2):
        t2 = int
    elif t2 in [unicode]:
        t2 = str

    # print 't1:', t1, term1
    # print 't2:', t2, term2
    type_order = [
        NoneType, int, str, OrderedDict, dict, list, binary.Binary, ObjectId, bool, datetime, bson.timestamp.Timestamp
    ]
    return cmp(type_order.index(t1), type_order.index(t2))


def mongo_compare_value(vl, vr):
    ret = mongo_compare_type(vl, vr)
    if ret != 0:
        return ret

    if type(vl) == OrderedDict:
        return mongo_compare_ordered_dict_items(vl, vr)
    elif type(vl) == dict:
        return mongo_compare_unordered_dict_items(vl, vr)
    elif type(vl) == list:
        return mongo_compare_list_items(vl, vr)
    elif type(vl) == binary.Binary:
        return mongo_compare_binary(vl, vr)

    return cmp(vl, vr)


def mongo_compare_pair(kl, vl, kr, vr):
    ret = mongo_compare_type(vl, vr)
    if ret != 0:
        return ret

    if kl < kr:
        return -1
    elif kl > kr:
        return 1

    if type(vl) == OrderedDict:
        return mongo_compare_ordered_dict_items(vl, vr)
    elif type(vl) == dict:
        return mongo_compare_unordered_dict_items(vl, vr)
    elif type(vl) == list:
        return mongo_compare_list_items(vl, vr)
    elif type(vl) == binary.Binary:
        return mongo_compare_binary(vl, vr)

    return cmp(vl, vr)


# lhs and rhs are OrderedDicts
def mongo_compare_ordered_dict_items(lhs, rhs):
    if lhs == rhs:
        return 0

    for kl, vl in lhs.iteritems():
        index = lhs.keys().index(kl)
        if (index + 1) > len(rhs):
            # lhs is longer(bigger) that rhs
            return 1
        ret = mongo_compare_pair(kl, vl, rhs.keys()[index], rhs.values()[index])
        if ret != 0:
            return ret

    return -1


def smallest_diff_key(A, B):
    """return the smallest key adiff in A such that A[adiff] != B[bdiff]"""
    diff_keys = [k for k in A if k not in B or A.get(k) != B.get(k)]
    if (len(diff_keys)) != 0:
        return min(diff_keys)
    else:
        return None


def mongo_compare_unordered_dict_items(A, B):
    if len(A) != len(B):
        return cmp(len(A), len(B))
    adiff = smallest_diff_key(A, B)
    bdiff = smallest_diff_key(B, A)
    if adiff is None:
        assert bdiff is None
        return 0
    if adiff != bdiff:
        return cmp(adiff, bdiff)
    return mongo_compare_value(A[adiff], B[bdiff])


def mongo_sort_list_of_ordered_dict(list, reverse=False):
    return sorted(list, cmp=mongo_compare_ordered_dict_items, reverse=reverse)


# lhs and rhs are lists
def mongo_compare_list_items(lhs, rhs):
    if lhs == rhs:
        return 0

    lenl = len(lhs)
    lenr = len(rhs)
    if lenl == 0 and lenr > 0:
        return -1
    if lenr == 0 and lenl < 0:
        return 1

    for index, vl in enumerate(lhs):
        if (index + 1) > len(rhs):
            # lhs is longer(bigger) that rhs
            return 1
        ret = mongo_compare_value(vl, rhs[index])
        if ret != 0:
            return ret

    return -1


def mongo_compare_binary(lhs, rhs):
    if len(lhs[:]) != len(rhs[:]):
        return cmp(len(lhs), len(rhs))
    if lhs.subtype != rhs.subtype:
        return cmp(lhs.subtype, rhs.subtype)
    return cmp(lhs[:], rhs[:])


def mongo_sort_list_of_list(list, reverse=False):
    return sorted(list, cmp=mongo_compare_list_items, reverse=reverse)


def mongo_sort_list(array, reverse=False):
    d = defaultdict(list)
    for v in array:
        if isinstance(v, unicode):
            d[str].append(v)
        else:
            d[type(v)].append(v)
    # print 'd0=', d

    for k in d.keys():
        if k == OrderedDict:
            d[k] = mongo_sort_list_of_ordered_dict(d[k], reverse=reverse)
        elif k == dict:
            d[k] = mongo_sort_list_of_ordered_dict(d[k], reverse=reverse)
        elif k == list:
            d[k] = mongo_sort_list_of_list(d[k], reverse=reverse)
        else:
            d[k] = sorted(d[k], reverse=reverse)
    # print 'd1=', d
    # print 'd1 none =', d[NoneType]
    # print 'd1 OrderedDict =', d[OrderedDict]
    # print 'd1 int =', d[int]
    # print 'd1 str =', d[str]
    # print 'd1 list =', d[list]
    # print 'd1 dict =', d[dict]
    if not reverse:
        return d[NoneType] + d[int] + d[str] + d[OrderedDict] + d[dict] + d[list] + d[datetime] + d[bson.timestamp.
                                                                                                    Timestamp]
    else:
        return d[bson.timestamp.
                 Timestamp] + d[datetime] + d[list] + d[dict] + d[OrderedDict] + d[str] + d[int] + d[NoneType]


def mongo_sort_list_by_field(array, field_name, reverse=False):
    # print 'array:', array
    # print 'field_name:', field_name
    new_array1 = list()
    new_array2 = list()
    for v in array:
        if isinstance(v, (dict, OrderedDict)) and field_name in v.keys() and v[field_name] is not None:
            new_array2.append(v)
        else:
            new_array1.append(v)
    # print 'new_array1:', new_array1
    # print 'new_array2:', new_array2
    if not reverse:
        return new_array1 + sorted(new_array2, cmp=mongo_compare_value, key=lambda abc: abc[field_name])
    else:
        return sorted(
            new_array2, cmp=mongo_compare_value, key=lambda abc: abc[field_name], reverse=reverse) + new_array1


def has_item(context, key):
    try:
        # print 'key:', key, context
        if not isinstance(key, (str, int)):
            print 'key', key, 'should be either string or integer!'
            return False

        if isinstance(context, (dict, OrderedDict)):
            if key in context and context[key] is not None:
                return True
        elif isinstance(context, list):
            key = int(key) if key.isdigit() else key
            if isinstance(key, int) and key < len(context) and context[key] is not None:
                return True
        else:
            return False

        if not isinstance(key, str):
            print 'key', key, 'is not string!'
            return False

        cur, _, rest = key.partition('.')
        if isinstance(context, list):
            cur = int(cur) if cur.isdigit() else cur
            if not isinstance(cur, int) or cur >= len(context):
                return False

        return has_item(context[cur], rest)
    except KeyError as e:
        return False


def mongo_sort_list_by_fields(array, field_name_map):
    # split into need_to and no_need_to sort lists
    lists_need_to_sort = defaultdict(list)
    new_list = list()
    for v in array:
        if isinstance(v, (dict, OrderedDict)):
            match = False
            for field_name in field_name_map.keys():
                # print 'field_name:', field_name, v
                if field_name in v.keys() and v[field_name] is not None:
                    lists_need_to_sort[field_name].append(v)
                    match = True
                    break
                elif has_item(v, field_name):
                    # print 'found field:', field_name, v
                    lists_need_to_sort[field_name].append(v)
                    match = True
                    break
            if not match:
                new_list.append(v)
        else:
            new_list.append(v)

    # print 'new_list:', new_list
    # print 'lists_need_to_sort:', lists_need_to_sort

    for k in field_name_map.keys()[::-1]:
        if k in lists_need_to_sort.keys():
            # print 'k=', k, lists_need_to_sort[k]
            reverse = (field_name_map[k] != 1)
            new_k, _, _ = k.partition('.')
            if not reverse:
                new_list = new_list + sorted(
                    lists_need_to_sort[k], cmp=mongo_compare_value, key=lambda abc: abc[new_k], reverse=reverse)
            else:
                new_list = sorted(
                    lists_need_to_sort[k], cmp=mongo_compare_value, key=lambda abc: abc[new_k],
                    reverse=reverse) + new_list
    return new_list


def mongo_sort_list_by_fields_list(array, field_name_list):
    field_name_map = OrderedDict()
    for pair in field_name_list:
        field_name_map[pair[0]] = pair[1]
    # print 'field_name_map:', field_name_map
    return mongo_sort_list_by_fields(array, field_name_map)


def has_object(str_field, doc):
    if type(doc) in [dict, OrderedDict, HashableOrderedDict] and str_field in doc:
        return True
    elif type(doc) is list and str_field.isdigit() and int(str_field) < len(doc):
        return True
    else:
        return False


def get_object(str_field, doc):
    if type(doc) in [dict, OrderedDict, HashableOrderedDict] and str_field in doc:
        return doc[str_field]
    elif type(doc) is list and str_field.isdigit() and int(str_field) < len(doc):
        return doc[int(str_field)]


def deep_convert_to_unordered(in_thing):
    if type(in_thing) in (dict, OrderedDict, HashableOrderedDict):
        return_dict = {}
        for k, v in in_thing.iteritems():
            return_dict[k] = deep_convert_to_unordered(v)
        return return_dict
    elif type(in_thing) is list:
        return [deep_convert_to_unordered(i) for i in in_thing]
    else:
        return in_thing


# The 'ordered' dict you get at the end of this is in a random order.
def deep_convert_to_ordered(in_thing):
    if type(in_thing) in (dict, OrderedDict):
        return_dict = OrderedDict()
        for k, v in in_thing.iteritems():
            return_dict[k] = deep_convert_to_ordered(v)
        return return_dict
    elif type(in_thing) is list:
        return [deep_convert_to_ordered(i) for i in in_thing]
    else:
        return in_thing


def deep_convert_unicode_to_str(obj):
    if isinstance(obj, (dict, OrderedDict, HashableOrderedDict)):
        new_dict = type(obj)()
        for k, v in list(obj.items()):
            new_dict[deep_convert_unicode_to_str(k)] = deep_convert_unicode_to_str(v)
        return new_dict
    elif isinstance(obj, list):
        return [deep_convert_unicode_to_str(i) for i in obj]
    elif isinstance(obj, unicode):
        return str(obj)

    return obj


def has_operator(obj, depth=0):
    if isinstance(obj, str):
        if obj and obj[0] == '$':
            return (obj, depth)
        else:
            return False
    if isinstance(obj, (dict, OrderedDict)):
        for k, v in obj.iteritems():
            result = has_operator(k, depth + 1) or has_operator(v, depth + 1)
            if result:
                operator, depth = result
                return (operator, depth)
        return False
    else:
        return False


def convert_compound_string_to_dict(compound_key, val):
    obj = OrderedDict()
    left, _, key = compound_key.rpartition('.')
    while key != '':
        obj = OrderedDict()
        obj[key] = val
        val = obj
        left, _, key = left.rpartition('.')
    return obj


def deep_convert_compound_string_to_dict(obj):
    if not hasattr(obj, '__iter__'):
        return obj

    if isinstance(obj, list):
        for i in range(0, len(obj)):
            obj[i] = deep_convert_compound_string_to_dict(obj[i])
    if isinstance(obj, dict):
        mutable = OrderedDict(obj)
        for k, v in obj.iteritems():
            if isinstance(k, str):
                oo = convert_compound_string_to_dict(k, v)
                del mutable[k]
                for kk, vv in oo.iteritems():
                    mutable[kk] = deep_convert_compound_string_to_dict(vv)
        obj = mutable

    return obj


def is_none(obj):
    if obj is None:
        return True
    elif isinstance(obj, list):
        if len(obj) == 0:
            return True
    return False


class MongoModelException(Exception):
    def __init__(self, message, Errors=None):
        Exception.__init__(self, message)
        self.Errors = Errors


def format_result(parent, result, index):
    if isinstance(parent, MongoModelNondeterministicList):
        source = 'Mongo Model'
    else:
        source = 'PyMongo'

    formatted = '{:<15} ({})'.format(source, len(result))
    if index < len(result):
        formatted += ': %r' % deep_convert_unicode_to_str(deep_convert_to_unordered(result[index]))

    return formatted


class SortKeyFetcher(object):
    def __init__(self, query, doc, options):
        self.query = query
        self.doc = doc
        self.filtered_lists = {}
        self.options = options

    def get_sort_value(self, path, reverse):
        subitem = self.get_subitem(self.doc, '', path, reverse)
        self.query = None
        return subitem

    def get_subitem(self, obj, current_path, path, reverse, is_sublist=False):
        if not path or obj is None:
            if isinstance(obj, list):
                if len(obj) == 0:
                    return 'EMPTY_LIST'
                obj = self.extract_from_array(obj, current_path, None, None, reverse, is_sublist, lambda x: x)

            return obj

        cur, _, rest = path.partition('.')

        sub_path = current_path + '.' + cur if current_path else cur

        if isinstance(obj, (dict, OrderedDict)):
            return self.get_subitem(obj.get(cur, None), sub_path, rest, reverse)

        elif isinstance(obj, list):
            if cur.isdigit():
                if int(cur) < len(obj):
                    return self.get_subitem(obj[int(cur)], sub_path, rest, reverse)
                elif len(obj) == 1 and isinstance(
                        obj[0], (dict, OrderedDict)) and cur in obj[0]:  # Possibly need to respect previous sort filter
                    return self.get_subitem(obj[0][int(cur)], sub_path, rest, reverse)
            else:
                if is_sublist:

                    def resolve_fxn(x):
                        self.get_subitem(x, sub_path, rest, reverse)
                else:

                    def resolve_fxn(x):
                        self.get_subitem(x, current_path, path, reverse, True)

                remaining_path = cur
                if rest:
                    remaining_path += '.' + rest

                return self.extract_from_array(obj, current_path, remaining_path, cur, reverse, is_sublist, resolve_fxn)

        return None

    def extract_from_array(self, array, path, remaining_path, key, reverse, is_sublist, resolve_fxn):
        # print '\nextractFromArray \n  array=%r, \n  path=%r, \n  key=%r, \n  reverse=%r, \n  is_sublist=%r' % (array, path, key, reverse,
        # is_sublist)
        if not array:
            return None

        if (path, is_sublist) in self.filtered_lists:
            array = self.filtered_lists[(path, is_sublist)]

        if key is None:
            sub_values = array
        else:
            sub_values = map(lambda v: v if isinstance(v, (dict, OrderedDict)) and key in v else None, array)

        resolved = [(resolve_fxn(v), i) for i, v in enumerate(sub_values)]
        resolved = self.apply_query_operators(resolved, path, remaining_path, self.query)

        sorted_items = sorted(resolved, cmp=compare_sort_value, key=lambda x: x[0], reverse=reverse)
        self.filtered_lists[(path, is_sublist)] = [
            array[i] for x, i in sorted_items if compare_sort_value(sorted_items[0][0], x) == 0
        ]
        return sorted_items[0][0]

    def apply_query_operators(self, array, path_to_array, path_to_filter, query):
        # print '\napplyQueryOperators: \n  array=%r, \n  path_to_array=%r, \n  path_to_filter=%r, \n  query=%r' %
        # (array, path_to_array, path_to_filter, query)

        if not array or not query:
            return array

        full_path = path_to_array
        if path_to_filter:
            full_path = path_to_array + '.' + path_to_filter

        return process_query_operator(full_path, array, query, val_func=lambda x: x[0], options=self.options)


# The MongoModelNondeterministicList generate a new list of documents by applying sort, limit and skip to a
# source list of documents. It first collects tuples of the sort values in each of the document, then it
# sort these tuples by the given orders. It creates index divider(tuple_value, offset) using the sorted tuples.
# When compare with other documents list, it uses index divider to go through both lists and make sure them have
# the same set of the documents for each distinct tuple.
# Some mongoDB special cases that I'm not able to figure out for now are documented here:
# https://docs.google.com/a/foundationdb.com/document/d/10CSqNEEUdfuo4uwKywe8-Zqnx06ynCa6mSo4SO_X3pk/edit
class MongoModelNondeterministicList(object):
    def __init__(self, doc_list, sort, limit, skip, query, projection, options):
        self.sort = sort
        self.limit = limit
        self.skip = skip
        self.query = query
        self.sortListReverse = False
        self.projection = projection
        self.options = options

        self.initialize(doc_list)

    def check_parallel_arrays(self, doc_list):
        # check if there are parallel arrays. MongoDB can not sort multiple fields with parallel arrays
        # https://jira.mongodb.org/browse/SERVER-13122

        # I'm not sure this check is actually quite right, because it looks to me like the two arrays must also
        # be in the same object.
        for doc in doc_list:
            array_path = ''
            for sort_key, sort_dir in self.sort:
                path = str(sort_key)
                while path:
                    item = get_subitem(doc, path, sort_dir != 1)[1]
                    if isinstance(item, list):
                        if not path.startswith(array_path) and not array_path.startswith(path):
                            raise MongoModelException('BadValue ' + sort_key +
                                                      ' cannot sort with keys that are parallel arrays')

                        array_path = max(path, array_path)
                        break

                    path = path.rpartition('.')[0]

    def initialize(self, doc_list):
        self.check_parallel_arrays(doc_list)

        if self.skip >= len(doc_list):
            self.index_divider = list()
            self.sorted_docs = list()
        else:
            self.generate_sorted_docs(doc_list)

    def generate_sorted_docs(self, doc_list):
        sort_tuples = [(i, ) + self.get_sort_tuple(doc) for i, doc in enumerate(doc_list)]

        for i, (_, sort_dir) in enumerate(reversed(self.sort)):
            sort_tuples = sorted(
                sort_tuples, cmp=compare_sort_value, key=lambda sort_tuple: sort_tuple[-i - 1], reverse=(sort_dir != 1))

        self.index_divider = [
            i for i in range(len(sort_tuples) + 1)
            if i == 0 or i == len(sort_tuples) or sort_tuples[i][1:] != sort_tuples[i - 1][1:]
        ]
        self.sorted_docs = [doc_list[x[0]] for x in sort_tuples]

    def __str__(self):
        ret = '\n============================== MongoModelNondeterministicList ==============================\n'
        old_sort_key_values = list()
        for idx, val in enumerate(self.sorted_docs):
            new_sort_key_values = self.get_sort_key_values(val)
            if new_sort_key_values != old_sort_key_values:
                ret += '===== sort key values:' + str(new_sort_key_values) + '=====\n'
                old_sort_key_values = new_sort_key_values
            ret += str(idx) + ' ' + json.dumps(val) + '\n'
        ret += '============================== End ==============================\n'
        return ret

    def get_sort_tuple(self, obj):
        fetcher = SortKeyFetcher(self.query, obj, self.options)
        return tuple([fetcher.get_sort_value(sort_key, sort_dir != 1) for (sort_key, sort_dir) in self.sort])

    def get_sort_key_values(self, obj):
        return zip([k[0] for k in self.sort], self.get_sort_tuple(obj))

    def compare(self, other):
        trace('debug', self)

        lhs = self.sorted_docs
        if isinstance(other, MongoModelNondeterministicList):
            if self.sort != other.sort:
                trace('error', 'Could not compare the two objects because they are not sorted the same:', self.sort,
                      other.sort)
                return False

            if self.limit == 0:
                end = len(other.sorted_docs)
            else:
                end = self.limit + self.skip

            rhs = other.sorted_docs[self.skip:end]
        else:
            trace('debug', '============================== MongoDB Result ==============================')
            for i, v in enumerate(other):
                trace('debug', i, v)
            trace('debug', '============================== End ==============================')

            rhs = other

        if self.skip >= len(lhs):
            trace('debug', 'Skip is larger than the length of data!')
            return True

        lhs_len = len(lhs) - self.skip
        if self.limit > 0:
            lhs_len = min(self.limit, lhs_len)

        if len(rhs) != lhs_len:
            trace('error', 'Result set sizes don\'t match!')
            return False

        # convert timestamp to integer in hours since the timestamp will not exactly the same
        lhs = deep_convert_datetime_to_integer(lhs)
        rhs = deep_convert_datetime_to_integer(rhs)

        start = self.skip
        if self.limit == 0:
            end = len(lhs)
        else:
            end = min(self.limit + self.skip, len(lhs))

        index = bisect(self.index_divider, start) - 1
        failed = False
        while self.index_divider[index] < end:
            s = max(self.index_divider[index], start) - start
            e = min(self.index_divider[index + 1], end) - start

            result1 = sorted(
                deep_convert_to_unordered(lhs[self.index_divider[index]:self.index_divider[index + 1]]),
                cmp=mongo_compare_value)
            result2 = sorted(deep_convert_to_unordered(rhs[s:e]), cmp=mongo_compare_value)

            index += 1

            try:
                i = j = 0
                while i < len(result1) and j < len(result2):
                    assert not failed and mongo_compare_value(result1[i], result2[j]) <= 0
                    if result1[i] == result2[j]:
                        j += 1

                    i += 1

                assert j == len(result2)
            except:
                if not failed:
                    print '\nSorted list mismatch at index (%d, %d)!' % (i, j)

                    print 'Query: %r' % self.query
                    print 'Projection: %r' % self.projection
                    print 'Sort: %r' % self.sort
                    print 'Skip: %r' % self.skip
                    print 'Limit: %r' % self.limit
                    print '\n------------First Mismatch-----------'
                    print '\n  %s' % format_result(self, result1, i)
                    print '  %s\n' % format_result(other, result2, j)

                    failed = True

                print '\n------------Model Sort Tuple: %r-----------' % self.get_sort_key_values(result1[0])

                for i in range(0, max(len(result1), len(result2))):
                    print '\n%d: %s' % (i, format_result(self, result1, i))
                    print '%d: %s' % (i, format_result(other, result2, i))

        return not failed


def compare_sort_value(vl, vr):
    if vl == vr:
        return 0
    elif vl == 'EMPTY_LIST':
        return -1
    elif vr == 'EMPTY_LIST':
        return 1

    ret = mongo_compare_type(vl, vr)
    if ret != 0:
        return ret

    if type(vl) == OrderedDict:
        return mongo_compare_ordered_dict_items(vl, vr)
    elif type(vl) == dict:
        return mongo_compare_unordered_dict_items(vl, vr)
    elif type(vl) == list:
        return mongo_compare_list_items(vl, vr)
    elif type(vl) == binary.Binary:
        return mongo_compare_binary(vl, vr)

    if vl < vr:
        return -1
    elif vl > vr:
        return 1
    else:
        return 0


def is_list_subset(src_list, sub_list):
    for i in sub_list:
        if sub_list.count(i) > src_list.count(i):
            return False
    return True


def check_ambiguous_array(obj, path):
    cur, _, rest = str(path).partition('.')
    if cur == '':
        return
    else:
        if isinstance(obj, (dict, OrderedDict)):
            if cur in obj:
                check_ambiguous_array(obj[cur], rest)
        elif isinstance(obj, list):
            if cur.isdigit():
                if int(cur) < len(obj):
                    if len(obj) > 0 and any(cur in d for d in obj if isinstance(d, (dict, OrderedDict))):
                        raise MongoModelException(
                            'Runner error: Location16746 Ambiguous field name found in array (do not use numeric field names in '
                            'embedded elements in an array), field: ' + str(cur) + ', array:' + str(obj))
                    check_ambiguous_array(obj[int(cur)], rest)
            for i in obj:
                if isinstance(i, (dict, OrderedDict)) and (cur in i):
                    check_ambiguous_array(i[cur], rest)


def regex_predicate(value, query, options):
    if type(value) != str:
        return False

    regex_flags = 0
    if '$options' in query and "m" in query["$options"]:
        regex_flags |= re.MULTILINE
    if '$options' in query and "s" in query["$options"]:
        regex_flags |= re.DOTALL
    if '$options' in query and "i" in query["$options"]:
        regex_flags |= re.IGNORECASE

    try:
        if re.search(query["$regex"], value, regex_flags) is not None:
            return True
    except:
        pass

    return False


def debug_predicate(f):
    def x(val, query, options):
        print('\nRunning predicate:\n  val=%r,\n  query=%r:' % (val, query)),
        result = f(val, query, options)
        print result
        return result

    return x


mongoQueryPredicates = {
    # TODO: the second part of the or below may only be applicable to sorting
    None: lambda val, query, options: comparable(val, query) and (mongo_compare_value(val, query) == 0 or mongo_compare_value([val], query)),

    '$gte': lambda val, query, options: comparable(val, query['$gte']) and mongo_compare_value(val, query['$gte']) >= 0,
    '$gt': lambda val, query, options: comparable(val, query['$gt']) and mongo_compare_value(val, query['$gt']) == 1,
    '$lte': lambda val, query, options: comparable(val, query['$lte']) and mongo_compare_value(val, query['$lte']) <= 0,
    '$lt': lambda val, query, options: comparable(val, query['$lt']) and mongo_compare_value(val, query['$lt']) <= 0,
    '$ne': lambda val, query, options: not comparable(val, query['$ne']) or mongo_compare_value(val, query['$ne']) != 0,
    '$in': lambda val, query, options: val in query['$in'],
    '$nin': lambda val, query, options: val not in query['$nin'],

    # This includes a random check to address some unusual mongo behavior (empty list counts as an object and numeric sort type matching)
    # TODO: This is behavior applicable only to sorting, so we can't use it if we start using the predicates for query resolution
    '$type': lambda val, query, options: BSON_type_codes[type(val)] == query['$type'] or (query['$type'] == 3 and val == []) or (is_numeric_type_code(query['$type']) and is_numeric(val)),

    '$size': lambda val, query, options: type(val) is list and len(val) == query['$size'],
    '$exists': lambda val, query, options: query['$exists'],
    '$regex': regex_predicate,
}

mongoLogicalQueryOperators = ['$and', '$not', '$or', '$nor']


def process_logical_query_operator(path, array, operation, query, val_func, options):
    # print '\nprocessLogicalQueryOperator: \n  path=%r, \n  array=%r, \n  operation=%r, \n  query=%r' % (path, array, operation, query)
    if operation == '$and':
        for expression in query:
            array = process_query_operator(path, array, expression, val_func, options)

    elif operation == '$or':
        result = []
        for expression in query:
            result += process_query_operator(path, array, expression, val_func, options)

        array = result

    elif operation == '$nor':
        return array  # TODO: implement this

    elif operation == '$not':
        result = process_query_operator(path, array, query, val_func, options)
        array = [x for x in array if x not in result]

    return array


def process_query_operator(path, array, query, val_func, options):
    for key, expression in query.items():
        if key in mongoLogicalQueryOperators:
            array = process_logical_query_operator(path, array, key, expression, val_func, options)
        elif path == key:
            # print '\nprocessQueryOperator: \n  path=%r, \n  array=%r, \n  query=%r' % (path, array, query)
            if not isinstance(expression, (dict, OrderedDict)):
                op = None
            elif '$regex' in expression:
                op = '$regex'
            else:
                assert len(expression) == 1
                op = expression.keys()[0]

            if op == '$elemMatch':
                # This is emulating mongo's apparent behavior to require filter items to only match one filter from an $elemMatch (not all of them)
                # It's possible we should not implement this behavior ourselves
                array = process_query_operator(path, array, expression[op], val_func, options)

            elif op == '$size':
                if len(array) != expression[op]:
                    array = []

            elif op == '$all':
                array = process_query_operator(path, array, {'$and': [{
                    key: p
                } for p in expression['$all']]}, val_func, options)

            elif op in mongoQueryPredicates:
                # This is emulating a mongo bug. We should enable this conditionally
                if (op == '$in' or op == '$all') and isinstance(expression[op], list):
                    expression = {
                        op: expression[op] + [v[0] for v in expression[op] if isinstance(v, list) and len(v) > 0]
                    }

                return [v for v in array if mongoQueryPredicates[op](val_func(v), expression, options)]
                # return [v for v in array if debugPredicate(mongoQueryPredicates[op])(val_func(v), expression, options)]

            elif op in mongoLogicalQueryOperators:
                array = process_logical_query_operator(path, array, op, expression, val_func, options)
            else:
                assert False

    return array


def get_subitem(obj, path, reverse, is_sublist=False):
    if path is None:
        return False, None, False
    if not str(path):
        return True, obj, is_sublist

    cur, _, rest = str(path).partition('.')

    if isinstance(obj, (dict, OrderedDict)) and cur in obj:
        return get_subitem(obj[cur], rest, reverse)

    elif isinstance(obj, list):
        if cur.isdigit():
            if int(cur) < len(obj):
                return get_subitem(obj[int(cur)], rest, reverse, True)
            elif len(obj) == 1 and isinstance(obj[0], (dict, OrderedDict)) and cur in obj[0]:
                return get_subitem(obj[0][cur], rest, reverse)
        else:
            temp = mongo_cursor_sort_list([i.get(cur) for i in obj if isinstance(i, (dict, OrderedDict)) and cur in i],
                                          reverse)
            if len(temp) > 0 and (len(temp) == len(obj) or reverse):
                return get_subitem(temp[0], rest, reverse)

    return False, None, False


# According to MongoDB, a comparison of an empty array (e.g. [ ]) treats the empty array as less than null or a missing field.
def mongo_cursor_sort_list(array, reverse=False):
    empty = [v for v in array if v == []]
    array = filter(lambda i: i != [], array)
    if reverse:
        return mongo_sort_list(array, reverse) + empty
    else:
        return empty + mongo_sort_list(array, reverse)


def deep_convert_datetime_to_integer(obj):
    if type(obj) is datetime:
        # make the timestamp to be hour based since the timestamps will be different
        # when update different databases
        return int(time.mktime(obj.timetuple()) / 3600)
    if type(obj) is bson.timestamp.Timestamp:
        return int(obj.time / 3600)
    elif type(obj) is dict:
        new_obj = {}
        for k, v in obj.iteritems():
            new_obj[k] = deep_convert_datetime_to_integer(v)
        return new_obj
    elif type(obj) is OrderedDict:
        new_obj = OrderedDict()
        for k, v in obj.iteritems():
            new_obj[k] = deep_convert_datetime_to_integer(v)
        return new_obj
    elif type(obj) is list:
        return [deep_convert_datetime_to_integer(i) for i in obj]
    else:
        return obj


TRACE_LEVEL_DEFINE = ['fatal', 'error', 'warning', 'debug']
traceLevel = 'error'


def trace(trace_level, *args):
    if TRACE_LEVEL_DEFINE.index(trace_level) <= TRACE_LEVEL_DEFINE.index(traceLevel):
        if trace_level == 'error':
            print '\033[91m',
        for x in args:
            print x,
        print '\033[0m'


def generate_list_of_ordered_dict_from_json(list_json_string):
    decoder = json.JSONDecoder(object_pairs_hook=OrderedDict)
    list_dict = list()
    for i in list_json_string:
        list_dict.append(decoder.decode(i))
    return list_dict


def alert(msg, type='okblue'):
    colors = {
        "header": '\033[95m',
        "okblue": '\033[94m',
        "okgreen": '\033[92m',
        "warning": '\033[93m',
        "fail": '\033[91m',
        "end": '\033[0m'
    }

    return "%s%s%s" % (colors[type], msg, colors['end'])


def indent(msg, times=2):
    return "%s%s" % (''.join(['\t' for _ in range(0, times)]), msg)


def join_n(*args):
    return '\n'.join(args)


def weaken_tests(ns):
    if 'mongo' in [ns['1'], ns['2']]:
        weaken_tests_for_mongo(ns)
    else:
        weaken_tests_for_doclayer()


def weaken_tests_for_mongo(ns):
    gen.generator_options.multi_updates = False
    gen.generator_options.mongo12754_enabled = False
    gen.generator_options.allow_long_ids = False
    gen.generator_options.allow_id_elemmatch = False
    gen.generator_options.allow_general_nots = False
    gen.generator_options.use_transactions = False
    # gen.generator_options.index_parallel_arrays = False
    # gen.generator_options.allow_long_fields = False
    ns['no_indexes'] = False
    # I'm still thinking this over, but it seems like even if Mongo produces different behavior with indexes on and off, we should not,
    # so we get nothing out of turning indexes on, and get to remove the above weakenings by turning them off. This may change in the
    # future though.


def weaken_tests_for_doclayer():
    gen.generator_options.nested_elemmatch = False  # FIXME
    gen.generator_options.mongo12754_enabled = False  # FIXME
    gen.generator_options.allow_id_elemmatch = False  # FIXME
    gen.generator_options.numeric_fieldnames = False  # FIXME
    gen.generator_options.allow_sorts = False  # FIXME


def get_cmd_line(ns):
    seed = random.randint(0, sys.maxint)
    gen.global_prng = random.Random(seed)

    # Generate the random data
    ns['no_updates'] = gen.global_prng.choice([True, False])
    sorting_tests_enabled = gen.global_prng.choice([True, False])
    gen.generator_options.allow_sorts = sorting_tests_enabled
    gen.generator_options.test_nulls = gen.global_prng.choice([True, False])
    gen.generator_options.upserts_enabled = gen.global_prng.choice([True, False])
    gen.generator_options.numeric_fieldnames = gen.global_prng.choice([True, False])
    ns['no_indexes'] = gen.global_prng.choice([True, False])
    ns['no_projections'] = gen.global_prng.choice([True, False])
    dd = gen.global_prng.random()

    num_doc = 100
    if dd < 0.75:
        num_doc = gen.global_prng.randint(1, 100)
    elif dd < 0.875:
        num_doc = gen.global_prng.randint(100, 300)
    elif dd < 0.9375:
        num_doc = gen.global_prng.randint(300, 900)
    else:
        num_doc = gen.global_prng.randint(900, 3000)

    ns['num_doc'] = num_doc

    num_iter = max(3000 / num_doc, 100)

    ns['num_iter'] = num_iter

    weaken_tests(ns)

    return command_line_str(ns, seed)


def command_line_str(ns, seed):
    verbose = ns['verbose']
    max_pool_size = ns['max_pool_size']
    instance_id = ns['instance_id']

    cmd_line = "python "
    cmd_line = cmd_line + os.path.join(os.path.dirname(os.path.realpath(__file__)), "document-correctness.py")
    cmd_line = cmd_line + ("" if not verbose else " --verbose")
    cmd_line = cmd_line + " --mongo-host " + str(ns["mongo_host"])
    cmd_line = cmd_line + " --mongo-port " + str(ns["mongo_port"])
    cmd_line = cmd_line + " --doclayer-host " + str(ns["doclayer_host"])
    cmd_line = cmd_line + " --doclayer-port " + str(ns["doclayer_port"])
    cmd_line = cmd_line + ("" if max_pool_size is None else " --max-pool-size " + str(max_pool_size))
    cmd_line = cmd_line + " forever " + str(ns["1"]) + " " + str(ns["2"])
    cmd_line = cmd_line + " --seed " + str(seed)
    cmd_line = cmd_line + " --num-doc " + str(ns['num_doc'])
    cmd_line = cmd_line + " --num-iter " + str(ns['num_iter'])
    cmd_line = cmd_line + ("" if ns['no_updates'] else " --no-update")
    cmd_line = cmd_line + ("" if gen.generator_options.allow_sorts else " --no-sort")
    cmd_line = cmd_line + ("" if gen.generator_options.numeric_fieldnames else " --no-numeric-fieldnames")
    cmd_line = cmd_line + ("" if gen.generator_options.test_nulls else " --no-nulls")
    cmd_line = cmd_line + ("" if gen.generator_options.upserts_enabled else " --no-upserts")
    cmd_line = cmd_line + ("" if ns['no_indexes'] else " --no-indexes")
    cmd_line = cmd_line + ("" if ns['no_projections'] else " --no-projections")
    cmd_line = cmd_line + ("" if instance_id == 0 else " --instance-id " + str(instance_id))

    return cmd_line + '\n'


def save_cmd_line(cmd_line):
    # Get the SEED from the command-line, easier way
    idx1 = cmd_line.find(" --seed ")  # length 8
    idx2 = cmd_line.find(" --num-doc ")
    if idx1 == -1 or idx2 == -1:
        raise Exception("This command line is not properly formatted")

    value = cmd_line[idx1 + 8:idx2].strip()

    # make sure the folder where failure will be stored is created
    directory = os.path.dirname(os.path.realpath(__file__)) + '/test_results/'
    if not os.path.exists(directory):
        os.makedirs(directory)

    fname = directory + "journal_" + value + ".running"
    ffile = open(fname, 'w')
    ffile.write(cmd_line)
    ffile.close()
    return fname


def rename_file(fname, new_ext):
    (froot, old_ext) = os.path.splitext(fname)
    try:
        os.rename(fname, froot + new_ext)
    except:
        pass
    return froot + new_ext


def cleanup_file(fname):
    if os.path.exists(fname):
        os.remove(fname)


def test_sort_key_fetcher():
    doc1 = {'a': ['a', 'b', 'c', binary.Binary('AAAA', 0)]}
    doc2 = {'a': [{'b': 1, 'c': 1}, {'b': 2, 'c': 2}]}

    sort1 = [('a', True)]
    sort2 = [('a.c', True), ('a.b', False)]

    # query1 = {'a.b': {'$gt':2}}
    query1 = {'a': {'$type': 2}}

    fetcher = SortKeyFetcher(query1, doc1, ModelOptions(''))
    result = tuple([fetcher.get_sort_value(k, dir) for (k, dir) in sort1])
    print result


def test_mongo_nondeterministic_list():
    docs = [{'a': []}, {'a': 2}]
    sort = [('a', True)]
    query = {}

    MongoModelNondeterministicList(docs, sort, 0, 0, query, ModelOptions(''))


if __name__ == '__main__':
    # test_mongo_nondeterministic_list()
    test_sort_key_fetcher()
