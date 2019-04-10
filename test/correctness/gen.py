#
# gen.py
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

import base64
import datetime
from collections import OrderedDict
from random import Random

from bson import binary
from bson.objectid import ObjectId

global_prng = Random()


# this is outlandishly unsafe and terrible
# however it should not crash anything, because we only use these for id fields which under mongo semantics are not allowed to change
# so this class causing some bizarre crash in python should be interpreted as a correctness failure in the model, and not just evidence that it's
# time to fix this horribleness
class HashableOrderedDict(OrderedDict):
    def __hash__(self):
        return self.__str__().__hash__()


class generator_options:
    mongo12754_enabled = True  # Enable generation of queries of the form {P:{$type: 4}} because Mongo is unable to answer them correctly.
    allow_long_ids = True  # Enable generation of longish id objects which currently break Mongo.
    allow_id_elemmatch = True  # Enable generation of queries that attempt $elemMatch vs the _id field (not permitted in Mongo).
    # Enable insertion of documents with array values in more than one field of a compound index (not permitted in Mongo).
    index_parallel_arrays = True
    allow_long_fields = True  # Enable insertion of documents with indexed values that are reasonably large (not permitted in Mongo).
    allow_general_nots = True  # Enable queries of the form {$not: {$not ... or {$not : {$regex...
    upserts_enabled = True
    numeric_fieldnames = True
    test_nulls = True
    allow_sorts = True
    multi_updates = True
    nested_elemmatch = True


def random_string(length):
    if length == 0:
        return ''
    return ''.join(global_prng.choice('abcde') for i in range(length))


def random_regex(length):
    if length == 0:
        return ''
    # what chars to use in the query
    var = "".join(global_prng.choice('abcde') for i in range(length))

    # what options to generate, can be all of 4, but cannot repeat, hence use of set
    opt = "".join(set(
        global_prng.choice('ims')
        for i in range(global_prng.randint(0, 4))))  # x is not supported by the python re, so do not use it

    # are we going to use prefix or not ?
    pre = "".join(set(global_prng.choice('^') for i in range(global_prng.randint(0, 1))))

    # do we need wild card ?
    w = global_prng.random()
    if w < 0.33:
        wld = "".join(set(global_prng.choice('*') for i in range(global_prng.randint(0, 1))))  # 0 or more
    elif w < 0.66:
        wld = "".join(set(global_prng.choice('+') for i in range(global_prng.randint(0, 1))))  # 1 or more
    else:
        wld = "".join(set(global_prng.choice('?') for i in range(global_prng.randint(0, 1))))  # 0 or 1
    wld += "."  # whatever we have generated can be followed by any other char except newline

    r = global_prng.random()
    div = '/' if global_prng.random() < 0.5 else ''

    # temporary disabled, it needs more work
    # if r < 0.3:
    #    res = "/" + pre + var + wld + "/" + opt                                                # format /ABC/ixms
    if opt == "":
        res = {'$regex': div + pre + var + wld + div}  # format  { $regex : /ABC/ }
    else:
        if global_prng.random() < 0.5:
            res = {
                '$regex': div + pre + var + wld + div,
                '$options': opt
            }  # format  { $regex : /ABC/, $options:'ixms' }
        else:
            res = {
                '$options': opt,
                '$regex': div + pre + var + wld + div
            }  # format  { $options:'ixms', $regex : /ABC/ }
    return res


def random_field_name():
    if generator_options.numeric_fieldnames:
        return global_prng.choice(u'ABCDE012')
    else:
        return global_prng.choice(u'ABCDE')


def random_compound_field_name(with_id):
    if with_id and global_prng.random() < 0.3:
        return '_id'
    if global_prng.random() < 0.8:
        return random_field_name()
    else:
        if generator_options.numeric_fieldnames:
            return random_field_name() + '.' + '.'.join(
                global_prng.choice(u'ABCDE012') for i in range(global_prng.randint(1, 2)))
        else:
            return random_field_name() + '.' + '.'.join(
                global_prng.choice(u'ABCDE') for i in range(global_prng.randint(1, 2)))


def random_int(absval=100):
    return global_prng.randint(-absval, absval)


def random_date(absval=100):
    return datetime.datetime.fromtimestamp(1000000 + global_prng.randint(0, 2 * absval))


def random_float():
    return global_prng.random()


def random_binary(length):
    b64 = base64.b64encode(random_string(length))
    return binary.Binary(b64, global_prng.choice([0, 1]))


def random_primitive_value():
    r = global_prng.random()
    if (r < 0.1) and generator_options.test_nulls:
        return None
    elif (r < 0.35):
        return random_string(1)
    elif (r < 0.45):
        return random_binary(2)
    elif (r < 0.55):
        return random_date()
    elif (r < 0.75):
        return random_float()
    else:
        return random_int()


def random_id_value():
    r = global_prng.random()
    if r < 0.2:
        return random_string(7)
    elif (r < 0.3):
        return random_binary(7)
    elif r < 0.4:
        return random_float()
    elif r < 0.5:
        return random_object_id()
    elif (r < 0.7):
        return random_int(100000)
    elif (r < 0.9):
        return random_date(1000000)
    else:
        return random_id_document()


def random_value():
    r = global_prng.random()
    while True:
        if (r < 0.1) and generator_options.test_nulls:
            val = None
        elif (r < 0.2):
            val = random_float()
        elif (r < 0.4):
            val = random_string(global_prng.randint(1, 8))
        elif (r < 0.5):
            val = random_binary(global_prng.randint(1, 8))
        elif (r < 0.6):
            val = random_int()
        elif (r < 0.7):
            val = random_date()
        elif (r < 0.8):
            val = random_array()
        else:
            val = random_document(False)
        if generator_options.allow_long_fields or len(str(val)) < 100:
            return val


def random_element():
    return (random_field_name(), random_value())


def random_id_document():
    if generator_options.allow_long_ids:
        doc = HashableOrderedDict()
        for i in range(0, 3):
            el = random_element()
            doc[el[0]] = el[1]
        return doc
    else:
        while True:
            doc = HashableOrderedDict()
            for i in range(0, 3):
                el = random_element()
                doc[el[0]] = el[1]
            if len(str(doc)) < 100:
                return doc


def random_document(with_id):
    doc = OrderedDict()
    for i in range(0, global_prng.randint(0, 6)):
        el = random_element()
        doc[el[0]] = el[1]
    if with_id:
        doc[u'_id'] = random_id_value()
    return doc


def random_array():
    arr = []
    for i in range(global_prng.randint(0, 6)):
        el = random_element()
        arr.append(el[1])
    return arr


def random_large_primitive_array():
    arr = []
    for i in range(global_prng.randint(11, 20)):
        arr.append(random_primitive_value())
    return arr


def random_all_array():
    arr = []
    if (global_prng.random() < 0.9):
        for i in range(global_prng.randint(0, 6)):
            el = random_element()
            arr.append(el[1])
    else:
        for i in range(global_prng.randint(0, 6)):
            q = random_elem_match_predicate()
            arr.append({q[0]: q[1]})
    return arr


def random_index_spec():
    indexObj = []
    if generator_options.index_parallel_arrays:
        for k in range(0, global_prng.randint(1, 5)):
            indexObj.append((random_field_name(), 1))
    else:
        indexObj.append((random_field_name(), 1))
    return indexObj


def random_range_predicate():
    return (global_prng.choice(['$lt', '$lte', '$gt', '$gte']), random_primitive_value())


def random_exists_predicate():
    if (global_prng.random() < 0.5):
        return ('$exists', True)
    else:
        return ('$exists', False)


def random_type_predicate():
    i = global_prng.randint(1, 18)
    # Do not generate {$type : 4} if we are testing vs. Mongo.
    while i == 17 or not generator_options.mongo12754_enabled and i == 4:
        i = global_prng.randint(1, 18)
    return ('$type', i)


def random_size_predicate():
    return ('$size', global_prng.randint(0, 5))


def random_all_predicate():
    return ('$all', random_all_array())


def random_elem_match_predicate():
    if (global_prng.random() < 0.5):
        e = dict()
        for i in range(0, global_prng.randint(1, 4)):
            if generator_options.nested_elemmatch:
                r = global_prng.choice([global_prng.uniform(0, 0.45), global_prng.uniform(0.5, 1.0)])
            else:
                r = global_prng.choice([global_prng.uniform(0, 0.4), global_prng.uniform(0.5, 1.0)])
            q = random_query(r)
            e.update(q)
        return ('$elemMatch', e)
    else:
        e = dict()
        for i in range(0, global_prng.randint(1, 4)):
            if generator_options.nested_elemmatch:
                r = global_prng.choice([global_prng.uniform(0, 0.45), global_prng.uniform(0.5, 0.9)])
            else:
                r = global_prng.choice([global_prng.uniform(0, 0.4), global_prng.uniform(0.5, 0.9)])
            q = random_query(r)
            q = {k: q.values()[0][k] for k in q.values()[0]}
            e.update(q)
        return ('$elemMatch', e)


def random_in_predicate():
    return ('$in', random_array())


def random_nin_predicate():
    return ('$nin', random_array())


def random_ne_predicate():
    return ('$ne', random_value())


def random_not_predicate():
    r = global_prng.uniform(0, 0.9)
    q = random_query(r).values()[0]
    while type(q) is list or not generator_options.allow_general_nots and ('$not' in q or '$regex' in q):
        r = global_prng.uniform(0, 0.9)
        q = random_query(r).values()[0]
    return ('$not', q)


def random_logical_predicate():
    if global_prng.random() < 0.25:
        return random_not_predicate()
    else:
        return (global_prng.choice(['$and', '$or', '$nor']),
                [random_query() for i in range(0, global_prng.randint(1, 3))])


value_operators = ['$ne', '$lt', '$lte', '$gt', '$gte']


def random_query(r=None):
    with_id = True  # Whether the predicate in question is allowed to target the _id field.
    if r is None:
        r = global_prng.random()
    if (r < 0.1):
        query = random_exists_predicate()
    elif (r < 0.2):
        query = random_size_predicate()
    # elif (r < 0.3):
    #     query = random_all_predicate()
    elif (r < 0.4):
        query = random_type_predicate()
    # elif (r < 0.45):
    #     query = random_elem_match_predicate()
    #     with_id = generator_options.allow_id_elemmatch
    elif (r < 0.50):
        query = random_logical_predicate()
        if query[0] == '$not':
            return {random_compound_field_name(with_id): {query[0]: query[1]}}
        else:
            return {query[0]: query[1]}
    elif (r < 0.6):
        return {random_compound_field_name(with_id): random_regex(global_prng.randint(1, 8))}
    elif (r < 0.75):
        query = random_range_predicate()
    elif (r < 0.8):
        query = random_in_predicate()
    elif (r < 0.85):
        query = random_nin_predicate()
    elif (r < 0.9):
        query = random_ne_predicate()
    else:
        return {random_compound_field_name(with_id): random_value()}
    return {random_compound_field_name(with_id): {query[0]: query[1]}}


def count_query_results(collection, query):
    find = collection.find(query)
    if isinstance(find, list):
        number_results = len(find)
    else:
        number_results = find.count(False)
    return number_results


def random_query_with_one_or_fewer_matches(collection, r):
    while True:
        query = random_query(r)
        if count_query_results(collection, query) <= 1:
            break
    return query


def random_update_operator_inc():
    doc = {random_field_name(): global_prng.randint(-5, 5) for i in range(0, global_prng.randint(1, 6))}
    return {'$inc': doc}


def random_update_operator_mul():
    return {'$mul': {random_field_name(): global_prng.randint(-5, 5)}}


def random_update_operator_rename():
    doc = {}
    while len(doc.keys()) == 0:
        for i in range(0, global_prng.randint(0, 3)):
            old_name = random_field_name()
            new_name = random_field_name()
            if old_name != new_name and old_name not in doc.values() and new_name not in doc.keys(
            ) and new_name not in doc.values():
                doc[old_name] = new_name
    return {'$rename': doc}


def random_update_operator_set_on_insert():
    doc = {}
    for i in range(0, global_prng.randint(0, 6)):
        doc[random_field_name()] = random_value()
    return {'$setOnInsert': doc}


def random_update_operator_set():
    doc = {}
    for i in range(0, global_prng.randint(1, 6)):
        doc[random_field_name()] = random_value()
    return {'$set': doc}


def random_update_operator_unset():
    doc = {}
    for i in range(0, global_prng.randint(1, 6)):
        doc[random_field_name()] = '""'
    return {'$unset': doc}


def random_update_operator_min():
    return {'$min': {random_field_name(): random_value()}}


def random_update_operator_max():
    return {'$max': {random_field_name(): random_value()}}


def random_update_operator_current_date():
    # MongoDB has a bug that can not sort on different type of time formats(datetime.datatime and bson.timestamp.Timestamp)
    # So here we generate time in only one format for now
    doc = {}
    if global_prng.random() < 0:
        doc[random_field_name()] = True
    elif global_prng.random() < 1:
        doc[random_field_name()] = {'$type': 'timestamp'}
    else:
        doc[random_field_name()] = {'$type': 'date'}
    return {'$currentDate': doc}


def random_update_operator_dollar():
    return {}


def random_update_operator_add_to_set():
    if global_prng.random() < 0.5:
        return {'$addToSet': {random_field_name(): random_value()}}
    else:
        return {'$addToSet': {random_field_name(): {'$each': random_array()}}}


def random_update_operator_pop():
    return {'$pop': {random_field_name(): global_prng.choice([-1, 1])}}


def random_update_operator_pull_all():
    return {'$pullAll': {random_field_name(): random_array()}}


def random_update_operator_pull():
    return {'$pull': {random_field_name(): random_value()}}


def random_sort_by_fields():
    doc = {}
    for i in range(0, global_prng.randint(1, 3)):
        field_name = random_field_name()
        if global_prng.random() < 0.5:
            for j in range(0, global_prng.randint(1, 2)):
                field_name = field_name + '.' + random_field_name()
        doc[field_name] = global_prng.choice([-1, 1])
    return doc


def random_update_operator_push():
    r = global_prng.random()
    if r < 0.2:
        return {'$push': {random_field_name(): {'$each': random_array()}}}
    elif r < 0.4:
        return {'$push': {random_field_name(): {'$each': random_array(), '$slice': global_prng.randint(-5, 5)}}}
    # elif r < 0.6:
    # return {'$push': {randomFieldName(): {'$each': randomArray(), '$position': global_prng.randint(0,5)}}}
    elif r < 0.7 and generator_options.allow_sorts:
        return {'$push': {random_field_name(): {'$each': random_array(), '$sort': global_prng.choice([-1, 1])}}}
    elif r < 0.8 and generator_options.allow_sorts:
        return {'$push': {random_field_name(): {'$each': random_array(), '$sort': random_sort_by_fields()}}}
    else:
        return {'$push': {random_field_name(): random_value()}}


def random_update_operator_bit():
    return {'$bit': {random_field_name(): {global_prng.choice(['and', 'or', 'xor']): global_prng.randint(-100, 100)}}}


def random_update_operator_isolated():
    return {}


update_operators = [
    random_update_operator_inc,
    random_update_operator_mul,
    random_update_operator_rename,
    random_update_operator_set_on_insert,
    random_update_operator_set,
    random_update_operator_unset,
    random_update_operator_min,
    random_update_operator_max,
    # randomUpdateOperatorCurrentDate,
    # randomUpdateOperatorDollar,
    random_update_operator_add_to_set,
    random_update_operator_pop,
    random_update_operator_pull_all,
    random_update_operator_pull,
    random_update_operator_push,
    random_update_operator_bit
]

no_sort_update_operators = [
    random_update_operator_inc,
    random_update_operator_mul,
    random_update_operator_rename,
    random_update_operator_set_on_insert,
    random_update_operator_set,
    random_update_operator_unset,
    # randomUpdateOperatorCurrentDate,
    # randomUpdateOperatorDollar,
    random_update_operator_add_to_set,
    random_update_operator_pop,
    random_update_operator_pull_all,
    random_update_operator_pull,
    random_update_operator_push,
    random_update_operator_bit
]


def random_update_document(multi, upsert):
    r = global_prng.random()
    if not multi and r < 0.2:
        # the <update> document contains only field:value expressions
        if global_prng.random() < 0.5:
            return (False, random_document(False))
        else:
            return (False, random_document(True))
    else:
        # the <update> document contains update operator expressions
        ret_update = OrderedDict()
        for i in range(0, global_prng.randint(1, 3)):
            if generator_options.allow_sorts:
                random_operator = global_prng.choice(update_operators)
            else:
                random_operator = global_prng.choice(no_sort_update_operators)
            if random_operator != random_update_operator_rename:
                ret_update.update(random_operator())
            else:
                return (True, random_operator())
        return (True, ret_update)


def random_update(collection):
    upsert = global_prng.choice([True, False])
    multi = global_prng.choice([True, False]) if generator_options.multi_updates else False

    has_operator, update = random_update_document(multi, upsert)

    if not multi and not upsert:
        query = random_query_with_one_or_fewer_matches(collection, None)
    elif multi and not upsert:
        query = random_query(global_prng.random())
    elif not multi and upsert:
        query = random_query_with_one_or_fewer_matches(
            collection, None if generator_options.upserts_enabled or not has_operator else 0.95)
    else:
        query = random_query(global_prng.random() if generator_options.upserts_enabled or not has_operator else 0.95)

    if upsert:
        if has_operator:
            query['_id'] = random_id_value()
            query = {'_id': random_id_value()}  # FIXME: remove this as soon as evaluate() can handle multipart queries
        else:
            if global_prng.choice([True, False]):
                query['_id'] = random_id_value()
                query = {
                    '_id': random_id_value()
                }  # FIXME: remove this as soon as evaluate() can handle multipart queries
                if '_id' in update:
                    del update['_id']
            else:
                update['_id'] = random_id_value()
                if '_id' in query:
                    query = {random_compound_field_name(False): query['_id']}

    return {'query': query, 'update': update, 'upsert': upsert, 'multi': multi}


def random_object_id():
    return ObjectId(''.join([global_prng.choice('0123456789abcdef') for i in range(0, 24)]))


def random_query_sort():
    sort = list()

    r = global_prng.random()
    if r < 0:
        sort.append((random_compound_field_name(False), global_prng.choice([-1, 1])))
    elif r < 1:
        for i in range(0, 2):
            field_name = random_compound_field_name(False)
            if not any(field_name in d for d in sort):
                sort.append((field_name, global_prng.choice([-1, 1])))
    else:
        for i in range(0, 3):
            field_name = random_field_name()
            if not any(field_name in d for d in sort):
                sort.append((field_name, global_prng.choice([-1, 1])))
    return sort


def random_projection():
    if global_prng.random() < 0.02:
        return None

    inclusive = global_prng.random() < 0.5
    include_id = global_prng.random() < 0.05
    exclude_id = global_prng.random() < 0.5

    doc = OrderedDict()
    for i in range(0, global_prng.randint(0, 6)):
        if global_prng.random() < 0.05:
            el = (random_compound_field_name(False), random_value())
        else:
            value = inclusive

            if global_prng.random() < 0.02:
                value = not value

            el = (random_compound_field_name(False), value)

        doc[el[0]] = el[1]

    if include_id:
        doc['_id'] = True
    if exclude_id:
        doc['_id'] = False

    return doc
