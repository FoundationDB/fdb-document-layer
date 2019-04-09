#
# mongo_model.py
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

from util import *
from collections import OrderedDict
from copy import deepcopy
import gen
from dateutil.tz import tzutc
import datetime

operators = {
    "comparison": ['$gt', '$gte', '$in', '$lt', '$lte', '$ne', '$nin', '$eq'],
    "logical": ['$or', '$and', '$nor'],
    "logical_not": ['$not'],
    "element": ['$exists', '$type'],
    "array": ['$all', '$elemMatch', '$size'],
    "evaluation": ["$regex", "$mod", "$where", "$text", "$options"]
}


def elem_match_pred(value, query, options):
    if isinstance(value, list):
        if False not in [k.startswith("$") for k in query["$elemMatch"]]:
            for i in value:
                okay = True
                for k, v in query["$elemMatch"].items():
                    if not evaluate('', {k: v}, i, options):
                        okay = False
                        break
                if okay:
                    return True
        elif True not in [k.startswith("$") for k in query["$elemMatch"]]:
            if True in [
                    type(i) in [OrderedDict, HashableOrderedDict, list] and evaluate(
                        '$and', [{
                            k: v
                        } for k, v in query["$elemMatch"].items()],
                        OrderedDict([(str(k), v) for k, v in enumerate(i)])
                        if options.elem_match_convert_to_dict and type(i) is list else i, options) for i in value
            ]:
                return True

    if options.mongo6050_enabled:
        if False not in [k.startswith("$") for k in query["$elemMatch"]]:
            for k, v in query["$elemMatch"].items():
                if not evaluate('', {k: v}, value, options):
                    return False
            return True
        elif True not in [k.startswith("$") for k in query["$elemMatch"]]:
            return (type(value) in [OrderedDict, HashableOrderedDict, list] and evaluate(
                '$and', [{
                    k: v
                } for k, v in query["$elemMatch"].items()],
                OrderedDict([(str(k), v) for k, v in enumerate(value)])
                if options.elem_match_convert_to_dict and type(value) is list else value, options))

    return False


# Assumes the `document` passed in is a type of, or subtype of `OrderedDict`
def expand(field, document, check_last_array, expand_array, add_last_array, check_none_at_all, check_none_next, debug):
    ret = []

    if has_object(field, document):
        sub = get_object(field, document)
        if type(document) in [OrderedDict, HashableOrderedDict]:
            expand_array = True
        if type(sub) is list and check_last_array and expand_array:
            ret.extend(sub)
        if type(sub) is not list or add_last_array:
            ret.append(sub)
    else:
        if '.' in field:
            left = field.split('.')[0]
            right = field[field.find('.') + 1:]
            if has_object(left, document):
                sub = get_object(left, document)
                if type(sub) is OrderedDict:
                    ret.extend(
                        expand(right, sub, check_last_array, expand_array, add_last_array, check_none_at_all,
                               check_none_next, debug))
                elif type(sub) is list:
                    ret.extend(
                        expand(right, sub, check_last_array, False, add_last_array, check_none_at_all, False, debug))
                    for k in range(0, len(sub)):
                        if type(sub[k]) is OrderedDict:
                            ret.extend(
                                expand(right, sub[k], check_last_array, expand_array, add_last_array, check_none_at_all,
                                       check_none_next, debug))
                elif check_none_at_all and check_none_next:
                    ret.append(None)
            elif check_none_at_all and check_none_next and type(document) is OrderedDict:
                ret.append(None)
        elif type(document) is OrderedDict and check_none_at_all:
            ret.append(None)

    if debug:
        print ret

    return ret


def evaluate(field, query, document, options, debug=False):
    assert type(document) is not dict

    # Transform logical (and effectively logical) operators
    if field == '$and':
        return False not in [evaluate(q.keys()[0], q.values()[0], document, options, debug) for q in query]
    elif field == '$or':
        return True in [evaluate(q.keys()[0], q.values()[0], document, options) for q in query]
    elif field == '$nor':
        return True not in [evaluate(q.keys()[0], q.values()[0], document, options) for q in query]

    if type(query) == dict:
        if '$not' in query:
            return not evaluate(field, query['$not'], document, options)
        elif '$ne' in query:
            return not evaluate(field, query['$ne'], document, options)
        elif '$nin' in query:
            return not evaluate(field, {'$in': query['$nin']}, document, options)
        elif '$in' in query:
            return True in [evaluate(field, q, document, options, debug) for q in query['$in']]
        elif '$exists' in query and query['$exists'] is False:
            return not evaluate(field, {'$exists': True}, document, options, debug)
        elif '$all' in query:
            if len(query["$all"]) == 0:
                return False
            else:
                return evaluate('$and', [{field: k} for k in query["$all"]], document, options)

    # Comparison Predicate
    if type(query) != dict:

        def pred(value, query, options):
            if isinstance(value, OrderedDict):
                value = dict(value)
            if isinstance(value, OrderedDict):
                query = dict(query)
            return value == query
    elif '$ne' in query:
        assert False  # $nin should be transformed into literal match by previous recursive call
    elif '$lt' in query:

        def pred(value, query, options):
            return comparable(value, query['$lt']) and compare(value, query['$lt']) == -1
    elif '$lte' in query:

        def pred(value, query, options):
            return comparable(value, query['$lte']) and compare(value, query['$lte']) <= 0
    elif '$gt' in query:

        def pred(value, query, options):
            return comparable(value, query['$gt']) and compare(value, query['$gt']) == 1
    elif '$gte' in query:

        def pred(value, query, options):
            return comparable(value, query['$gte']) and compare(value, query['$gte']) >= 0
    elif '$exists' in query:

        def pred(value, query, options):
            return query['$exists']
    elif '$type' in query:

        def pred(value, query, options):
            return BSON_type_codes[type(value)] == query['$type']
    elif '$in' in query or '$nin' in query:
        assert False  # $nin an d $in should be transformed by previous recursive call
    elif '$size' in query:

        def pred(value, query, options):
            return type(value) is list and len(value) == query['$size']
    elif '$all' in query:
        assert False  # We can get rid of this, thank goodness
    elif '$elemMatch' in query:

        def pred(value, query, options):
            return elem_match_pred(value, query, options)
    elif '$regex' in query:

        def pred(value, query, options):
            return regex_predicate(value, query, options)
    elif '$not' in query:
        assert False  # $not should be transformed away by previous recursive call
    else:
        return True

    # Check Top-Level List?
    checkTopList = True
    if type(query) == dict and ('$type' in query):
        checkTopList = False

    # Check Within List?
    checkWithinList = True
    if type(query) == dict and (('$size' in query) or ('$all' in query) or ('$elemMatch' in query)):
        checkWithinList = False

    if field == '':
        values = [document]
    else:
        values = expand(
            field=field,
            document=document,
            check_last_array=checkWithinList,
            expand_array=True,
            add_last_array=checkTopList,
            check_none_at_all=(type(query) is not dict
                               or ("$exists" not in query and "$type" not in query and "$elemMatch" not in query)),
            check_none_next=True,
            debug=debug)

    if debug:
        print values

    if len(values) == 0:
        return False

    if True in [pred(v, query, options) for v in values]:
        return True
    else:
        return False


def is_dict(obj):
    return type(obj) == dict or type(obj) == OrderedDict


def is_simple(obj):
    return not is_dict(obj) and not type(obj) == list


class Projection(object):
    UNDEFINED = -1
    EXCLUSIVE = 0
    INCLUSIVE = 1

    class Node(object):
        def __init__(self, included=True):
            self.fields = {}
            self.included = included

        def sub(self, key):
            if key in self.fields:
                return self.fields[key]

            return Projection.Node(self.included)

        def __str__(self):
            return self.recursive_str()

        def recursive_str(self, first=True):
            s = ''
            if first and not self.included:
                s += '.'

            s += '{'
            for f, n in self.fields.items():
                if not n.included:
                    s += '.'

                if n.fields:
                    s += f + ': ' + n.recursive_str(False) + ', '
                else:
                    s += f + ', '

            if self.fields:
                s = s[0:-2]

            return s + '}'

    def __init__(self, projection_spec):
        self.mode = Projection.UNDEFINED
        self.include_id = True
        self.root = Projection.Node()

        self.parse(projection_spec)

    def parse(self, projection_spec):
        already_parsed = set()

        for field_name, value in projection_spec.items():
            if len(field_name) >= 2 and field_name[-2:] == '.$':
                # TODO: $ operator
                raise MongoModelException(".$ operator in projections not supported")
            elif is_dict(value):
                # TODO: $slice, $elemMatch, $meta operators
                raise MongoModelException("Objects in projections not supported")
            else:
                if value == 0 or value == None or value == False:
                    mode = Projection.EXCLUSIVE
                else:
                    mode = Projection.INCLUSIVE
                if field_name == '_id':
                    if self.mode == Projection.EXCLUSIVE and mode == Projection.INCLUSIVE:
                        raise MongoModelException("Cannot include and exclude fields in the same projection")

                    self.include_id = mode == Projection.INCLUSIVE
                else:
                    if self.mode == Projection.UNDEFINED:
                        self.mode = mode
                    elif self.mode != mode:
                        raise MongoModelException("Cannot include and exclude fields in the same projection")

                    if field_name in already_parsed:
                        continue

                    already_parsed.add(field_name)

                    field_parts = field_name.split('.')
                    current_node = self.root
                    for i, field in enumerate(field_parts):
                        if len(field) == 0:
                            current_node.fields['.'] = None
                            break

                        if field not in current_node.fields:
                            current_node.fields[field] = Projection.Node()

                        current_node = current_node.fields[field]

                        if i == len(field_parts) - 1:
                            current_node.included = self.mode == Projection.INCLUSIVE
                        else:
                            current_node.included = self.mode != Projection.INCLUSIVE

        if self.mode == Projection.UNDEFINED:
            if self.include_id:
                self.mode = Projection.INCLUSIVE
            else:
                self.mode = Projection.EXCLUSIVE

        self.root.included = self.mode != Projection.INCLUSIVE
        # print 'Parsed projection: %s' % self.root


def include_in_projection(projection, k, v, node):
    # print "Validating %r => %r with %s" % (k, v, node)
    return node.sub(k).included or (not is_simple(v) and node.sub(k).fields)


def project(documents, projection_fields):
    projection = Projection(projection_fields)
    projected_results = []
    for document in documents:
        results = {}
        # print "Projecting %r with %r" % (document, projection_fields)
        for k, v in document.items():
            # print "Checking %r => %r with %s" % (k, v, projection.root)
            if k == '_id':
                if projection.include_id:
                    results[k] = v
            elif k not in projection.root.fields:
                if projection.mode == Projection.EXCLUSIVE:
                    results[k] = v
            elif include_in_projection(projection, k, v, projection.root):
                results[k] = recursive_project(projection, k, v, projection.root)

        projected_results.append(results)

    return projected_results


def recursive_project(projection, key, value, node):
    # print "Projecting %r => %r with %s" % (key, value, node)

    if is_simple(value):
        return value
    elif is_dict(value):
        sub_node = node.sub(key)

        results = {}
        for k, v in value.items():
            if include_in_projection(projection, k, v, sub_node):
                results[k] = recursive_project(projection, k, v, sub_node)

        return results
    else:
        return [
            recursive_project(projection, key, v, node) for v in value
            if include_in_projection(projection, key, v, node)
        ]


class MongoModel(object):
    def __init__(self, compare_vs):
        self.options = ModelOptions(compare_vs)

    def __getitem__(self, dbname):
        MongoDatabase.connection = self
        return MongoDatabase(self.options, dbname)

    def list_database_names(self):
        return ['admin', 'test', 'local']


class MongoDatabase(object):
    def __init__(self, options, dbname=''):
        self.dbname = dbname
        self.options = options

    def __getitem__(self, collectionname):
        MongoCollection.database = self
        return MongoCollection(self.options, collectionname)

    def collection_names(self):
        return [
            'test',
        ]


# Return the corresponding DVTypeCode defined in DocLayer.
def getTypeCode(value):
    if value is None:
        return "20"
    elif isinstance(value, (long, float, int)):
        return "30"
    elif isinstance(value, binary.Binary):
        # this needs to come before basestring because `bson.binary.Binary` is also a subtype of `basestring`
        return "70"
    elif isinstance(value, basestring):
        return "40"
    elif isinstance(value, OrderedDict):
        return "51"
    elif isinstance(value, ObjectId):
        return "80"
    elif isinstance(value, datetime.datetime):
        return "100"
    else:
        raise MongoModelException("Unexpected value field type: " + str(type(value)))

# Sort the items by keys in the sort order (DVTypeCode) defined in QLTypes.h
class SortedDict(OrderedDict):
    def __init__(*args, **kwds):
        OrderedDict.__init__(*args, **kwds)
    def __setitem__(self, key, value, dict_setitem=dict.__setitem__):
        super(SortedDict, self).__setitem__(key, value, dict_setitem=dict_setitem)
        items = super(SortedDict, self).items()
        # group them together first
        tmp = OrderedDict()
        tmp["20"] = []
        tmp["30"] = []
        tmp["40"] = []
        tmp["51"] = []
        tmp["70"] = []
        tmp["80"] = []
        tmp["100"] = []
        for kv in items:
            tmp[getTypeCode(kv[0])].append(kv)
        # import pprint;pprint.pprint(dict(tmp))
        # sort each group
        sortedItems = []
        for typeCode, kvs in tmp.items():
            if typeCode == "51":
                sortedItems.extend(sorted(kvs, key=lambda kv: bson.BSON.encode(SortedDict.fromOrderedDict(kv[0]))))
            elif typeCode == "70":
                sortedItems.extend(sorted(kvs, key=lambda kv: str(len(kv[0][:-1])) + str(kv[0].subtype) + kv[0][:-1]))
            else:
                sortedItems.extend(sorted(kvs))
        # print str(sortedItems)
        super(SortedDict, self).clear()
        for k, v in sortedItems:
            super(SortedDict, self).__setitem__(k, v, dict_setitem=dict_setitem)

    @staticmethod
    def fromOrderedDict(orderedDict):
        sorted = SortedDict()
        for item in orderedDict.items():
            sorted[item[0]] = item[1]
        return sorted


class MongoCollection(object):
    def __init__(self, options, collectionname=''):
        self.collectionname = collectionname
        self.data = SortedDict()
        self.options = options
        self.mapUpdateOperator = {
            '$inc': self.process_update_operator_inc,
            '$mul': self.process_update_operator_mul,
            '$rename': self.process_update_operator_rename,
            '$setOnInsert': self.process_update_operator_set_on_insert,
            '$set': self.process_update_operator_set,
            '$unset': self.process_update_operator_unset,
            '$min': self.process_update_operator_min,
            '$max': self.process_update_operator_max,
            '$currentDate': self.process_update_operator_current_date,
            '$addToSet': self.process_update_operator_add_to_set,
            '$pop': self.process_update_operator_pop,
            '$pullAll': self.process_update_operator_pull_all,
            '$pull': self.process_update_operator_pull,
            '$push': self.process_update_operator_push,
            '$bit': self.process_update_operator_bit,
        }
        self.indexes = []

    def remove(self):
        self.data = SortedDict()

    def _insert(self, doc):
        if '_id' not in doc:
            doc['_id'] = gen.random_object_id()
        if doc['_id'] in self.data:
            raise MongoModelException("Duplicated value not allowed by unique index", code=11000)
        tmp = self.data.values() + [doc]
        for index in self.indexes:
            if not index.inError:
                index.validate_and_build_entry(tmp)
        self.data[doc['_id']] = deepcopy(doc)

    def insert(self, input):
        oldData = deepcopy(self.data)
        try:
            if isinstance(input, OrderedDict):
                self._insert(input)
            elif isinstance(input, list):
                all_ids = set()
                for i in input:
                    if '_id' in i:
                        if not self.options.object_field_order_matters and isinstance(i['_id'], HashableOrderedDict):
                            i['_id'] = HashableOrderedDict(sorted(i['_id'].items(), key=lambda (key, value): key))
                        if i['_id'] in all_ids:
                            # print i['_id']
                            raise MongoModelException("Duplicated value not allowed by unique index", code=11000)
                        all_ids.add(i['_id'])
                # All '_id's are good
                buffer = []
                # keep in mind when inserting many docs at once, the operation needs to be atomic, i.e. all or nothing.
                for doc in input:
                    if '_id' not in doc:
                        doc['_id'] = gen.random_object_id()
                    if doc['_id'] in self.data:
                        raise MongoModelException("Duplicated value not allowed by unique index", code=11000)
                    buffer.append(doc)
                tmp = self.data.values() + buffer
                for index in self.indexes:
                    if not index.inError:
                        index.validate_and_build_entry(tmp)
                # Ready to insert them all.
                for doc in buffer:
                    self.data[doc['_id']] = deepcopy(doc)
            else:
                raise MongoModelException("Tried to insert an unordered document.")
        except MongoModelException as e:
            self.data = oldData
            raise e

    def insert_one(self, dict):
        self.insert(dict)

    def insert_many(self, list):
        self.insert(list)

    def find(self, query, fields=None, batch_size=None):
        if len(query) == 0:
            results = self.data.values()
        else:
            assert len(query) == 1  # FIXME: test weakness
            k = query.keys()[0]
            results = [item for item in self.data.values() if evaluate(k, query[k], item, self.options)]

        if fields is None:
            return results
        else:
            return project(results, fields)

    def distinct(self, field, filter=None):
        distinct_values = set()
        if filter is None:
            try:
                return reduce(operator.getitem, field.split('.'), self.data)
            except KeyError as e:
                raise MongoModelException("Bad filed path: " + field)
        else:
            query_result = self.find(filter)
            try:
                for doc in query_result:
                    distinct_values.add(reduce(operator.getitem, field.split('.'), doc))
                return list(distinct_values)
            except KeyError as e:
                raise MongoModelException("Bad filed path: " + field)

    def replace(self, key, new_value):
        if "_id" in new_value:
            raise MongoModelException("The _id field cannot be changed", code=16836)
        _id = self.data[key]['_id']
        self.data[key] = deepcopy(new_value)
        self.data[key]['_id'] = _id

    def drop(self):
        self.drop_indexes()
        self.remove()

    # So that we can use PyMongo and MongoModel implementations of a "collection" interchangeably
    def drop_indexes(self):
        self.indexes = []

    def ensure_index(self, keys, **kwargs):
        return self._create_index(keys, kwargs)

    # Only support online index build. No background build yet.
    def create_index(self, keys, **kwargs):
        return self._create_index(keys, kwargs)

    def _create_index(self, keys, kwargs):
        def _get_index_name(keys):
            return "_".join(["%s_%s" % item for item in keys])
        kwargs.setdefault("name", _get_index_name(keys))
        seen = set()
        deduplicatedKeys = []
        for key in keys:
            if key[0] not in seen:
                deduplicatedKeys.append(key)
                seen.add(key[0])
        for i in self.indexes:
            if i.keys == deduplicatedKeys:
                # There was an index that indexes on the same field(s), abort here.
                return None
            if i.name == kwargs["name"]:
                raise MongoModelException("There is an index with this name and a different key spec", code=29993)

        if "unique" in kwargs.keys() and kwargs["unique"]:
            newIndex = MongoUniqueIndex(deduplicatedKeys, kwargs)
        else:
            newIndex = MongoIndex(deduplicatedKeys, kwargs)

        self.indexes.append(newIndex) # insert first, since an index can be added but in error state if its constraints are violated.
        newIndex.build(self.data.values())
        return newIndex.name

    @staticmethod
    def validate_update_object(update):
        affected_fields = set()
        prefixes_of_affected_fields = set()
        for operator_name in update:
            if not update[operator_name] or len(update[operator_name]) == 0:
                raise MongoModelException('Update operator has empty object for parameter. You must specify a field.', code=26840)
            for field_name in update[operator_name]:
                if field_name in affected_fields:
                    raise MongoModelException('Field name duplication not allowed with modifiers', code=10150)
                if field_name in prefixes_of_affected_fields:
                    raise MongoModelException('have conflicting mods in update', code=10151)
                affected_fields.add(field_name)
                for i in range(0, len(field_name)):
                    if field_name[i] == '.':
                        if field_name[:i] in affected_fields:
                            raise MongoModelException('have conflicting mods in update', code=10151)
                        prefixes_of_affected_fields.add(field_name[:i])

            if operator_name == '$rename':
                for field_name in update[operator_name]:
                    rename_target = update[operator_name][field_name]
                    if not isinstance(rename_target, basestring):
                        raise MongoModelException('$rename target must be a string', code=13494)
                    if rename_target in affected_fields:
                        raise MongoModelException('Field name duplication not allowed with modifiers', code=10150)
                    if rename_target in prefixes_of_affected_fields:
                        raise MongoModelException('have conflicting mods in update', code=10151)
                    affected_fields.add(rename_target)
                    for i in range(0, len(rename_target)):
                        if rename_target[i] == '.':
                            if rename_target[:i] in affected_fields:
                                raise MongoModelException('have conflicting mods in update', code=10151)
                            prefixes_of_affected_fields.add(rename_target[:i])

    def process_update_operator_inc(self, key, update_expression):
        # print "Update Operator: $inc ", update

        # validation check: if all fields updated are numerical
        for k, v in update_expression.iteritems():
            if k in self.data[key]:
                # print alert("%s: %s" % (self.data[key][k], str(v)))
                if not isinstance(self.data[key][k], (int, long, float)):
                    # print "Filed \"", k, "\" is not numerical type!"
                    raise MongoModelException('Cannot apply $inc to a value of non-numeric type.', code=10140)

        for k, v in update_expression.iteritems():
            # print "Inc: key: ", k, " value: ", v
            if k in self.data[key]:
                self.data[key][k] = self.data[key][k] + v
            else:
                self.data[key][k] = v

    def process_update_operator_mul(self, key, update_expression):
        # print "Update Operator: $mul ", update

        # validation check: if all fields updated are numerical
        for k, v in update_expression.iteritems():
            if k in self.data[key]:
                if not isinstance(self.data[key][k], (int, long, float)):
                    # print "Field \"", k, "\" is not numerical type!"
                    raise MongoModelException('Cannot apply $mul to a value of non-numeric type.', code=16837)

        for k, v in update_expression.iteritems():
            # print "Mul: key: ", k, " value: ", v
            if k in self.data[key]:
                self.data[key][k] = self.data[key][k] * v
            else:
                self.data[key][k] = 0

    def process_update_operator_rename(self, key, update_expression):
        # print "Update Operator: $rename ", update
        for k, v in update_expression.iteritems():
            # print "Rename: key: ", k, " value: ", v
            if k in self.data[key]:
                self.data[key][v] = self.data[key][k]
                del self.data[key][k]

    def process_update_operator_set_on_insert(self, key, update_expression, new_doc=False):
        # print "Update Operator: $setOnInsert ", key, update_expression
        if new_doc:
            self.data[key] = OrderedDict(self.data[key].items() + update_expression.items())

    def process_update_operator_set(self, key, update_expression):
        # print "Update Operator: $set ", update
        self.data[key] = OrderedDict(self.data[key].items() + update_expression.items())

    def process_update_operator_unset(self, key, update_expression):
        # print "Update Operator: $unset ", update
        for k in update_expression.keys():
            if k in self.data[key]:
                del self.data[key][k]

    def process_update_operator_min(self, key, update_expression):
        # print "Update Operator: $min ", update
        for k, v in update_expression.iteritems():
            # print "Inc: key: ", k, " value: ", v
            if k in self.data[key]:
                # The $min updates the value of the field to a specified value if the
                # specified value is less than the current value of the field.
                if mongo_compare_value(self.data[key][k], v) > 0:
                    self.data[key][k] = v
            else:
                # If the field does not exists, the $min operator sets the field to the specified value.
                self.data[key][k] = v

    def process_update_operator_max(self, key, update_expression):
        # print "Update Operator: $max ", update
        for k, v in update_expression.iteritems():
            # print "Inc: key: ", k, " value: ", v
            if k in self.data[key]:
                # The $max operator updates the value of the field to a specified value if the
                # specified value is greater than the current value of the field
                if mongo_compare_value(self.data[key][k], v) < 0:
                    self.data[key][k] = v
            else:
                # If the field does not exists, the $max operator sets the field to the specified value.
                self.data[key][k] = v

    def process_update_operator_current_date(self, key, update_expression):
        # print "Update Operator: $currentDate", update_expression
        for k, v in update_expression.iteritems():
            if v:
                # mongoDB use the UTC time
                self.data[key][k] = datetime.datetime.now(tzutc())
            elif v == {'$type': "timestamp"}:
                self.data[key][k] = bson.timestamp.Timestamp(datetime.datetime.now(tzutc()), 1)
            elif v == {'$type': "date"}:
                self.data[key][k] = datetime.datetime.now(tzutc())

    @staticmethod
    def append_each(dst, src):
        if isinstance(src, (dict, OrderedDict)) and '$each' in src:
            for item in src['$each']:
                if item not in dst:
                    dst.append(item)
        else:
            if src not in dst:
                # print "AddToSet Array", dst, src
                dst.append(src)
            else:
                pass
            #  print "Value", src, "already in dst!", dst

    def process_update_operator_add_to_set(self, key, update_expression):
        # print "Update Operator: $addToSet ", update_operator, update
        for k, v in update_expression.iteritems():
            # print "Inc: key: ", k, " value: ", v
            if k in self.data[key]:
                if isinstance(self.data[key][k], list):
                    self.append_each(self.data[key][k], v)
                else:
                    # print "Field \"", k, "\" is not array type!"
                    raise MongoModelException("Cannot apply $addToSet to a non-array field.", code=12591)
            else:
                self.data[key][k] = []
                self.append_each(self.data[key][k], v)

    def process_update_operator_pop(self, key, update_expression):
        # print "Update Operator: $pop ", update_operator, update
        for k, v in update_expression.iteritems():
            # print "Inc: key: ", k, " value: ", v
            if k in self.data[key]:
                if isinstance(self.data[key][k], list):  # and len(self.data[key][k]) > 0:
                    if len(self.data[key][k]) > 0:
                        if v == 1:
                            del self.data[key][k][-1]
                        elif v == -1:
                            del self.data[key][k][0]
                        else:
                            pass
                            # print "Value", v, "should be 1 or -1!"
                else:
                    # print "Field \"", k, "\" is not array type!"
                    raise MongoModelException("Cannot apply $pop to a non-array field.", code=10143)

    def process_update_operator_pull_all(self, key, update_expression):
        # print "Update Operator: $pullAll ", update_operator, update
        for k, v in update_expression.iteritems():
            # print "Inc: key: ", k, " value: ", v
            if k in self.data[key]:
                if isinstance(self.data[key][k], list):
                    if len(self.data[key][k]) > 0:
                        self.data[key][k] = [x for x in self.data[key][k] if x not in v]
                else:
                    # print "Field \"", k, "\" is not array type!"
                    raise MongoModelException("Cannot apply $pullAll to a non-array field.", code=10142)

    def evaluate(self, query, document):
        if len(query) == 0:
            return True # match empty query, since coll.find({}) returns all docs
        acc = True
        for field in query.keys():
            if field == '_id':
                tmp = OrderedDict()
                for k,v in sorted(query[field].items(), key= lambda i: i[0]):
                    tmp[k] = v
                acc = acc and evaluate(field, tmp, document, self.options, True)
            else:
                acc = acc and evaluate(field, query[field], document, self.options, True)
        return acc

    def process_update_operator_pull(self, key, update_expression):
        # print "Update Operator: $pull ", update
        for k, v in update_expression.iteritems():
            # print "$pull: key: ", k, " value: ", v
            if k in self.data[key]:
                if isinstance(self.data[key][k], list):
                    if len(self.data[key][k]) > 0:
                        if isinstance(v, (dict, OrderedDict)):
                            for item in self.data[key][k][:]:
                                if isinstance(item, OrderedDict):
                                    # print "k:", k, "v:", v, "item:", item
                                    if self.evaluate(v, item):
                                        # print "item match0!, remove: ", item
                                        self.data[key][k].remove(item)
                        elif isinstance(v, list):
                            if item in v:
                                # print "item match1!, remove: ", item
                                self.data[key][k].remove(item)
                        else:
                            self.data[key][k] = [x for x in self.data[key][k] if x != v]
                else:
                    # print "Field \"", k, "\" is not array type!"
                    raise MongoModelException("Cannot apply $pull to a non-array field.", code=10142)

    def process_update_operator_push(self, key, update_expression):
        # print "Update Operator: $push ", update
        for k, v in update_expression.iteritems():
            # print "Push: key: ", k, " value: ", v
            if k in self.data[key]:
                if isinstance(self.data[key][k], list):
                    if isinstance(v, (dict, OrderedDict)) and '$each' in v:
                        if '$position' in v:
                            position = v['$position']
                            self.data[key][k] = deep_convert_to_ordered(self.data[key][k][:position] + v['$each'] +
                                                                        self.data[key][k][position:])
                        else:
                            for item in v['$each']:
                                self.data[key][k].append(item)
                    else:
                        self.data[key][k].append(v)
                else:
                    # print "Field \"", k, "\" is not array type!"
                    raise MongoModelException("Cannot apply $push to a non-array field.", code=10141)
            else:
                # If the field is absent in the document to update, $push adds the array field with the value as its element.
                if isinstance(v, (dict, OrderedDict)) and '$each' in v:
                    self.data[key][k] = []
                    if '$position' in v:
                        position = v['$position']
                        self.data[key][k] = deep_convert_to_ordered(self.data[key][k][:position] + v['$each'] +
                                                                    self.data[key][k][position:])
                    else:
                        for item in v['$each']:
                            self.data[key][k].append(item)
                else:
                    self.data[key][k] = [v]

            # for $slice
            if isinstance(v, (dict, OrderedDict)) and '$slice' in v:
                if v['$slice'] == 0:
                    self.data[key][k] = []
                elif v['$slice'] > 0:
                    self.data[key][k] = self.data[key][k][:v['$slice']]
                else:
                    self.data[key][k] = self.data[key][k][v['$slice']:]

            # for $sort
            if isinstance(v, (dict, OrderedDict)) and '$sort' in v:
                if isinstance(v['$sort'], (dict, OrderedDict)):
                    self.data[key][k] = mongo_sort_list_by_fields(self.data[key][k], v['$sort'])
                else:
                    self.data[key][k] = mongo_sort_list(self.data[key][k], reverse=(v['$sort'] != 1))

    def process_update_operator_bit(self, key, update_expression):
        # print "Update Operator: $bit ", update_operator, update
        # validation check: if all fields updated are numerical
        for k, v in update_expression.iteritems():
            if k in self.data[key]:
                if not isinstance(self.data[key][k], (int, long)):
                    # print "Filed \"", k, "\" is not numerical type!"
                    raise MongoModelException('Cannot apply $bit to a value of non-integeral type.', code=10138)

        for k, v in update_expression.iteritems():
            # print "Bit: key: ", k, " value: ", v
            if k not in self.data[key]:
                self.data[key][k] = 0
            bit_operator = v.keys()[0]
            bit_num = v[v.keys()[0]]
            if not isinstance(bit_num, (int, long, float)):
                raise MongoModelException('$bit field must be a number', code=10139)
            # print "op:", bit_operator, "num:", bit_num, "self.data[key][k]:", self.data[key][k]
            if bit_operator == "and":
                self.data[key][k] = self.data[key][k] & bit_num
            elif bit_operator == "or":
                self.data[key][k] = self.data[key][k] | bit_num
            elif bit_operator == "xor":
                self.data[key][k] = self.data[key][k] ^ bit_num

    @staticmethod
    def has_operator_expressions(doc):
        fields = [k.startswith('$') for k in doc]
        if True not in fields:
            return False
        elif False not in fields:
            return True
        else:
            raise MongoModelException('Cannot mix operator and literal updates')

    def process_update_operator(self, key, update, new_doc=False):
        op_update = self.has_operator_expressions(update)
        old_data = None

        try:
            old_data = deepcopy(self.data[key])
            if op_update:
                for k in update:
                    if k == '$setOnInsert':
                        self.mapUpdateOperator[k](key, update[k], new_doc=new_doc)
                    elif k in self.mapUpdateOperator:
                        self.mapUpdateOperator[k](key, update[k])
            else:
                self.replace(key, update)
        except MongoModelException as e:
            self.data[key] = old_data
            raise e

    def deep_transform_logical_operators(self, selector=None):
        new_selector = {}

        if not selector:
            return None

        for k, v in selector.iteritems():
            if k in operators['logical']:
                if isinstance(v, list):
                    if k == '$or' and len(v) > 1:
                        new_selector = new_selector
                    elif k == '$nor':
                        new_selector = new_selector
                    else:
                        for i in v:
                            if isinstance(i, dict):
                                for kk, vv in i.iteritems():
                                    result = has_operator(kk)
                                    if result:
                                        logical_child = self.deep_transform_logical_operators(i)
                                        for kkk, vvv in logical_child.iteritems():
                                            new_selector[kkk] = vvv
                                    else:
                                        new_selector[kk] = vv
                else:
                    new_selector = None
                    break

            else:
                new_selector[k] = v
        return new_selector

    def transform_operator_query_to_updatable_document(self, selector, depth=0):

        selector = OrderedDict(selector)
        clone = dict(selector.items())
        operators_found = []

        for k in selector:
            operator_found = has_operator(k)
            operator_found and operators_found.append(operator_found)
            if not operator_found and len(operators_found):
                if depth == 0:
                    for op, _ in operators_found:
                        if op not in operators['logical']:
                            raise MongoModelException("bad query!")
                else:
                    raise MongoModelException("bad query!")

            if operator_found:
                operator, key_not_str = operator_found
                if key_not_str:
                    raise MongoModelException("bad query!")

                if k in operators['logical']:
                    if depth == 0:
                        logical_operator_tranformation = self.deep_transform_logical_operators(clone)
                        if logical_operator_tranformation != None:
                            clone.update(logical_operator_tranformation)
                            del clone[k]
                        else:
                            raise MongoModelException("bad query!")
                    else:
                        raise MongoModelException("bad query!")

                if k in operators['array']:
                    if depth == 0:
                        raise MongoModelException("bad query!")
                    elif depth > 1:
                        raise MongoModelException("bad query!")
                    else:
                        if k == '$all':
                            if not isinstance(selector[k], list) or len(selector[k]) > 1:
                                raise MongoModelException("bad query!")
                            elif not selector[k]:
                                del clone[k]
                            else:
                                clone = selector[k][0]
                        elif k == '$size' and selector[k] is None:
                            raise MongoModelException("bad query!")
                        else:
                            del clone[k]

                if k in operators['comparison']:
                    if depth == 0 or depth > 1:
                        raise MongoModelException("bad query!")
                    else:
                        if k == '$in':
                            if not isinstance(selector[k], list):
                                raise MongoModelException("bad query!")
                            else:
                                del clone[k]
                        elif k == '$eq':
                            clone = selector[k]
                        elif clone:
                            del clone[k]

                if k in operators['element'] or k in operators['logical_not'] or k in operators['evaluation']:
                    if depth == 0 or depth > 1:
                        raise MongoModelException("bad query!")
                    else:
                        del clone[k]

            if clone and isinstance(selector[k], dict):
                clone[k] = self.transform_operator_query_to_updatable_document(selector[k], depth + 1)
                if clone[k] is None:
                    raise MongoModelException("bad query!")
                elif clone[k] == {}:
                    del clone[k]
        return deep_convert_compound_string_to_dict(clone)

    def transform_operator_query_to_upsert(self, selector):

        selector = OrderedDict(selector)

        operator, depth = has_operator(selector)
        if (depth == 1 and operator in operators['logical']):
            selector = self.deep_transform_logical_operators(deep_convert_compound_string_to_dict(selector))
        elif (depth == 2 and operator in operators['array']):
            if operator == '$all':
                selector = self.deep_transform_logical_operators(deep_convert_compound_string_to_dict(selector))
            else:
                for k, v in selector.iteritems():
                    count = 0
                    if isinstance(v, dict):
                        for kk, vv in v.iteritems():
                            count += 1
                            if kk == operator:
                                selector = {}
                    if count > 1:
                        raise MongoModelException("bad query!")
        elif (depth == 2 and operator not in operators['logical']):
            for k, v in selector.iteritems():
                if isinstance(v, dict):
                    for kk, vv in v.iteritems():
                        if kk == operator:
                            selector = {}
                            break
        else:
            raise MongoModelException("bad query!")
        return selector

    def update(self, query, update, upsert, multi):
        isOperatorUpdate = self.has_operator_expressions(update)
        if not isOperatorUpdate and multi:
            raise MongoModelException('multi update only works with $ operators', code=10158)
        if isOperatorUpdate:
            self.validate_update_object(update)
        if len(query) == 0:
            return
        key = query.keys()[0]
        any = False
        old_data = deepcopy(self.data)
        n = 0
        try:
            for k, item in self.data.iteritems():
                if evaluate(key, query[key], item, self.options):
                    any = True
                    n += 1
                    # print "Result: ", item
                    if len(update) > 0:
                        self.process_update_operator(k, update)
                        if not multi:
                            return
                    else:
                        self.replace(k, update)
                        if not multi:
                            return
            if any:
                for index in self.indexes:
                    if not index.inError:
                        index.validate_and_build_entry(self.data.values())
        except MongoModelException as e:
            # print "*********************** Reseting update due to {}".format(e)
            self.data = old_data
            raise e

        # need to create a new doc
        if upsert and not any:
            n += 1
            if self.has_operator_expressions(update):
                new_id = None
                try:
                    # this case break determinism when tested against non-model things
                    if "_id" not in query:
                        new_id = gen.random_object_id()
                    else:
                        new_id = query["_id"]
                    if has_operator(query):
                        #self.data[new_id] = deepcopy(self.transformOperatorQueryToUpsert(query))
                        self.data[new_id] = deepcopy(self.transform_operator_query_to_updatable_document(query))
                    else:
                        self.data[new_id] = OrderedDict(query)
                    if self.data[new_id] is not None:
                        self.data[new_id]['_id'] = new_id
                        self.process_update_operator(new_id, update, new_doc=True)
                    else:
                        del self.data[new_id]
                    for index in self.indexes:
                        if not index.inError:
                            index.validate_and_build_entry(self.data.values())
                except MongoModelException as e:
                    # print "delete new_id", new_id, "because of the exception"
                    if new_id in self.data:
                        del self.data[new_id]
                    raise e
            else:
                if "_id" in query:
                    update["_id"] = query["_id"]
                self.insert(update)
        else:
            # mongoDB raise an exception for the '$setOnInsert' update operator even if the upsert is False
            if '$setOnInsert' in update.keys() and len(update['$setOnInsert']) == 0:
                raise MongoModelException(
                    "'$setOnInsert' is empty. You must specify a field like so: {$mod: {<field>: ...}}", code=26840)
        return {'n': n}

    def update_one(self, query, update, upsert):
        self.update(query, update, upsert, multi=False)

    def update_many(self, query, update, upsert):
        self.update(query, update, upsert, multi=True)


class MongoIndex(object):
    def __init__(self, indexKeys, kwargs):
        self.name = kwargs["name"]
        self.inError = False
        if len(indexKeys) == 1:
            if indexKeys[0][1] == "text" :
                raise MongoModelException("Document Layer does not support this index type, yet.", code=29969)
            elif (type(indexKeys[0][1]) is int and abs(indexKeys[0][1]) != 1) or (type(indexKeys[0][1]) is not int):
                raise MongoModelException("Index must be 1 for ascending or -1 for descending", code=29991)
        else :
            first = True
            ascending = True
            for key in indexKeys:
                if key[1] == "text":
                    raise MongoModelException("Document Layer does not support this index type, yet.", code=29969)
                elif (type(key[1]) is int and abs(key[1]) != 1) or (type(key[1]) is not int):
                    raise MongoModelException("Index must be 1 for ascending or -1 for descending", code=29991)
                if first:
                    first = False
                    ascending = (key[1] == 1)
                else:
                    if ascending != (key[1] == 1):
                        raise MongoModelException("Mixed order compound indexes are not supported", code=29990)
        self.keys = indexKeys
        if len(self.keys) == 1:
            self.isSimple = True
        else:
            self.isSimple = False
        # self check
        self.validate_self()

    # Invariants to check:
    #    - Name cannot be empty, otherwise throw error code 29967
    def validate_self(self):
        if self.name is None or self.name == "":
            self.inError = True
            raise MongoModelException("No index name specified", code=29967)

    def build(self, documents):
        self.validate_and_build_entry(documents, first_build=True)

    # Validate the entry(ies) that will be built on this particular document for the following invariants:
    #    - If it's compund index, the cartesian product of all index values cannot exceed 1000
    def validate_and_build_entry(self, documents, first_build=False):
        for document in documents:
            nValues = 1
            for key in self.keys:
                values = expand(
                    field=key[0],
                    document=document,
                    check_last_array=True,
                    expand_array=True,
                    add_last_array=False,
                    check_none_at_all=True,
                    check_none_next=True,
                    debug=False)
                nValues * len(values)
            if nValues > 1000 and not self.isSimple:
                self.inError = first_build
                raise MongoModelException("Multi-multikey index size exceeds maximum value.", code=0)

class MongoUniqueIndex(MongoIndex):
    def __init__(self, indexKeys, kwargs):
        super(MongoUniqueIndex, self).__init__(indexKeys, kwargs)

    def build(self, documents):
        super(MongoUniqueIndex, self).validate_and_build_entry(documents, first_build=True)
        self.validate_and_build_entry(documents, first_build=True)

    # Validate the entry(ies) that will be built on this particular document for the following invariants:
    #    - No duplicates
    def validate_and_build_entry(self, documents, first_build=False):
        seen = set()
        for document in documents:
            entry = ""
            for key in self.keys:
                _values = expand(
                    field=key[0],
                    document=document,
                    check_last_array=True,
                    expand_array=True,
                    add_last_array=False,
                    check_none_at_all=True,
                    check_none_next=True,
                    debug=False)
                entry = entry + reduce((lambda acc, x: acc + x), map((lambda x: str(x)), _values), "")
            if entry in seen:
                # print "[{}] in {} ? {}".format(entry, seen, entry in seen)
                # print "Violating keys " + str(key)
                # print "Duplicated value: " + entry
                # import pprint
                # print "Violation doc"
                # pprint.pprint(dict(document))
                self.inError = first_build
                raise MongoModelException("Duplicated value not allowed by unique index", code=11000)
            else:
                # print "Inserting {} for key {}".format(entry, key)
                seen.add(entry)
