/*
 * ExtOperator.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * MongoDB is a registered trademark of MongoDB, Inc.
 */

#include "Ext.h"
#include "ExtMsg.h"
#include "ExtOperator.h"
#include "ExtUtil.actor.h"

#include "ordering.h"

#include "QLExpression.h"

using namespace FDB;

Reference<IPredicate> queryToPredicate(bson::BSONObj const& query, bool toplevel = false);
Reference<IPredicate> valueQueryToPredicate(bson::BSONObj const& query, std::string const& path);
void execute(Reference<QueryContext> cx, StringRef const& path, bson::BSONElement const& element);

struct ExtValueOperatorIn {
	static const char* name;
	static Reference<IPredicate> toPredicate(std::string const& unencoded_path, bson::BSONElement const& element) {
		std::vector<Reference<IPredicate>> terms;
		for (auto it = element.Obj().begin(); it.more();) {
			auto el = it.next();
			terms.push_back(eq_predicate(el, unencoded_path));
		}
		return ref(new OrPredicate(terms));
	}
};
REGISTER_VALUE_OPERATOR(ExtValueOperatorIn, "$in");

struct ExtValueOperatorNin {
	static const char* name;
	static Reference<IPredicate> toPredicate(std::string const& unencoded_path, bson::BSONElement const& element) {
		std::vector<Reference<IPredicate>> terms;
		// We can do this *despite* Mongo array semantics, because those semantics inconsistently treat a
		// $ne like a $not: {path: {$in: element}}.
		for (auto it = element.Obj().begin(); it.more();) {
			auto el = it.next();
			terms.push_back(ref(new NotPredicate(eq_predicate(el, unencoded_path))));
		}
		return ref(new AndPredicate(terms));
	}
};
REGISTER_VALUE_OPERATOR(ExtValueOperatorNin, "$nin");

struct ExtValueOperatorGt {
	static const char* name;
	static Reference<IPredicate> toPredicate(std::string const& unencoded_path, bson::BSONElement const& element) {
		DataValue min(element);
		return any_predicate(unencoded_path,
		                     ref(new RangePredicate(min, false, DataValue::firstOfnextType(min), false)));
	}
};
REGISTER_VALUE_OPERATOR(ExtValueOperatorGt, "$gt");

struct ExtValueOperatorGte {
	static const char* name;
	static Reference<IPredicate> toPredicate(std::string const& unencoded_path, bson::BSONElement const& element) {
		DataValue min(element);
		return any_predicate(unencoded_path,
		                     ref(new RangePredicate(min, true, DataValue::firstOfnextType(min), false)));
	}
};
REGISTER_VALUE_OPERATOR(ExtValueOperatorGte, "$gte");

struct ExtValueOperatorLt {
	static const char* name;
	static Reference<IPredicate> toPredicate(std::string const& unencoded_path, bson::BSONElement const& element) {
		DataValue max(element);
		return any_predicate(unencoded_path, ref(new RangePredicate(DataValue::firstOfType(max), true, max, false)));
	}
};
REGISTER_VALUE_OPERATOR(ExtValueOperatorLt, "$lt");

struct ExtValueOperatorLte {
	static const char* name;
	static Reference<IPredicate> toPredicate(std::string const& unencoded_path, bson::BSONElement const& element) {
		DataValue max(element);
		return any_predicate(unencoded_path, ref(new RangePredicate(DataValue::firstOfType(max), true, max, true)));
	}
};
REGISTER_VALUE_OPERATOR(ExtValueOperatorLte, "$lte");

struct ExtValueOperatorNe {
	static const char* name;
	static Reference<IPredicate> toPredicate(std::string const& unencoded_path, bson::BSONElement const& element) {
		// We can do this *despite* Mongo array semantics, because those semantics inconsistently treat a
		// $ne like a $not: {path: element}.
		return ref(new NotPredicate(eq_predicate(element, unencoded_path)));
	}
};
REGISTER_VALUE_OPERATOR(ExtValueOperatorNe, "$ne");

struct ExtValueOperatorExists {
	static const char* name;
	static Reference<IPredicate> toPredicate(std::string const& unencoded_path, bson::BSONElement const& element) {
		auto e = any_predicate(unencoded_path, ref(new AllPredicate()));
		if (element.trueValue()) {
			return e;
		} else {
			return ref(new NotPredicate(e));
		}
	}
};
REGISTER_VALUE_OPERATOR(ExtValueOperatorExists, "$exists");

struct ExtValueOperatorType {
	static const char* name;
	static Reference<IPredicate> toPredicate(std::string const& unencoded_path, bson::BSONElement const& element) {
		return any_predicate(unencoded_path, ref(new BSONTypePredicate((bson::BSONType)(int)element.Number())));
	}
};
REGISTER_VALUE_OPERATOR(ExtValueOperatorType, "$type");

struct ExtValueOperatorNot {
	static const char* name;
	static Reference<IPredicate> toPredicate(std::string const& unencoded_path, bson::BSONElement const& element) {
		return ref(
		    new NotPredicate(queryToPredicate(bson::BSONObjBuilder().appendAs(element, unencoded_path.c_str()).obj())));
	}
};
REGISTER_VALUE_OPERATOR(ExtValueOperatorNot, "$not");

struct ExtValueOperatorSize {
	static const char* name;
	static Reference<IPredicate> toPredicate(std::string const& unencoded_path, bson::BSONElement const& element) {
		return ref(
		    new AnyPredicate(ref(new ExtPathExpression(unencoded_path, false, false)),
		                     ref(new ArraySizePredicate(element.Number()))));
	}
};
REGISTER_VALUE_OPERATOR(ExtValueOperatorSize, "$size");

struct ExtValueOperatorElemMatch {
	static const char* name;
	static Reference<IPredicate> toPredicate(std::string const& unencoded_path, bson::BSONElement const& element) {
		// Not forcing the last element of the path to match an array "fixes" MongoDB bug SERVER-6050, making it
		// possible to do e.g. range queries on nonarray fields of subdocuments in an array
		Reference<IPredicate> pred;
		try {
			pred = queryToPredicate(element.Obj());
			pred = ref(new AndPredicate(pred, ref(new IsObjectPredicate)));
		} catch (Error&) {
			pred = valueQueryToPredicate(element.Obj(), std::string());
		}
		return any_predicate(unencoded_path, pred, true);
	}
};
REGISTER_VALUE_OPERATOR(ExtValueOperatorElemMatch, "$elemMatch");

struct ExtValueOperatorAll {
	static const char* name;
	static Reference<IPredicate> toPredicate(std::string const& unencoded_path, bson::BSONElement const& element) {
		std::vector<Reference<IPredicate>> terms;
		if (element.Array().size() == 0) {
			return ref(new NonePredicate());
		} else {
			for (auto e : element.Array()) {
				if (e.isABSONObj() && e.Obj().hasField("$elemMatch")) {
					terms.push_back(valueQueryToPredicate(e.Obj(), unencoded_path));
				} else {
					terms.push_back(eq_predicate(e, unencoded_path));
				}
			}
			return ref(new AndPredicate(terms));
		}
	}
};
REGISTER_VALUE_OPERATOR(ExtValueOperatorAll, "$all");

struct ExtValueOperatorEq {
	static const char* name;
	static Reference<IPredicate> toPredicate(std::string const& unencoded_path, bson::BSONElement const& element) {
		return eq_predicate(element, unencoded_path);
	}
};
REGISTER_VALUE_OPERATOR(ExtValueOperatorEq, "$eq");

struct ExtValueOperatorDebug_None {
	static const char* name;
	static Reference<IPredicate> toPredicate(std::string const& unencoded_path, bson::BSONElement const& element) {
		return ref(new NonePredicate);
	}
};
REGISTER_VALUE_OPERATOR(ExtValueOperatorDebug_None, "$DEBUG_none");

struct ExtValueOperatorRegEx {
	static const char* name;
	static Reference<IPredicate> toPredicate(std::string const& unencoded_path, bson::BSONElement const& element) {
		return re_predicate(element, unencoded_path);
	}
};
REGISTER_VALUE_OPERATOR(ExtValueOperatorRegEx, "$regex");

struct ExtBoolOperatorAnd {
	static const char* name;
	static Reference<IPredicate> toPredicate(bson::BSONObj const& obj) {
		std::vector<Reference<IPredicate>> terms;
		for (auto it = obj.begin(); it.more();) {
			auto el = it.next();
			terms.push_back(queryToPredicate(el.Obj()));
		}
		return ref(new AndPredicate(terms));
	}
};
REGISTER_BOOL_OPERATOR(ExtBoolOperatorAnd, "$and");

struct ExtBoolOperatorOr {
	static const char* name;
	static Reference<IPredicate> toPredicate(bson::BSONObj const& obj) {
		std::vector<Reference<IPredicate>> terms;
		for (auto it = obj.begin(); it.more();) {
			auto el = it.next();
			terms.push_back(queryToPredicate(el.Obj()));
		}
		return ref(new OrPredicate(terms));
	}
};
REGISTER_BOOL_OPERATOR(ExtBoolOperatorOr, "$or");

struct ExtBoolOperatorNor {
	static const char* name;
	static Reference<IPredicate> toPredicate(bson::BSONObj const& obj) {
		return ref(new NotPredicate(ExtBoolOperator::toPredicate("$or", obj)));
	}
};
REGISTER_BOOL_OPERATOR(ExtBoolOperatorNor, "$nor");

struct ExtUpdateOperatorSet {
	static const char* name;
	static Future<Void> execute(Reference<IReadWriteContext> cx,
	                            StringRef const& path,
	                            bson::BSONElement const& element) {
		ASSERT(path == encodeMaybeDotted(element.fieldName()));
		cx->getSubContext(path)->clearDescendants();
		insertElementRecursive(element, cx);
		return Void();
	}
};
REGISTER_UPDATE_OPERATOR(ExtUpdateOperatorSet, "$set");

struct ExtUpdateOperatorUnset {
	static const char* name;
	static Future<Void> execute(Reference<IReadWriteContext> cx,
	                            StringRef const& path,
	                            bson::BSONElement const& element) {
		cx->getSubContext(path)->clearDescendants();
		cx->clear(path);
		return Void();
	}
};
REGISTER_UPDATE_OPERATOR(ExtUpdateOperatorUnset, "$unset");

ACTOR static Future<Void> doRenameActor(Reference<IReadWriteContext> firstCx,
                                        Reference<IReadWriteContext> secondCx,
                                        Reference<IReadWriteContext> docCx,
                                        std::string renamedRoot,
                                        std::string renamedField) {
	Optional<DataValue> optionalValue = wait(firstCx->get(std::string("")));
	if (optionalValue.present()) {
		secondCx->clearDescendants();
		secondCx->set(std::string(""), optionalValue.get().encode_value());
		// fprintf(stderr, "root: %s\n", optionalValue.get().printable().c_str());
		DVTypeCode type = optionalValue.get().getSortType();
		if (type == DVTypeCode::OBJECT || type == DVTypeCode::ARRAY) {
			state GenFutureStream<KeyValue> descendents = firstCx->getDescendants();
			loop {
				try {
					KeyValue kv = waitNext(descendents);
					// fprintf(stderr, "%s : %s\n", printable(kv.key).c_str(), printable(kv.value).c_str());
					secondCx->set(kv.key, kv.value);
				} catch (Error& e) {
					if (e.code() == error_code_end_of_stream)
						break;
					else
						throw;
				}
			}
		}
		firstCx->clearDescendants();
		firstCx->clear(std::string(""));

		// Create the chain of objects above this one if they don't exist. In most update operators, we do this during
		// update document validation in this case we can't, because we don't want a bunch of ghost objects if the query
		// is well-formatted but the field to be renamed doesn't exist.
		if (renamedRoot != "") {
			Void _ = wait(ensureValidObject(docCx, renamedRoot, renamedField, true));
		}
	}

	return Void();
}

struct ExtUpdateOperatorRename {
	static const char* name;
	static Future<Void> execute(Reference<IReadWriteContext> cx,
	                            StringRef const& path,
	                            bson::BSONElement const& element) {
		Reference<IReadWriteContext> firstCx = cx->getSubContext(path.toString());
		Reference<IReadWriteContext> secondCx = cx->getSubContext(encodeMaybeDotted(element.String()));

		return doRenameActor(firstCx, secondCx, cx, upOneLevel(element.String()), getLastPart(element.String()));
	}
};

REGISTER_UPDATE_OPERATOR(ExtUpdateOperatorRename, "$rename");

template <class Op>
DataValue doubleDispatchArithmetic(DataValue valueInDatabase, DataValue valueToAdd, Op op) {
	switch (valueInDatabase.getBSONType()) {
	case bson::BSONType::NumberDouble: {
		switch (valueToAdd.getBSONType()) {
		case bson::BSONType::NumberDouble:
			return DataValue((double)op(valueInDatabase.getDouble(), valueToAdd.getDouble()));
		case bson::BSONType::NumberLong:
			return DataValue((double)op(valueInDatabase.getDouble(), valueToAdd.getLong()));
		case bson::BSONType::NumberInt:
			return DataValue((double)op(valueInDatabase.getDouble(), valueToAdd.getInt()));
		default:
			break;
		}
		break;
	}
	case bson::BSONType::NumberLong: {
		switch (valueToAdd.getBSONType()) {
		case bson::BSONType::NumberDouble:
			return DataValue((double)op(valueInDatabase.getLong(), valueToAdd.getDouble()));
		case bson::BSONType::NumberLong:
			return DataValue((long long)op(valueInDatabase.getLong(), valueToAdd.getLong()));
		case bson::BSONType::NumberInt:
			return DataValue((long long)op(valueInDatabase.getLong(), valueToAdd.getInt()));
		default:
			break;
		}
		break;
	}
	case bson::BSONType::NumberInt: {
		switch (valueToAdd.getBSONType()) {

		case bson::BSONType::NumberDouble:
			return DataValue((double)op(valueInDatabase.getInt(), valueToAdd.getDouble()));
		case bson::BSONType::NumberLong:
			return DataValue((long long)op(valueInDatabase.getInt(), valueToAdd.getLong()));
		case bson::BSONType::NumberInt:
			return DataValue((int)op(valueInDatabase.getInt(), valueToAdd.getInt()));
		default:
			break;
		}
		break;
	}
	default:
		break;
	}
	TraceEvent(SevError, "BD_doubleDispatchArithmeticError");
	throw internal_error();
}

ACTOR static Future<Void> getValueAndAdd(Reference<IReadWriteContext> cx,
                                         Standalone<StringRef> path,
                                         DataValue valueToAdd) {
	Optional<DataValue> valueInDatabase = wait(cx->get(path));
	if (valueInDatabase.present()) {
		DataValue actualValue = valueInDatabase.get();
		if (actualValue.getSortType() != DVTypeCode::NUMBER)
			throw inc_applied_to_non_number();
		cx->set(path,
		        doubleDispatchArithmetic(actualValue, valueToAdd, [](LongDouble a, LongDouble b) { return a + b; })
		            .encode_value());
		return Void();
	} else {
		cx->set(path, valueToAdd.encode_value());
		return Void();
	}
}

struct ExtUpdateOperatorInc {
	static const char* name;
	static Future<Void> execute(Reference<IReadWriteContext> cx,
	                            StringRef const& path,
	                            bson::BSONElement const& element) {
		DataValue valueToAdd(element);
		if (valueToAdd.getSortType() != DVTypeCode::NUMBER)
			throw inc_or_mul_with_non_number();

		return getValueAndAdd(cx, path, valueToAdd);
	}
};
REGISTER_UPDATE_OPERATOR(ExtUpdateOperatorInc, "$inc");

ACTOR static Future<Void> getValueAndMultiply(Reference<IReadWriteContext> cx,
                                              Standalone<StringRef> path,
                                              DataValue valueToMultiply) {
	Optional<DataValue> valueInDatabase = wait(cx->get(path));
	if (valueInDatabase.present()) {
		DataValue actualValue = valueInDatabase.get();
		if (actualValue.getSortType() != DVTypeCode::NUMBER)
			throw inc_applied_to_non_number();
		cx->set(path,
		        doubleDispatchArithmetic(actualValue, valueToMultiply, [](LongDouble a, LongDouble b) { return a * b; })
		            .encode_value());
		return Void();
	} else {
		cx->set(path, DataValue(0).encode_value());
		return Void();
	}
}

struct ExtUpdateOperatorMul {
	static const char* name;
	static Future<Void> execute(Reference<IReadWriteContext> cx,
	                            StringRef const& path,
	                            bson::BSONElement const& element) {
		DataValue valuetoMultiply(element);
		if (valuetoMultiply.getSortType() != DVTypeCode::NUMBER)
			throw inc_or_mul_with_non_number();

		return getValueAndMultiply(cx, path, valuetoMultiply);
	}
};
REGISTER_UPDATE_OPERATOR(ExtUpdateOperatorMul, "$mul");

ACTOR static Future<Void> getValueAndBitwise(Reference<IReadWriteContext> cx,
                                             Standalone<StringRef> path,
                                             bson::BSONObj obj) {

	Optional<DataValue> valueInDatabase = wait(cx->get(path));

	int32_t numberValue;
	bool storeLong;

	if (!valueInDatabase.present()) {
		numberValue = 0;
		storeLong = false;
	} else {
		DataValue actualValue = valueInDatabase.get();

		if (actualValue.getBSONType() == bson::BSONType::NumberInt) {
			storeLong = false;
			numberValue = actualValue.getInt();
		} else if (actualValue.getBSONType() == bson::BSONType::NumberLong) {
			storeLong = true;
			numberValue = actualValue.getLong();
		} else
			throw invalid_bitwise_applicand();
	}

	auto elem = obj.firstElement();

	if (!elem.isNumber())
		throw invalid_bitwise_parameter();

	int32_t n = elem.numberInt();

	auto opName = std::string(elem.fieldName());

	int32_t outValue;

	if (opName == "or") {
		outValue = numberValue | n;
	} else if (opName == "xor") {
		outValue = numberValue ^ n;
	} else if (opName == "and") {
		outValue = numberValue & n;
	} else {
		throw invalid_bitwise_update();
	}

	if (storeLong) {
		cx->set(path, DataValue((long long)outValue).encode_value());
	} else {
		cx->set(path, DataValue(outValue).encode_value());
	}

	return Void();
}

struct ExtUpdateOperatorBit {
	static const char* name;
	static Future<Void> execute(Reference<IReadWriteContext> cx,
	                            StringRef const& path,
	                            bson::BSONElement const& element) {
		auto obj = element.Obj();
		if (obj.nFields() != 1)
			throw invalid_bitwise_update();

		return getValueAndBitwise(cx, path, obj);
	}
};

REGISTER_UPDATE_OPERATOR(ExtUpdateOperatorBit, "$bit");

struct ExtUpdateOperatorSetOnIns {
	static const char* name;
	static Future<Void> execute(Reference<IReadWriteContext> cx,
	                            StringRef const& path,
	                            bson::BSONElement const& element) {
		// this code path is only taken when we're updating, never when we're "upserting", so do nothing as the operator
		// says
		return Void();
	}
};
REGISTER_UPDATE_OPERATOR(ExtUpdateOperatorSetOnIns, "$setOnInsert");

ACTOR static Future<Void> doPopActor(Reference<IReadWriteContext> cx,
                                     Standalone<StringRef> path,
                                     bson::BSONElement element) {
	state int length = wait(isArray(cx, path.toString()));
	if (length == -1)
		throw pop_non_array();
	else if (length == -2)
		length = 0;

	state Reference<IReadWriteContext> scx = cx->getSubContext(path);
	state std::string kp;

	if (element.number() == 1) {
		if (length > 0) {
			kp = DataValue(length - 1).encode_key_part();
			scx->getSubContext(kp)->clearDescendants();
			scx->clear(kp);
			scx->set(LiteralStringRef(""), DataValue::arrayOfLength(length - 1).encode_value());
		}
	} else if (element.number() == -1) {
		if (length > 0) {
			PromiseStream<DataValue> p;
			state FutureStream<DataValue> f = p.getFuture();

			state Future<Void> held = getArrayStream(cx, path, p);
			state int oldNumber = 0;

			loop {
				try {
					DataValue next = waitNext(f);
					kp = DataValue(oldNumber).encode_key_part();
					scx->getSubContext(kp)->clearDescendants();
					scx->clear(kp);
					if (oldNumber > 0) {
						if (next.getSortType() == DVTypeCode::NULL_ELEMENT) {
							// no-op, so our sparse arrays stay sparse
						} else if (next.isSimpleType()) {
							scx->set(DataValue(oldNumber - 1).encode_key_part(), next.encode_value());
						} else if (next.getSortType() == DVTypeCode::PACKED_OBJECT) {
							insertElementRecursive(oldNumber - 1, next.getPackedObject(), scx);
						} else if (next.getSortType() == DVTypeCode::PACKED_ARRAY) {
							insertElementRecursive(oldNumber - 1, next.getPackedArray(), scx);
						} else {
							TraceEvent(SevError, "BD_doPopActor_error").detail("nextTypecode", (int)next.getSortType());
							throw internal_error();
						}
					}
					oldNumber++;
				} catch (Error& e) {
					if (e.code() == error_code_end_of_stream)
						break;
					else
						throw;
				}
			}
			scx->set(LiteralStringRef(""), DataValue::arrayOfLength(length - 1).encode_value());
		}
	} else {
		throw generic_invalid_parameter();
	}

	return Void();
}

struct ExtUpdateOperatorPop {
	static const char* name;
	static Future<Void> execute(Reference<IReadWriteContext> cx,
	                            StringRef const& path,
	                            bson::BSONElement const& element) {
		return doPopActor(cx, path, element);
	}
};
REGISTER_UPDATE_OPERATOR(ExtUpdateOperatorPop, "$pop");

DataValue fullValueFromElement(bson::BSONElement element) {
	if (element.type() == bson::Object)
		return DataValue(element.Obj());
	else if (element.type() == bson::Array)
		return DataValue(bson::BSONArray(element.Obj()));
	else
		return DataValue(element);
}

ACTOR static Future<Void> doPullActor(Reference<IReadWriteContext> cx,
                                      Standalone<StringRef> path,
                                      std::set<DataValue> uniques) {
	state int length = wait(isArray(cx, path.toString()));
	if (length == -1)
		throw push_non_array();
	else if (length == -2)
		return Void();

	if (length > 0) {
		state Reference<IReadWriteContext> scx = cx->getSubContext(path);
		state int pulled = 0;
		state int oldNumber = 0;
		PromiseStream<DataValue> p;
		state FutureStream<DataValue> f = p.getFuture();

		state Future<Void> held = getArrayStream(cx, path, p);

		loop {
			try {
				DataValue next = waitNext(f);
				std::string kp = DataValue(oldNumber).encode_key_part();
				bool dontwrite = false;
				if (uniques.find(next) != uniques.end()) {
					pulled++;
					dontwrite = true;
				}
				if (pulled > 0) {
					scx->getSubContext(kp)->clearDescendants();
					scx->clear(kp);
					if (dontwrite || next.getSortType() == DVTypeCode::NULL_ELEMENT) {
						// no-op, so our sparse arrays stay sparse
					} else if (next.isSimpleType()) {
						scx->set(DataValue(oldNumber - pulled).encode_key_part(), next.encode_value());
					} else if (next.getSortType() == DVTypeCode::PACKED_OBJECT) {
						insertElementRecursive(oldNumber - pulled, next.getPackedObject(), scx);
					} else if (next.getSortType() == DVTypeCode::PACKED_ARRAY) {
						insertElementRecursive(oldNumber - pulled, next.getPackedArray(), scx);
					} else {
						TraceEvent(SevError, "BD_doPullActor_error").detail("nextTypecode", (int)next.getSortType());
						throw internal_error();
					}
				}
				oldNumber++;
			} catch (Error& e) {
				if (e.code() == error_code_end_of_stream)
					break;
				else
					throw;
			}
		}

		if (pulled > 0) {
			scx->set(LiteralStringRef(""), DataValue::arrayOfLength(length - pulled).encode_value());
		}
	}

	return Void();
}

struct ExtUpdateOperatorPull {
	static const char* name;
	static Future<Void> execute(Reference<IReadWriteContext> cx,
	                            StringRef const& path,
	                            bson::BSONElement const& element) {
		std::set<DataValue> only;
		DataValue dv = fullValueFromElement(element);
		only.insert(dv);
		return doPullActor(cx, path, only);
	}
};
REGISTER_UPDATE_OPERATOR(ExtUpdateOperatorPull, "$pull");

struct ExtUpdateOperatorPullAll {
	static const char* name;
	static Future<Void> execute(Reference<IReadWriteContext> cx,
	                            StringRef const& path,
	                            bson::BSONElement const& element) {
		std::vector<DataValue> all;
		std::vector<bson::BSONElement> elems = element.Array();
		for (auto e : elems) {
			all.push_back(fullValueFromElement(e));
		}
		std::set<DataValue> uniques(all.begin(), all.end());
		return doPullActor(cx, path, uniques);
	}
};
REGISTER_UPDATE_OPERATOR(ExtUpdateOperatorPullAll, "$pullAll");

ACTOR static Future<Void> doPushActor(Reference<IReadWriteContext> cx,
                                      Standalone<StringRef> path,
                                      bson::BSONElement element) {
	state int length = wait(isArray(cx, path.toString()));
	if (length == -1)
		throw push_non_array();
	else if (length == -2)
		length = 0;
	state Reference<IReadWriteContext> scx = cx->getSubContext(path);
	if (element.isABSONObj() && element.Obj().hasField("$each")) {
		state std::vector<bson::BSONElement> arr = element.Obj().getField("$each").Array();
		if (element.Obj().hasField("$slice")) {
			state int slice = element.Obj().getIntField("$slice");
			if (slice == 0) {
				scx->clearDescendants();
				cx->set(path, DataValue::arrayOfLength(0).encode_value());
				return Void();
			}
			if (element.Obj().hasField("$sort")) {
				PromiseStream<DataValue> p;
				state FutureStream<DataValue> f = p.getFuture();
				state std::vector<bson::BSONObj> objs;
				for (auto e : arr) {
					if (!e.isABSONObj())
						throw generic_invalid_parameter();
					objs.push_back(e.Obj().getOwned());
				}

				state Future<Void> held = getArrayStream(cx, path, p);
				loop {
					try {
						DataValue next = waitNext(f);
						if (next.getSortType() != DVTypeCode::PACKED_OBJECT)
							throw Error(error_code_generic_invalid_parameter);
						objs.push_back(next.getPackedObject().getOwned());
					} catch (Error& e) {
						if (e.code() == error_code_end_of_stream) {
							break;
						} else {
							throw;
						}
					}
				}

				bson::BSONObj orderObj = element.Obj().getObjectField("$sort");
				bson::Ordering o = bson::Ordering::make(orderObj);
				std::sort(
				    objs.begin(), objs.end(), [o, orderObj](const bson::BSONObj& first, const bson::BSONObj& second) {
					    return first.extractFields(orderObj, true).woCompare(second.extractFields(orderObj, true), o) <=
					           0;
				    });

				scx->clearDescendants();
				if (slice > 0) {
					for (int n = 0; n < std::min((int)objs.size(), slice); ++n) {
						insertElementRecursive(n, objs[n], scx);
					}
					cx->set(path, DataValue::arrayOfLength(std::min((int)objs.size(), slice)).encode_value());
				} else {
					int negslice = slice * -1;
					int m = 0;
					for (int n = std::max((int)objs.size() - negslice, 0); n < objs.size(); ++n) {
						insertElementRecursive(m, objs[n], scx);
						m++;
					}
					cx->set(path, DataValue::arrayOfLength(std::min((int)objs.size(), negslice)).encode_value());
				}
			} else {
				if (slice == length) {
					return Void();
				}
				if (slice > 0) {
					if (slice < length) {
						for (int n = slice; n < length; ++n) {
							std::string kp = DataValue(n).encode_key_part();
							scx->getSubContext(kp)->clearDescendants();
							scx->clear(kp);
						}
						cx->set(path, DataValue::arrayOfLength(slice).encode_value());
					} else {
						int n;
						for (n = 0; n < arr.size() && n + length < slice; ++n) {
							insertElementRecursive(length + n, arr[n], scx);
						}
						cx->set(path, DataValue::arrayOfLength(length + n).encode_value());
					}
				} else {
					state int negslice = slice * -1;
					if (negslice >= length + arr.size()) {
						int n = 0;
						for (auto b : arr) {
							insertElementRecursive(length + n, b, scx);
							n++;
						}
						cx->set(path, DataValue::arrayOfLength(length + n).encode_value());
					} else if (negslice <= arr.size()) {
						scx->clearDescendants();
						cx->set(path, DataValue::arrayOfLength(negslice).encode_value());
						int m = 0;
						for (int n = arr.size() - negslice; n < arr.size(); ++n) {
							insertElementRecursive(m, arr[n], scx);
							m++;
						}
					} else {
						state int skip = length + arr.size() - negslice;
						state int oldNumber = 0;
						PromiseStream<DataValue> p;
						state FutureStream<DataValue> f2 = p.getFuture();

						state Future<Void> held2 = getArrayStream(cx, path, p);

						loop {
							try {
								DataValue next = waitNext(f2);
								std::string kp = DataValue(oldNumber).encode_key_part();
								scx->getSubContext(kp)->clearDescendants();
								scx->clear(kp);
								if (oldNumber >= skip) {
									if (next.getSortType() == DVTypeCode::NULL_ELEMENT) {
										// no-op, so our sparse arrays stay sparse
									} else if (next.isSimpleType()) {
										scx->set(DataValue(oldNumber - skip).encode_key_part(), next.encode_value());
									} else if (next.getSortType() == DVTypeCode::PACKED_OBJECT) {
										insertElementRecursive(oldNumber - skip, next.getPackedObject(), scx);
									} else if (next.getSortType() == DVTypeCode::PACKED_ARRAY) {
										insertElementRecursive(oldNumber - skip, next.getPackedArray(), scx);
									} else {
										TraceEvent(SevError, "BD_doPushActor_error")
										    .detail("nextTypecode", (int)next.getSortType());
										throw internal_error();
									}
								}
								oldNumber++;
							} catch (Error& e) {
								if (e.code() == error_code_end_of_stream) {
									break;
								} else {
									throw;
								}
							}
						}
						for (auto b : arr) {
							insertElementRecursive(oldNumber - skip, b, scx);
							oldNumber++;
						}
						cx->set(path, DataValue::arrayOfLength(negslice).encode_value());
					}
				}
			}
		} else {
			int n = 0;
			for (auto b : arr) {
				insertElementRecursive(length + n, b, scx);
				n++;
			}
			cx->set(path, DataValue::arrayOfLength(length + n).encode_value());
		}
	} else {
		insertElementRecursive(length, element, scx);
		cx->set(path, DataValue::arrayOfLength(length + 1).encode_value());
	}

	return Void();
}

struct ExtUpdateOperatorPush {
	static const char* name;
	static Future<Void> execute(Reference<IReadWriteContext> cx,
	                            StringRef const& path,
	                            bson::BSONElement const& element) {
		return doPushActor(cx, path, element);
	}
};
REGISTER_UPDATE_OPERATOR(ExtUpdateOperatorPush, "$push");

ACTOR static Future<Void> doAddToSetActor(Reference<IReadWriteContext> cx,
                                          Standalone<StringRef> path,
                                          bson::BSONElement element) {
	state int length = wait(isArray(cx, path.toString()));
	if (length == -1)
		throw add_to_set_non_array();
	else if (length == -2) {
		length = 0;
	}
	state std::set<DataValue> uniques;
	PromiseStream<DataValue> p;
	state FutureStream<DataValue> f = p.getFuture();

	state Future<Void> held = getArrayStream(cx, path, p);

	loop {
		try {
			DataValue next = waitNext(f);
			uniques.insert(next);
		} catch (Error& e) {
			if (e.code() == error_code_end_of_stream) {
				break;
			} else {
				throw;
			}
		}
	}

	if (element.isABSONObj() && element.Obj().hasField("$each")) {
		std::vector<bson::BSONElement> arr = element.Obj().getField("$each").Array();
		int n = 0;
		for (auto b : arr) {
			DataValue dv = fullValueFromElement(b);
			if (uniques.find(dv) == uniques.end()) {
				insertElementRecursive(length + n, b, cx->getSubContext(path));
				n++;
				uniques.insert(dv);
			}
		}
		cx->set(path, DataValue::arrayOfLength(length + n).encode_value());
	} else {
		DataValue dv = fullValueFromElement(element);
		if (uniques.find(dv) == uniques.end()) {
			insertElementRecursive(length, element, cx->getSubContext(path));
			cx->set(path, DataValue::arrayOfLength(length + 1).encode_value());
		}
	}

	return Void();
}

struct ExtUpdateOperatorAddToSet {
	static const char* name;
	static Future<Void> execute(Reference<IReadWriteContext> cx,
	                            StringRef const& path,
	                            bson::BSONElement const& element) {
		return doAddToSetActor(cx, path, element);
	}
};
REGISTER_UPDATE_OPERATOR(ExtUpdateOperatorAddToSet, "$addToSet");

struct ExtUpdateOperatorCurrentDate {
	static const char* name;
	static Future<Void> execute(Reference<IReadWriteContext> cx,
	                            StringRef const& path,
	                            bson::BSONElement const& element) {
		if (element.isBoolean() && element.Bool() == true) {
			DataValue dv = DataValue(bson::Date_t(timer() * 1000));
			cx->getSubContext(path)->clearDescendants();
			cx->set(path, dv.encode_value());
		} else if (element.isABSONObj() && element.Obj().hasField("$type")) {
			auto typeField = element.Obj().getField("$type");
			if (typeField.isString() && typeField.String() == "date") {
				DataValue dv = DataValue(bson::Date_t(timer() * 1000));
				cx->getSubContext(path)->clearDescendants();
				cx->set(path, dv.encode_value());
			} else if (typeField.isString() && typeField.String() == "timestamp") {
				throw no_mongo_timestamps();
			}
		}
		return Void();
	}
};
REGISTER_UPDATE_OPERATOR(ExtUpdateOperatorCurrentDate, "$currentDate");

ACTOR static Future<Void> getAndCompare(Reference<IReadWriteContext> cx,
                                        Standalone<StringRef> path,
                                        bson::BSONElement element,
                                        bool isMax) {
	Optional<DataValue> odv = wait(getMaybeRecursiveIfPresent(cx->getSubContext(path)));
	if (!odv.present()) {
		insertElementRecursive(element, cx);
		return Void();
	} else {
		bson::Ordering o = bson::Ordering::make(BSON("fake_fieldname" << 1));
		bson::BSONObj obj1 = element.wrap("fake_fieldname");
		bson::BSONObj obj2 = odv.get().wrap("fake_fieldname");
		if ((obj1.woCompare(obj2, o) >= 0) == isMax) {
			cx->getSubContext(path)->clearDescendants();
			insertElementRecursive(element, cx);
			return Void();
		} else {
			return Void();
		}
	}
}

struct ExtUpdateOperatorMax {
	static const char* name;
	static Future<Void> execute(Reference<IReadWriteContext> cx,
	                            StringRef const& path,
	                            bson::BSONElement const& element) {
		return getAndCompare(cx, path, element, true);
	}
};
REGISTER_UPDATE_OPERATOR(ExtUpdateOperatorMax, "$max");

struct ExtUpdateOperatorMin {
	static const char* name;
	static Future<Void> execute(Reference<IReadWriteContext> cx,
	                            StringRef const& path,
	                            bson::BSONElement const& element) {
		return getAndCompare(cx, path, element, false);
	}
};
REGISTER_UPDATE_OPERATOR(ExtUpdateOperatorMin, "$min");
