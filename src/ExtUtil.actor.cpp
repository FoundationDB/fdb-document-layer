/*
 * ExtUtil.actor.cpp
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
 */

#include <string>

#include "DocumentError.h"

#include "ExtUtil.actor.h"

#include "QLContext.h"
#include "QLPredicate.h"
#include "QLProjection.actor.h"
#include "QLTypes.h"
#include "flow/actorcompiler.h" // This must be the last #include.

using namespace FDB;

std::string upOneLevel(std::string maybeDottedFieldName) {
	size_t pos = maybeDottedFieldName.rfind('.');
	if (pos == std::string::npos) {
		return "";
	} else {
		return maybeDottedFieldName.substr(0, pos);
	}
}

std::string getLastPart(std::string maybeDottedFieldName) {
	size_t pos = maybeDottedFieldName.rfind('.');
	if (pos == std::string::npos) {
		return "";
	} else {
		return maybeDottedFieldName.substr(pos + 1, std::string::npos);
	}
}

Key encodeMaybeDotted(std::string fieldname) {
	if (fieldname.empty())
		return StringRef(fieldname);

	std::string path;
	std::string fn = fieldname;
	fn.append(".");

	size_t pos;

	while ((pos = fn.find('.')) != std::string::npos) {
		std::string sub = fn.substr(0, pos);
		if (std::all_of(sub.begin(), sub.end(), ::isdigit)) {
			const char* c_sub = sub.c_str();
			path += DataValue(atoi(c_sub)).encode_key_part().toString();
		} else {
			path += DataValue(sub).encode_key_part().toString();
		}

		fn = fn.substr(pos + 1);
	}
	return StringRef(path);
}

int insertElementRecursive(std::string fn, bson::BSONObj const& obj, Reference<IReadWriteContext> cx) {
	if (fn[0] == '$')
		throw fieldname_with_dollar();
	Key kp = encodeMaybeDotted(fn);

	int nrFDBKeys = 1;
	cx->set(kp, DataValue::subObject().encode_value());

	auto scx = cx->getSubContext(kp);
	for (auto i = obj.begin(); i.more();) {
		auto e = i.next();
		nrFDBKeys += insertElementRecursive(e, scx);
	}

	return nrFDBKeys;
}

int insertElementRecursive(std::string fn, bson::BSONArray const& arr, Reference<IReadWriteContext> cx) {
	if (fn[0] == '$')
		throw fieldname_with_dollar();

	Key kp = encodeMaybeDotted(fn);
	int nrFDBKeys = 1;

	cx->set(kp, DataValue::arrayOfLength(arr.nFields()).encode_value());

	auto scx = cx->getSubContext(kp);
	for (auto i = arr.begin(); i.more();) {
		bson::BSONElement e = i.next();
		nrFDBKeys += insertElementRecursive(e, scx);
	}

	return nrFDBKeys;
}

int insertElementRecursive(std::string fn, bson::BSONElement const& elem, Reference<IReadWriteContext> cx) {
	if (fn[0] == '$')
		throw fieldname_with_dollar();

	Key kp = encodeMaybeDotted(fn);
	if (!elem.isABSONObj()) {
		cx->set(kp, DataValue(elem).encode_value());
		return 1;
	}

	if (elem.type() == bson::BSONType::Array)
		return insertElementRecursive(fn, bson::BSONArray(elem.Obj()), cx);

	return insertElementRecursive(fn, elem.Obj(), cx);
}

int insertElementRecursive(bson::BSONElement const& elem, Reference<IReadWriteContext> cx) {
	std::string fn = elem.fieldName();
	if (std::all_of(fn.begin(), fn.end(), ::isdigit)) {
		const char* c_fn = fn.c_str();
		return insertElementRecursive(atoi(c_fn), elem, cx);
	}

	return insertElementRecursive(fn, elem, cx);
}

int insertElementRecursive(int fn, bson::BSONElement const& elem, Reference<IReadWriteContext> cx) {
	Standalone<StringRef> kp = DataValue(fn).encode_key_part();
	if (!elem.isABSONObj()) {
		cx->set(kp, DataValue(elem).encode_value());
		return 1;
	}

	if (elem.type() == bson::BSONType::Array)
		return insertElementRecursive(fn, bson::BSONArray(elem.Obj()), cx);

	return insertElementRecursive(fn, elem.Obj(), cx);
}

int insertElementRecursive(int fn, bson::BSONObj const& obj, Reference<IReadWriteContext> cx) {
	Standalone<StringRef> kp = DataValue(fn).encode_key_part();
	int nrFDBKeys = 1;

	cx->set(kp, DataValue::subObject().encode_value());

	auto scx = cx->getSubContext(kp);
	for (auto i = obj.begin(); i.more();) {
		auto e = i.next();
		nrFDBKeys += insertElementRecursive(e, scx);
	}
	return nrFDBKeys;
}

int insertElementRecursive(int fn, bson::BSONArray const& arr, Reference<IReadWriteContext> cx) {
	Standalone<StringRef> kp = DataValue(fn).encode_key_part();
	int nrFDBKeys = 1;

	cx->set(kp, DataValue::arrayOfLength(arr.nFields()).encode_value());

	auto scx = cx->getSubContext(kp);
	for (auto i = arr.begin(); i.more();) {
		bson::BSONElement e = i.next();
		nrFDBKeys += insertElementRecursive(e, scx);
	}
	return nrFDBKeys;
}

ACTOR Future<Void> ensureValidObject(Reference<IReadWriteContext> cx,
                                     std::string objectRoot,
                                     std::string objectSubfield,
                                     bool createRoot) {
	state Key encodedObjectRoot = encodeMaybeDotted(objectRoot);
	state Optional<DataValue> optionalValue = wait(cx->get(encodedObjectRoot));
	state Future<Optional<DataValue>> futureOptionalSubfield =
	    cx->getSubContext(encodedObjectRoot)->get(encodeMaybeDotted(objectSubfield));

	if (optionalValue.present()) {
		DVTypeCode type = optionalValue.get().getSortType();
		if (type == DVTypeCode::OBJECT) {

		} else if (type == DVTypeCode::ARRAY) {
			// See if it could be a valid array field
			char* r;
			long int n = strtol(objectSubfield.data(), &r, 10);
			if (*r)
				throw append_to_array_with_string_field();
			// Maintain array length field invariant
			if (n + 1 > optionalValue.get().getArraysize())
				cx->set(encodedObjectRoot, DataValue::arrayOfLength(n + 1).encode_value());
		} else {
			throw subfield_of_simple_type();
		}
	} else if (createRoot) {
		cx->set(encodedObjectRoot, DataValue::subObject().encode_value());
		if (!upOneLevel(objectRoot).empty()) {
			wait(ensureValidObject(cx, upOneLevel(objectRoot), getLastPart(objectRoot), createRoot));
		}
	}

	return Void();
}

ACTOR Future<int> isArray(Reference<IReadWriteContext> docCx, std::string arrayRoot) {
	Optional<DataValue> optionalValue = wait(docCx->get(arrayRoot));

	if (optionalValue.present()) {
		if (optionalValue.get().getSortType() == DVTypeCode::ARRAY) {
			return optionalValue.get().getArraysize();
		} else {
			return -1;
		}
	}
	return -2;
}

ACTOR Future<DataValue> getMaybeRecursive(Reference<IReadContext> cx, StringRef path) {
	Optional<DataValue> dv = wait(getMaybeRecursiveIfPresent(cx->getSubContext(path)));
	if (!dv.present())
		return DataValue::nullValue();
	else
		return dv.get();
}

Projection::Iterator const Projection::Iterator::end = Projection::Iterator();

// Moves the iterator to the next projection lexicographically in the projection tree
Projection::Iterator& Projection::Iterator::operator++() {
	if (stack.empty()) {
		return *this;
	}

	while (!stack.empty() &&
	       (stack.back().itr == stack.back().projection->fields.end() || stack.back().projection->shouldBeRead)) {
		if (stack.back().itr == stack.back().projection->fields.end()) {
			stack.pop_back();
			if (!stack.empty()) {
				path.pop_back();
			}
		} else {
			++stack.back().itr;
		}
	}

	if (!stack.empty()) {
		path.push_back(stack.back().itr->first);
		StackEntry newEntry = StackEntry(stack.back().itr->second);
		++stack.back().itr;
		stack.push_back(newEntry);
	}

	return *this;
}

Projection::Iterator Projection::Iterator::operator++(int) {
	Projection::Iterator tmp = *this;
	operator++();
	return tmp;
}

bool Projection::Iterator::operator==(Projection::Iterator const& other) const {
	return this->path == other.path && this->stack == other.stack;
}

Projection::Iterator Projection::begin() {
	Reference<Projection> root = Reference<Projection>::addRef(this);

	Projection::Iterator itr;
	itr.stack.emplace_back(root);

	return ++itr;
}

Projection::Iterator const Projection::end() {
	return Projection::Iterator::end;
}

std::string Projection::debugString(bool first) {
	std::stringstream s;
	if (first && !included) {
		s << ".";
	}

	s << "{";
	bool addComma = false;
	for (const auto& itr : fields) {
		if (addComma)
			s << ", ";
		else
			addComma = true;

		if (!itr.second->included) {
			s << ".";
		}

		s << itr.first;

		if (!itr.second->fields.empty()) {
			s << ": " << itr.second->debugString(false);
		}
	}

	s << "}";
	return s.str();
}

// Called for each field read from a document in lexicographic order. Returns INCLUDE_ALL if the entire field should be
// included, INCLUDE_PARTIAL if the field should be partially included (e.g. for an array, the array will be included
// but not all of its elements will), and EXCLUDE if the field should be excluded.
Projector::IncludeType Projector::includeNextField(int depth, std::string fieldName, bool isSimple, bool inArray) {
	// This is the case if no projection was supplied
	if (projectionStack.empty()) {
		return INCLUDE_ALL;
	}

	Reference<Projection> currentProjection;
	if (projectionStack.size() >= depth) {
		projectionStack.resize(depth);
		currentProjection = projectionStack.back().second;

		// Projection specifications can match through arrays (e.g. {'A': [{'B':'c'}]} is included by the projection
		// {'A.B':1}) Therefore, when descending into an array, we can just duplicate the previous projection
		if (inArray) {
			projectionStack.push_back(projectionStack.back());
		}

		// Otherwise, check if the current projection has the current field. If so, add its projection to the stack
		else {
			auto itr = currentProjection->fields.find(fieldName);
			if (itr != currentProjection->fields.end()) {
				projectionStack.emplace_back(fieldName, itr->second);
			}
		}
	}

	currentProjection = projectionStack.back().second;

	IncludeType result = EXCLUDE;
	if (currentProjection->included) {
		result = INCLUDE_ALL;
	}

	// If we are projecting an object or array and we haven't reached the end of the current field specification (e.g.
	// we are at 'A.B' and the projection specified 'A.B.C'), then we partially include the object or array.
	else if (!isSimple && projectionStack.size() == depth + 1 && !currentProjection->fields.empty()) {
		result = INCLUDE_PARTIAL;
	}

	return result;
}

ACTOR Future<Void> getArrayStream(Reference<IReadWriteContext> document,
                                  Standalone<StringRef> arrayField,
                                  PromiseStream<DataValue> p) {

	state GenFutureStream<KeyValue> kvs = document->getDescendants(arrayField, strinc(arrayField));
	state int n;

	try {
		KeyValue root = waitNext(kvs);
		DataValue rootDv = DataValue::decode_value(root.value);
		ASSERT(rootDv.getSortType() == DVTypeCode::ARRAY);
		n = rootDv.getArraysize();
	} catch (Error& e) {
		if (e.code() == error_code_end_of_stream) {
			p.sendError(end_of_stream());
			return Void();
		}
		throw;
	}

	state std::vector<BOBObj> bobs;
	state int currentLoc = 0;

	loop {
		try {
			KeyValue kv = waitNext(kvs);

			DataKey dk = DataKey::decode_bytes(kv.key);

			for (auto i = bobs.size(); i > dk.size() - 2; --i) {
				auto popped = std::move(bobs.back());
				bobs.pop_back();
				if (popped.isArrayLength >= 0) {
					if (!bobs.empty())
						bobs.back().append(popped.fieldname, DataValue(bson::BSONArray(popped.build())));
					else {
						ASSERT(i == dk.size() - 1);
						p.send(DataValue(bson::BSONArray(popped.build().getOwned())));
						currentLoc++;
					}
				} else {
					if (!bobs.empty())
						bobs.back().append(popped.fieldname, DataValue(popped.build()));
					else {
						ASSERT(i == dk.size() - 1);
						p.send(DataValue(popped.build().getOwned()));
						currentLoc++;
					}
				}
			}

			DataValue kdv = DataValue::decode_key_part(dk[dk.size() - 1]);
			if (bobs.empty()) {
				ASSERT(kdv.getSortType() == DVTypeCode::NUMBER);
				int j = kdv.getDouble();
				for (; currentLoc < j; ++currentLoc) {
					p.send(DataValue::nullValue());
				}
			}
			std::string fieldname =
			    kdv.getSortType() == DVTypeCode::STRING ? kdv.getString() : std::to_string((int)kdv.getDouble());
			DataValue dv = DataValue::decode_value(kv.value);
			switch (dv.getBSONType()) {
			case bson::BSONType::Object:
				bobs.emplace_back(-1, fieldname);
				break;
			case bson::BSONType::Array:
				bobs.emplace_back(dv.getArraysize(), fieldname);
				break;
			default: {
				if (!bobs.empty())
					bobs.back().append(fieldname, dv);
				else {
					p.send(dv);
					currentLoc++;
				}
			}
			}
		} catch (Error& e) {
			if (e.code() == error_code_end_of_stream) {
				break;
			}
			throw;
		}
	}

	if (!bobs.empty()) {
		for (auto i = bobs.size() - 1; i > 0; --i) {
			if (bobs[i].isArrayLength >= 0) {
				bobs[i - 1].append(bobs[i].fieldname, DataValue(bson::BSONArray(bobs[i].build())));
			} else {
				bobs[i - 1].append(bobs[i].fieldname, DataValue(bobs[i].build()));
			}
		}
		if (bobs[0].isArrayLength >= 0)
			p.send(DataValue(bson::BSONArray(bobs[0].build().getOwned())));
		else
			p.send(DataValue(bobs[0].build().getOwned()));
		currentLoc++;
	}

	for (; currentLoc < n; ++currentLoc) {
		p.send(DataValue::nullValue());
	}

	p.sendError(end_of_stream());

	return Void();
}

bool is_literal_match(bson::BSONObj const& obj) {
	if (obj.nFields() == 0) {
		return true;
	}

	for (auto i = obj.begin(); i.more();) {
		auto el = i.next();

		if (el.fieldName()[0] == '$') {
			return false;
		}
	}

	return true;
}

Reference<IPredicate> any_predicate(std::string unencoded_path, Reference<IPredicate> pred, bool inElemMatch) {
	if (unencoded_path.empty())
		return pred;
	return ref(
	    new AnyPredicate(ref(new ExtPathExpression(unencoded_path, true, !inElemMatch && pred->wantsNulls())), pred));
}

Reference<IPredicate> eq_predicate(const bson::BSONElement& el, const std::string& prefix) {
	DataValue dv;

	if (!el.isABSONObj()) {
		dv = DataValue(el);
	} else if (el.type() == bson::BSONType::Array) {
		dv = DataValue(bson::BSONArray(el.Obj()));
	} else {
		dv = DataValue(sortBsonObj(el.Obj()));
	}	

	return any_predicate(prefix, ref(new EqPredicate(dv)));
}

Reference<IPredicate> re_predicate(const bson::BSONElement& el, const std::string& prefix) {
	std::string regexPattern;
	std::string regexOptions;
	if (el.type() == bson::BSONType::RegEx) {
		regexPattern = el.regex();
		regexOptions = el.regexFlags();
	} else if (el.type() == bson::BSONType::String) {
		regexPattern = el.String();
		/*regexOptions = options; will come later*/
	} else {
		throw bad_regex_input();
	}

	return any_predicate(prefix, ref(new RegExPredicate(regexPattern, regexOptions)));
}

int hasOperatorFieldnames(bson::BSONObj obj,
                          int maxDepth,
                          int ensureEveryFieldnameBeyondThisDepthIsAnOperator,
                          int currentDepth) {
	int index = 0;
	for (auto i = obj.begin(); i.more(); index++) {
		auto next = i.next();
		if (next.fieldName()[0] == '$') {
			if (currentDepth > ensureEveryFieldnameBeyondThisDepthIsAnOperator) {
				if (index > 0)
					throw mixed_operator_literal_update();
				else
					while (i.more())
						if (i.next().fieldName()[0] != '$')
							throw mixed_operator_literal_update();
			}
			return ++currentDepth;
		}
		int depth;
		if ((maxDepth > currentDepth || maxDepth < 0) && next.type() == bson::BSONType::Object &&
		    (depth = hasOperatorFieldnames(next.Obj(), maxDepth, ensureEveryFieldnameBeyondThisDepthIsAnOperator,
		                                   ++currentDepth)))
			return depth;
	}
	return 0;
}

template <class T>
bool isin(std::list<T> coll, T item) {
	return std::find(coll.begin(), coll.end(), item) != coll.end();
}

static std::list<std::string> comparison_operators = {"$gt", "$gte", "$in", "$lt", "$lte", "$ne", "$nin", "$eq"},
                              logical_operators = {"$or", "$and", "$nor"}, element_operators = {"$exists", "$type"},
                              array_operators = {"$all", "$elemMatch", "$size"},
                              evaluation_operators = {"$regex", "$mod", "$where", "$text", "$options"};

bson::BSONObj deepTransformLogicalOperators(bson::BSONObj obj) {
	bson::BSONObjBuilder builder;

	for (auto entries = obj.begin(); entries.more();) {
		auto entry = entries.next();
		std::string fieldName(entry.fieldName());

		if (isin(logical_operators, fieldName)) {
			if (entry.type() != bson::BSONType::Array)
				throw invalid_query_operator();

			if ((fieldName == "$or" && entry.Array().size() > 1) || fieldName == "$nor") {
			} else
				for (auto item : entry.Array()) {
					/* fieldName was $and or $or (with one element) */
					if (item.type() == bson::BSONType::Object) {
						for (auto itemEntries = item.Obj().begin(); itemEntries.more();) {
							auto itemEntry = itemEntries.next();
							int operator_found = hasOperatorFieldnames(itemEntry.wrap());
							if (operator_found) {
								bson::BSONObj logicalChild = deepTransformLogicalOperators(item.Obj());
								builder.appendElements(logicalChild);
							} else
								builder.append(itemEntry);
						}
					}
				}
		} else if (!hasOperatorFieldnames(entry.wrap())) {
			builder.append(entry);
		} else
			builder.appendElements(transformOperatorQueryToUpdatableDocument(entry.wrap()));
	}
	return builder.obj();
}

bson::BSONObj getUpdatedObjectsDifference(bson::BSONObj original, bson::BSONObj updated, bool isSet) {
	bson::BSONObjBuilder builder;

	for (auto entries = updated.begin(); entries.more();) {
		auto next = entries.next();
		auto nextField = next.fieldName();

		if(!original.hasElement(nextField)) {
			if (isSet) {
				builder.append(next);
			} else {
				builder.appendNull(nextField);
			}
			continue;
		}

		auto sibbling = original.getField(nextField);

		if (next.type() == bson::Object) {
			auto child = getUpdatedObjectsDifference(sibbling.Obj(), next.Obj(), isSet);
			if (!child.isEmpty()) {
				builder.append(nextField, child);
			}
			continue;
		}

		if (!next.valuesEqual(sibbling)) {
			builder.append(next);
		}
	}

	return builder.obj();
}

bson::BSONObj convertCompoundStringToObj(bson::BSONObj obj) {
	bson::BSONObjBuilder builder1;

	for (auto entries = obj.begin(); entries.more();) {
		auto next = entries.next();

		if (strchr(next.fieldName(), '.')) {
			bson::BSONObjBuilder builder2;
			bson::BSONObj child;
			std::string fieldName(next.fieldName());
			std::string::size_type pos = fieldName.find('.');
			std::string childName(fieldName.substr(pos + 1));
			fieldName = fieldName.substr(0, pos);

			builder2.appendAs(next, bson::StringData(childName));

			if (childName.find('.'))
				child = convertCompoundStringToObj(builder2.obj());
			else
				child = builder2.obj();

			builder1.append(bson::StringData(fieldName), child);
		} else
			builder1.append(next);
	}
	return builder1.obj();
}

// bson::BSONObj deepConvertCompoundStringToObj(bson::BSONObj obj)
//{
//	bson::BSONObjBuilder builder1;
//	bson::BSONObjBuilder builder2;
//
//	for (auto entries = obj.begin(); entries.more();) {
//
//		auto next = entries.next();
//
//		if (next.type() == bson::BSONType::Object) {
//			builder2.appendElements(
//				deepConvertCompoundStringToObj(
//					next.Obj()
//				)
//			);
//			builder1.append(
//				bson::StringData(next.fieldName()),
//				builder2.obj()
//			);
//		}
//		else if (next.type() == bson::BSONType::Array) {
//			bson::BSONArrayBuilder abuilder;
//			for (auto item : next.Array()) {
//				if (item.type() == bson::BSONType::Object) {
//					abuilder.append(
//						deepConvertCompoundStringToObj(item.Obj())
//					);
//				} else if (item.type() == bson::BSONType::Array) {
//					// create a function for handling arrays and call it recursively
//					//
//				} else abuilder.append(item);
//				builder1.append(
//					bson::StringData(next.fieldName()),
//					abuilder.arr()
//				);
//			}
//		}
//		else builder1.append(next);
//
//	}
//	return convertCompoundStringToObj(builder1.obj());
//}

bson::BSONObj transformOperatorQueryToUpdatableDocument(bson::BSONObj selector, std::string parentKey, int depth) {
	bson::BSONObjBuilder builder;

	for (auto i = selector.begin(); i.more();) {
		auto next = i.next();
		std::string fieldName(next.fieldName());
		bool stop = false;

		int operator_found = hasOperatorFieldnames(next.wrap());

		if (operator_found) {
			if (isin(logical_operators, fieldName)) {
				if (!depth)
					builder.appendElements(deepTransformLogicalOperators(selector));
				else
					throw malformed_logical_query_operator();
			}

			if (isin(array_operators, fieldName)) {
				if (!depth || depth > 1)
					throw invalid_query_operator();
				else if (fieldName == "$all") {
					if (next.type() != bson::BSONType::Array || next.Array().size() > 1)
						throw invalid_query_operator();
					else if (!next.Array().empty())
						builder.appendAs(next.Array()[0], bson::StringData(parentKey));
				} else if (fieldName == "$elemMatch")
					stop = true;
			}

			if (isin(comparison_operators, fieldName)) {
				if (!depth || depth > 1)
					throw invalid_query_operator();
				else if (fieldName == "$in" && next.type() != bson::BSONType::Array)
					throw invalid_query_operator();
				else if (fieldName == "$eq")
					builder.appendAs(next, bson::StringData(parentKey));
			}

			if (fieldName == "$not" || isin(element_operators, fieldName) || isin(evaluation_operators, fieldName)) {
				if (!depth || depth > 1)
					throw invalid_query_operator();
				else
					stop = true;
			}
		} else
			builder.append(next);

		if (next.type() == bson::BSONType::Object && !stop) {
			bson::BSONObj child = transformOperatorQueryToUpdatableDocument(next.Obj().getOwned(), fieldName, ++depth);
			if (!child.isEmpty())
				builder.appendElements(child);
		}
	}
	return convertCompoundStringToObj(builder.obj());
}

bson::BSONObj sortBsonObj(bson::BSONObj obj) {
	std::vector<bson::BSONElement> fields;
	std::vector<bson::BSONObj> holder;

	for (auto i = obj.begin(); i.more();) {
		auto e = i.next();
		if (e.type() != bson::BSONType::Object) {
			fields.push_back(e);
		} else {
			bson::BSONObj newObj = BSON(e.fieldName() << sortBsonObj(e.Obj())).getOwned();
			holder.push_back(newObj);
			fields.push_back(newObj.firstElement());
		}
	}
	std::sort(fields.begin(), fields.end(), [](bson::BSONElement left, bson::BSONElement right) {
		return strcmp(left.fieldName(), right.fieldName()) < 0;
	});
	bson::BSONObjBuilder b(obj.objsize());
	for (auto e : fields) {
		b.append(e);
	}
	return b.obj();
}

Optional<IdInfo> extractEncodedIds(bson::BSONObj obj) {
	bson::BSONElement el;
	Optional<IdInfo> encodedIds;
	bool elok = obj.getObjectID(el);
	if (!elok) {
		encodedIds = Optional<IdInfo>();
	} else {
		Standalone<StringRef> encodedId;
		Standalone<StringRef> valueEncodedId;
		Optional<bson::BSONObj> idObj;
		if (el.type() == bson::BSONType::Array) {
			throw no_array_id();
		} else if (el.isABSONObj()) {
			encodedId = DataValue(sortBsonObj(el.Obj())).encode_key_part();
			valueEncodedId = DataValue::subObject().encode_value();
			idObj = el.Obj().getOwned();
		} else {
			encodedId = DataValue(el).encode_key_part();
			valueEncodedId = DataValue(el).encode_value();
			idObj = Optional<bson::BSONObj>();
		}
		encodedIds = IdInfo(encodedId, valueEncodedId, idObj);
	}
	return encodedIds;
}

Optional<IdInfo> extractEncodedIdsFromUpdate(bson::BSONObj update) {
	for (auto i = update.begin(); i.more();) {
		auto el = i.next();
		std::string operatorName = el.fieldName();
		if (operatorName == "$set" || operatorName == "$setOnInsert") {
			return extractEncodedIds(el.Obj());
		}
	}
	return Optional<IdInfo>();
}

Future<Reference<Plan>> getIndexesForCollectionPlan(Namespace const& ns,
                                                    Reference<DocTransaction> tr,
                                                    Reference<MetadataManager> mm) {
	std::string nsStr = ns.first + "." + ns.second;
	Future<Reference<UnboundCollectionContext>> unbound = mm->indexesCollection(tr, ns.first);
	return map(unbound, [nsStr](Reference<UnboundCollectionContext> unbound) {
		Reference<IPredicate> pred =
		    any_predicate(DocLayerConstants::NS_FIELD, ref(new EqPredicate(DataValue(nsStr))), false);
		return FilterPlan::construct_filter_plan_no_pushdown(unbound, ref(new TableScanPlan(unbound)), pred);
	});
}

Reference<Plan> getIndexesForCollectionPlan(Reference<UnboundCollectionContext> indexesCollection,
                                            Namespace const& ns) {
	std::string nsStr = ns.first + "." + ns.second;
	Reference<IPredicate> pred =
	    any_predicate(DocLayerConstants::NS_FIELD, ref(new EqPredicate(DataValue(nsStr))), false);
	return FilterPlan::construct_filter_plan_no_pushdown(indexesCollection, ref(new TableScanPlan(indexesCollection)),
	                                                     pred);
}

ACTOR Future<std::vector<bson::BSONObj>> getIndexesTransactionally(Reference<Plan> indexMetadataPlan,
                                                                   Reference<DocTransaction> tr) {
	state std::vector<bson::BSONObj> ret;

	state Reference<PlanCheckpoint> checkpoint(new PlanCheckpoint);
	state FutureStream<Reference<ScanReturnedContext>> indexStream =
	    indexMetadataPlan->execute(checkpoint.getPtr(), tr);
	state FlowLock* flowControlLock = checkpoint->getDocumentFinishedLock();

	loop {
		try {
			state Reference<ScanReturnedContext> nextIndex = waitNext(indexStream);
			DataValue indexDv = wait(getRecursiveKnownPresent(nextIndex));
			bson::BSONObj indexObj = indexDv.getPackedObject().getOwned();
			ret.push_back(indexObj);
			flowControlLock->release();
		} catch (Error& e) {
			checkpoint->stop();
			if (e.code() == error_code_end_of_stream)
				break;
			throw;
		}
	}
	return ret;
}

// Parse a Projection tree from a BSON projection specification
Reference<Projection> parseProjection(bson::BSONObj const& fieldSelector) {
	std::set<std::string> parsedFields;
	bool includeID = true;
	bool hasIncludeValue = false;

	Reference<Projection> root(new Projection());

	for (auto itr = fieldSelector.begin(); itr.more(); ++itr) {
		bson::BSONElement el = *itr;
		std::string fieldName = el.fieldName();
		if (fieldName.length() >= 2 && fieldName.compare(fieldName.length() - 2, 2, ".$") == 0) {
			// TODO: $ operator
			throw invalid_projection();
		} else if (el.type() == bson::Object) {
			// TODO: $slice operator
			// TODO: $elemMatch operator
			// TODO: $meta operator
			throw invalid_projection();
		} else {
			bool elIncluded = el.trueValue();
			if (fieldName == DocLayerConstants::ID_FIELD) {
				// Specifying _id:1 makes the projection an inclusive projection
				if (elIncluded) {
					if (hasIncludeValue && root->included) {
						throw invalid_projection();
					}

					root->included = false; // Our specification is inclusive, so unspecified fields should be excluded
					hasIncludeValue = true;
				}

				includeID = elIncluded;
			} else {
				if (!hasIncludeValue) {
					root->included =
					    !elIncluded; // If el was inclusive, then we should exclude unspecified fields (and vice versa)
					hasIncludeValue = true;
				}

				// inclusive and exclusive modes cannot be mixed
				else if (root->included == elIncluded) {
					throw invalid_projection();
				}

				// The whole _id field is either included or excluded as a unit regardless of whether its sub-fields are
				// included in the projection
				if (fieldName.size() >= 4 && fieldName.compare(0, 4, "_id.") == 0) {
					continue;
				}

				// Split the field into its consituent parts and insert the parts into the Projection tree
				if (parsedFields.insert(fieldName).second) {
					size_t pos = -1;
					size_t prev;
					Reference<Projection>* current = &root;
					do {
						prev = pos + 1;
						pos = fieldName.find('.', prev);
						current = &(*current)->fields[fieldName.substr(prev, std::max(pos - prev, (size_t)1))];
						if (!*current) {
							*current = Reference<Projection>(new Projection());
						}

						// The last field in the fieldName spec should be included iff the element was inclusive. All
						// others should be the opposite.
						if (pos == std::string::npos) {
							(*current)->included = elIncluded;
						} else {
							(*current)->included = !elIncluded;
						}

					} while (pos != prev && pos < fieldName.length() - 1);
				}
			}
		}
	}

	if (!hasIncludeValue) {
		root->included = true;
	}

	if (includeID != root->included) {
		root->fields[DocLayerConstants::ID_FIELD] = Reference<Projection>(new Projection(includeID));
	}

	Projection::filterUnneededReads(root);
	return root;
}
