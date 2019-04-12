/*
 * QLExpression.actor.cpp
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

#include "QLExpression.h"

ACTOR Future<Optional<std::pair<Standalone<StringRef>, DataValue>>> getArrayAncestor(DataKey dk,
                                                                                     Reference<IReadContext> cx,
                                                                                     int start,
                                                                                     bool checkLast) {
	std::vector<Future<Optional<DataValue>>> futures;

	for (int i = start; i < dk.size(); ++i) {
		futures.push_back(cx->get(dk.prefix(i)));
	}
	if (checkLast && start <= dk.size()) {
		futures.push_back(cx->get(dk.bytes()));
	}

	if (!futures.empty()) {
		std::vector<Optional<DataValue>> results = wait(getAll(futures));

		for (int i = 0; i < (checkLast ? results.size() - 1 : results.size()); ++i) {
			if (results[i].present() && results[i].get().getBSONType() == bson::BSONType::Array) {
				return std::make_pair(dk.prefix(start + i), results[i].get());
			}
		}
		if (checkLast && start <= dk.size()) {
			if (results[results.size() - 1].present() &&
			    results[results.size() - 1].get().getBSONType() == bson::BSONType::Array) {
				return std::make_pair(dk.bytes(), results[results.size() - 1].get());
			}
		}
	}

	return Optional<std::pair<Standalone<StringRef>, DataValue>>();
}

static Future<Void> doPathExpansion(PromiseStream<Reference<IReadContext>> const& promises,
                                    Standalone<StringRef> const& queryPath,
                                    Reference<IReadContext> const& document,
                                    int const& arrayLookStart,
                                    bool const& isOuter,
                                    bool const& expandLastArray,
                                    bool const& imputeNulls);

ACTOR static Future<Void> doPathExpansionIfNotArray(PromiseStream<Reference<IReadContext>> promises,
                                                    Standalone<StringRef> queryPath,
                                                    Standalone<StringRef> arrayPath,
                                                    Reference<IReadContext> document,
                                                    bool expandLastArray,
                                                    bool imputeNulls) {
	Optional<DataValue> v = wait(document->get(arrayPath));
	if (!v.present() || v.get().getSortType() != DVTypeCode::ARRAY) {
		Void _ = wait(doPathExpansion(promises, queryPath, document,
		                              DataKey::decode_bytes(Standalone<StringRef>(arrayPath)).size() + 1, false,
		                              expandLastArray, imputeNulls));
	}
	return Void();
}

/**
 * If the query is on arrays - Mongo data model is bit tricky when it comes to array expansion. High level
 * explanation is here - https://docs.mongodb.com/manual/tutorial/query-arrays/. Array expansion code obeys
 * following rules
 *
 *  - Case 1: If the component after array root is not an index (not a numeric component), then just expand
 *  the query path with all the possible array indexes. For example, if query path is a.b.c and b turned out
 *  to be array of size 3, then this query path would be expanded to a.b.0.c, a.b.1.c and a.b.2.c. To avoid
 *  nested array expansion, bump arrayLookStart by 2. So, we skip expansion if one of the array elements
 *  themselves are arrays.
 *
 *  - Case 2: Component after array root is numeric and treating it as array index. Ex: query path is
 *  a.b.1.c, b is array and treating "1" as array index. As we have explicit array index, no need of array
 *  expansion, so just bump arrayLookStart by 1. NOTE: There is a corner case here. If array element
 *  referred by explicit numeric index (In example, a.b.1), turns out to be an array and it's expanded then
 *  it can't match leaf elements. Ex: queryPath -> a.b.1, a.b is array and a.b.1 is array, then we don't
 *  allow array expansion on 1. But, it is okay if queryPath is a.b.1.c, as "1" is not leaf element.
 *
 *  - Case 3: Component after array root is numeric and treating it as field name. Ex: a.b.1.c, b is array
 *  of size 3 and treating "1" as field name. This can be expanded to a.b.0.1.c, a.b.1.1.c and a.b.2.1.c. As
 *  Doc Layer serializes numeric field name and array index same way, we have to pass this information.
 */
ACTOR static Future<Void> doArrayExpansion(PromiseStream<Reference<IReadContext>> promises,
                                           Standalone<StringRef> queryPath,
                                           Reference<IReadContext> document,
                                           std::pair<Standalone<StringRef>, DataValue> arrayAncestor,
                                           bool expandLastArray,
                                           bool imputeNulls) {
	Standalone<StringRef> arrayRootPath = arrayAncestor.first;
	const auto pathEnd = queryPath.substr(arrayRootPath.size(), queryPath.size() - arrayRootPath.size());
	const auto arrayRootPathSize = DataKey::decode_bytes(arrayRootPath).size();
	std::vector<Future<Void>> futures;

	const auto nextComponentIsNumeric = (pathEnd.size() && pathEnd[0] == (uint8_t)DVTypeCode::NUMBER);
	// Case 2 - Treat next component as index
	if (nextComponentIsNumeric) {
		const auto isLeaf = (arrayRootPathSize + 1 == DataKey::decode_bytes(queryPath).size());

		// If numeric index is pointing to leaf, then don't expand it even if it is an array.
		futures.push_back(doPathExpansion(promises, queryPath, document, arrayRootPathSize + 1, false,
		                                  expandLastArray && !isLeaf, imputeNulls));
	}

	for (int i = 0; i < arrayAncestor.second.getArraysize(); ++i) {
		Standalone<StringRef> arrayElementPath = arrayRootPath.withSuffix(DataValue(i).encode_key_part());
		Standalone<StringRef> newPath = arrayElementPath.withSuffix(pathEnd);

		// Case 3 - Treat next component as field name.
		if (nextComponentIsNumeric) {
			futures.push_back(doPathExpansionIfNotArray(promises, StringRef(newPath), arrayElementPath, document,
			                                            expandLastArray, imputeNulls));
		} else { // Case 1
			futures.push_back(doPathExpansion(promises, newPath, document, arrayRootPathSize + 2, false,
			                                  expandLastArray, imputeNulls));
		}
	}
	Void _ = wait(waitForAll(futures));

	return Void();
}

ACTOR static Future<Void> doPathExpansion(PromiseStream<Reference<IReadContext>> promises,
                                          Standalone<StringRef> queryPath,
                                          Reference<IReadContext> document,
                                          int arrayLookStart,
                                          bool isOuter,
                                          bool expandLastArray,
                                          bool imputeNulls) {
	try {
		state DataKey dk = DataKey::decode_bytes(queryPath);
		ASSERT(dk.size());

		state Future<Optional<std::pair<Standalone<StringRef>, DataValue>>> arrayAncestorF =
		    getArrayAncestor(dk, document, arrayLookStart, expandLastArray);

		state Reference<IReadContext> scx = document->getSubContext(queryPath);
		state Optional<DataValue> v = wait(scx->get(StringRef()));
		if (v.present()) {
			promises.send(scx);
		}

		state Optional<std::pair<Standalone<StringRef>, DataValue>> arrayAncestor = wait(arrayAncestorF);
		if (arrayAncestor.present()) {
			Void _ = wait(
			    doArrayExpansion(promises, queryPath, document, arrayAncestor.get(), expandLastArray, imputeNulls));
		}

		// MongoDB will impute a null here (for certain predicates) if the "most recent existing ancestor" in the dotted
		// path is not an array. However, we may have a mutated path, since this could be an inner recursive call in the
		// expansion. So we also need to check that if the "most recent existing ancestor" is a primitive value, then
		// its direct parent is also not an array. If the MREA is an object, then no check is necessary, since MongoDB
		// switches back into null-imputing mode if that happens (even if we expanded an array right before).
		if (imputeNulls && !v.present() && !arrayAncestor.present()) {
			std::vector<Future<Optional<DataValue>>> futureAncestors;
			futureAncestors.push_back(document->get(StringRef()));
			for (int i = 1; i <= dk.size(); i++)
				futureAncestors.push_back(document->get(dk.prefix(i)));

			std::vector<Optional<DataValue>> ancestors = wait(getAll(futureAncestors));

			for (auto j = ancestors.rbegin();
			     !(j == ancestors.rend()) /* yes, it can be !=, but MSVC has a bug, hence !(==)*/; ++j) {
				if (j->present()) {
					DVTypeCode type = j->get().getSortType();
					if (type == DVTypeCode::OBJECT ||
					    (type != DVTypeCode::ARRAY &&
					     (j + 1 == ancestors.rend() || (j + 1)->get().getSortType() != DVTypeCode::ARRAY))) {
						promises.send(Reference<NullContext>(new NullContext()));
					}
					break;
				}
			}
		}

		if (isOuter) {
			promises.sendError(end_of_stream());
		}

		return Void();
	} catch (Error& e) {
		// fprintf(stderr, "ERROR in path expansion: %s\n", e.what());
		promises.sendError(e);
		throw;
	}
}

GenFutureStream<Reference<IReadContext>> ExtPathExpression::evaluate(Reference<IReadContext> const& document) {
	PromiseStream<Reference<IReadContext>> p;
	GenFutureStream<Reference<IReadContext>> r(p.getFuture());
	r.actor = doPathExpansion(p, path, document, 1, true, expandLastArray && path.size(),
	                          imputeNulls); // Even if expandLastArray, the empty path ->"" not ->"$n?"

	return r;
}
