/*
 * QLProjection.actor.cpp
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

#include "QLProjection.h"

ACTOR Future<bson::BSONObj> projectDocument_impl(Reference<IReadContext> doc, Reference<Projection> projection) {
	try {
		// If we are just reading the root document, then we can return the whole thing
		if (projection->shouldBeRead) {
			DataValue dv = wait(getRecursiveKnownPresent(doc, projection));
			return dv.getPackedObject().getOwned();
		}

		// Read all of the sub-ranges of the document
		state std::vector<Future<Optional<DataValue>>> dataValueFutures;
		for (auto itr = projection->begin(); itr != projection->end(); ++itr) {
			auto projCx = doc;
			for (const auto& field : itr.path) {
				projCx = projCx->getSubContext(DataValue(field).encode_key_part());
			}
			if (itr.projection()->shouldBeRead) {
				dataValueFutures.push_back(getMaybeRecursiveIfPresent(projCx, itr.projection()));
			} else {
				dataValueFutures.push_back(projCx->get(LiteralStringRef("")));
			}
		}

		Void _ = wait(waitForAll(dataValueFutures));

		std::vector<BOBObj> currentPath;
		currentPath.emplace_back(-1, "");

		// Build the result object from the individual sub-queries.
		auto valueItr = dataValueFutures.begin();
		for (auto itr = projection->begin(); itr != projection->end(); ++itr, ++valueItr) {
			ASSERT(valueItr != dataValueFutures.end());

			// Find the first descendant where the current read and the previous one differ
			int index = 0;
			for (; index < itr.path.size() && index < currentPath.size() - 1 &&
			       itr.path[index] == currentPath[index + 1].fieldname;
			     ++index) {
				// No body
			}

			// Fold in all of the finished objects that will no longer be modified
			for (int rIndex = currentPath.size() - 1; rIndex > index; --rIndex) {
				if (currentPath[rIndex].isArrayLength >= 0) {
					currentPath[rIndex - 1].append(currentPath[rIndex].fieldname,
					                               DataValue(bson::BSONArray(currentPath[rIndex].build().getOwned())));
				} else {
					currentPath[rIndex - 1].append(currentPath[rIndex].fieldname,
					                               DataValue(currentPath[rIndex].build().getOwned()));
				}
			}

			currentPath.resize(index + 1);

			// Append the results of this sub-query to our bson object
			if (valueItr->get().present()) {
				DataValue dv = valueItr->get().get();
				if (!itr.projection()->shouldBeRead) {
					if (!dv.isSimpleType()) {
						ASSERT(dv.getBSONType() !=
						       bson::BSONType::Array); // SOMEDAY: This restriction should be replaced by proper array
						                               // handling if the code in filterUnneededReads is enabled
						currentPath.emplace_back(-1, itr.path.back());
					}
				} else {
					currentPath.back().append(itr.path.back(), valueItr->get().get());
				}
			}
		}

		ASSERT(valueItr == dataValueFutures.end());

		// Collapse the object stack into a single object and return it
		for (int index = currentPath.size() - 1; index > 0; --index) {
			if (currentPath[index].isArrayLength >= 0) {
				currentPath[index - 1].append(currentPath[index].fieldname,
				                              DataValue(bson::BSONArray(currentPath[index].build().getOwned())));
			} else {
				currentPath[index - 1].append(currentPath[index].fieldname,
				                              DataValue(currentPath[index].build().getOwned()));
			}
		}

		return currentPath[0].build().getOwned();
	} catch (Error& e) {
		if (e.code() != error_code_actor_cancelled)
			TraceEvent(SevError, "BD_projectDocument").error(e);
		throw;
	}
}

ACTOR Future<bson::BSONObj> projectDocument(Reference<IReadContext> doc,
                                            Reference<Projection> projection,
                                            Optional<bson::BSONObj> orderObj) {
	if (orderObj.present()) {
		Future<bson::BSONObj> fpDoc = projectDocument_impl(doc, projection);
		bson::BSONObjBuilder bob;
		for (auto i = orderObj.get().begin(); i.more();) {
			auto el = i.next();
			bob.append(el.fieldName(), 1);
		}
		bson::BSONObj sortKeyobj = bob.obj();
		state Future<bson::BSONObj> fpSortKey = projectDocument_impl(doc, parseProjection(sortKeyobj));
		state bson::BSONObj pDoc = wait(fpDoc);
		bson::BSONObj pSortKey = wait(fpSortKey);
		bson::BSONObjBuilder bob2;
		bob2.appendObject("doc", pDoc.objdata());
		bob2.appendObject("sortKey", pSortKey.objdata());
		return bob2.obj().getOwned();
	} else {
		bson::BSONObj ret = wait(projectDocument_impl(doc, projection));
		return ret;
	}
}

ACTOR Future<DataValue> getRecursive(Reference<IReadContext> cx, Reference<Projection> projection, int isArrayLength) {
	state Projector projector(projection);
	state std::vector<BOBObj> bobs;
	state GenFutureStream<FDB::KeyValue> descendantStream = cx->getDescendants();

	bobs.emplace_back(isArrayLength, "");

	loop {
		try {
			FDB::KeyValue kv = waitNext(descendantStream);

			int nItems = 0;
			StringRef last = DataKey::decode_item_rev(kv.key, 0, &nItems);

			for (auto i = bobs.size(); i > nItems; --i) {
				auto popped = std::move(bobs.back());
				bobs.pop_back();

				if (popped.isArrayLength >= 0) {
					bobs.back().append(popped.fieldname, DataValue(bson::BSONArray(popped.build())));
				} else {
					bobs.back().append(popped.fieldname, DataValue(popped.build()));
				}
			}
			DataValue kdv = DataValue::decode_key_part(last);
			std::string fieldname =
			    kdv.getSortType() == DVTypeCode::STRING ? kdv.getString() : std::to_string((int)kdv.getDouble());
			DataValue dv = DataValue::decode_value(kv.value);
			Projector::IncludeType includeType =
			    projector.includeNextField(nItems, fieldname, dv.isSimpleType(), bobs.back().isArrayLength >= 0);
			if (includeType != Projector::EXCLUDE) {
				switch (dv.getSortType()) {
				case DVTypeCode::OBJECT:
					bobs.emplace_back(-1, fieldname);
					break;
				case DVTypeCode::ARRAY:
					bobs.emplace_back(includeType == Projector::INCLUDE_ALL ? dv.getArraysize() : 0, fieldname);
					break;
				default:
					bobs.back().append(fieldname, dv);
				}
			}
		} catch (Error& e) {
			if (e.code() == error_code_end_of_stream) {
				break;
			}
			throw;
		}
	}

	for (auto i = bobs.size() - 1; i > 0; --i) {
		if (bobs[i].isArrayLength >= 0) {
			bobs[i - 1].append(bobs[i].fieldname, DataValue(bson::BSONArray(bobs[i].build().getOwned())));
		} else {
			bobs[i - 1].append(bobs[i].fieldname, DataValue(bobs[i].build().getOwned()));
		}
	}

	if (bobs[0].isArrayLength >= 0)
		return DataValue(bson::BSONArray(bobs[0].build().getOwned()));
	else
		return DataValue(bobs[0].build().getOwned());
}

Future<DataValue> getRecursiveKnownPresent(Reference<IReadContext> const& cx, Reference<Projection> const& projection) {
	return getRecursive(cx, projection, -1);
}

ACTOR Future<Optional<DataValue>> getMaybeRecursiveIfPresent(Reference<IReadContext> cx,
                                                             Reference<Projection> projection) {
	state Optional<DataValue> rootValue = wait(cx->get(LiteralStringRef("")));

	if (!rootValue.present()) {
		return Optional<DataValue>();
	}

	bool rootFullyIncluded = !projection || projection->included;

	if (rootValue.get().isSimpleType()) {
		if (rootFullyIncluded) {
			return rootValue.get();
		} else {
			return Optional<DataValue>();
		}
	}

	int isArrayLength = -1;
	if (rootValue.get().getBSONType() == bson::BSONType::Array) {
		isArrayLength = rootFullyIncluded ? rootValue.get().getArraysize() : 0;
	}

	DataValue dv = wait(getRecursive(cx, projection, isArrayLength));
	return dv;
}

void BOBObj::append(std::string fn, DataValue dv) {
	if (isArrayLength >= 0) {
		char* r;
		long int n = strtol(fn.data(), &r, 10);
		ASSERT(!*r);
		bool compact = isArrayLength == 0;
		while (!compact && currentLoc < n) {
			bob->appendNull(fn);
			currentLoc++;
		}
		_append(fn, dv);
		currentLoc++;
	} else {
		_append(fn, dv);
	}
}

void BOBObj::_append(std::string fn, DataValue dv) {
	switch (dv.getBSONType()) {
	case bson::BSONType::NumberInt:
		bob->append(fn, dv.getInt());
		break;
	case bson::BSONType::NumberLong:
		bob->append(fn, (long long)dv.getLong());
		break;
	case bson::BSONType::NumberDouble:
		bob->append(fn, dv.getDouble());
		break;
	case bson::BSONType::String:
		bob->append(fn, dv.getString());
		break;
	case bson::BSONType::BinData:
		bob->appendBinData(fn, dv.getBinaryData().size() - 5, (bson::BinDataType) * (dv.getBinaryData().begin() + 4),
		                   dv.getBinaryData().begin() + 5);
		break;
	case bson::BSONType::Bool:
		bob->append(fn, dv.getBool());
		break;
	case bson::BSONType::jstOID:
		bob->append(fn, dv.getId());
		break;
	case bson::BSONType::Date:
		bob->append(fn, dv.getDate());
		break;
	case bson::BSONType::jstNULL:
		bob->appendNull(fn);
		break;
	case bson::BSONType::Object:
		bob->append(fn, dv.getPackedObject());
		break;
	case bson::BSONType::Array:
		bob->appendArray(fn, dv.getPackedArray());
		break;
	case bson::BSONType::RegEx:
		bob->appendAs(dv.getRegExObject(), fn);
		break;
	case bson::BSONType::MinKey:
		bob->appendMinKey(fn);
		break;
	case bson::BSONType::MaxKey:
		bob->appendMaxKey(fn);
		break;
	default:
		throw internal_error();
	}
}

bson::BSONObj BOBObj::build() {
	if (isArrayLength >= 0) {
		bool compact = isArrayLength == 0;
		while (!compact && currentLoc < isArrayLength) {
			bob->appendNull(std::to_string(currentLoc));
			currentLoc++;
		}
		return bob->obj();
	} else {
		return bob->obj();
	}
}