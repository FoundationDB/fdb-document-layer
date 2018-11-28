/*
 * QLProjection.h
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

#ifndef _QL_PROJECTION_H
#define _QL_PROJECTION_H
#pragma once

#include "QLContext.h"

struct Projection : public ReferenceCounted<Projection> {

	/**
	 * Iterates over a Projection tree in lexicographic order. All nodes whose parent projection had shouldBeRead set to
	 * false will be included in the iteration. All others will be excluded (note that the root node will be excluded
	 * since it has no parent).
	 */
	class Iterator {
	public:
		std::vector<std::string> path;
		Reference<Projection> projection() {
			return stack.empty() ? Reference<Projection>(nullptr) : stack.back().projection;
		}

		Iterator& operator++();
		Iterator operator++(int);

		bool operator==(Projection::Iterator const& other) const;

		static Projection::Iterator const end;

	private:
		struct StackEntry {
			Reference<Projection> projection;
			std::map<std::string, Reference<Projection>>::iterator itr;

			bool operator==(StackEntry const& other) const {
				return projection == other.projection && itr == other.itr;
			}
			explicit StackEntry(Reference<Projection> projection)
			    : projection(projection), itr(projection->fields.begin()) {}
		};

		std::vector<StackEntry> stack;
		friend struct Projection;
	};

	// True if all fields not explicitly listed in the fields map will be included by the projection. Otherwise, all
	// such fields will be excluded.
	bool included;

	// True if the projection's full range of keys should be read when projecting a document. Otherwise, only the root
	// key is read.
	bool shouldBeRead;

	// Projection specification for sub-fields of this projection's field. If a field is not listed in fields, then its
	// inclusion is determined by the included variable.
	std::map<std::string, Reference<Projection>> fields;

	std::string debugString(bool first = true);

	explicit Projection(bool included = true) : included(included), shouldBeRead(true) {}

	// True if the read of this projection's field can be replaced by reads of its sub-fields
	bool expandable() { return !fields.empty() && !included; }

	/**
	 * Projections with the smallest number of root children should sort last. Those which have no children or have the
	 * included flag set are not expandable and should sort first.
	 */
	struct SizeComparer {
		bool operator()(Reference<Projection> const& lhs, Reference<Projection> const& rhs) const {
			return rhs->expandable() && (!lhs->expandable() || rhs->fields.size() < lhs->fields.size());
		}
	};

	static void filterUnneededReads(Reference<Projection> const& startingProjection,
	                                int maxRanges = DOCLAYER_KNOBS->MAX_PROJECTION_READ_RANGES);

	Iterator begin();
	Iterator const end();
};

class Projector {
private:
	std::vector<std::pair<std::string, Reference<Projection>>> projectionStack;

public:
	enum IncludeType { EXCLUDE, INCLUDE_ALL, INCLUDE_PARTIAL };

	explicit Projector(Reference<Projection> projection) {
		if (projection) {
			projectionStack.emplace_back("", projection);
		}
	}

	IncludeType includeNextField(int depth, std::string fieldName, bool isSimple, bool inArray);
};

Future<bson::BSONObj> projectDocument(const Reference<IReadContext>& di,
                                      const Reference<Projection>& projection,
                                      const Optional<bson::BSONObj>& orderObj);

Future<Optional<DataValue>> getMaybeRecursiveIfPresent(
    const Reference<IReadContext>& cx,
    const Reference<Projection>& projection = Reference<Projection>());
Future<DataValue> getMaybeRecursive(const Reference<IReadContext>& cx, const StringRef& path);
Future<DataValue> getRecursiveKnownPresent(const Reference<IReadContext>& cx,
                                           const Reference<Projection>& projection = Reference<Projection>());

// Parse a Projection tree from a BSON projection specification
Reference<Projection> parseProjection(bson::BSONObj const& fieldSelector); // FIXME: Where does this belong?

class BOBObj {
public:
	BOBObj() = default;

	// If isArrayLength < 0, then this is not an array
	// If isArrayLength == 0, then the items in the array will be compacted (if the array is sparse)
	// If isArrayLength > 0, then the array will be a (possibly sparse) array of length isArrayLength
	BOBObj(int isArrayLength, std::string fieldname)
	    : bob(new bson::BSONObjBuilder()), isArrayLength(isArrayLength), fieldname(fieldname), currentLoc(0) {}
	~BOBObj() noexcept(false) { delete bob; }

	BOBObj(BOBObj&& rhs) noexcept(false) {
		bob = rhs.bob;
		rhs.bob = nullptr;
		fieldname = std::move(rhs.fieldname);
		isArrayLength = rhs.isArrayLength;
		currentLoc = rhs.currentLoc;
	}

	const BOBObj& operator=(BOBObj&& rhs) { return (*this); }

	void append(std::string fn, DataValue dv);
	void _append(std::string fn, DataValue dv);
	bson::BSONObj build();

	std::string fieldname;
	int currentLoc;
	int isArrayLength;

protected:
	bson::BSONObjBuilder* bob;
	BOBObj(const BOBObj& rhs);
	const BOBObj& operator=(const BOBObj& rhs);
};

#endif