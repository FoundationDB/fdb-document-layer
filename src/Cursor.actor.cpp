/*
 * Cursor.actor.cpp
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

#include "Cursor.h"
#include "DocLayer.h"
#include "Knobs.h"
#include "flow/actorcompiler.h" // This must be the last #include.

int32_t Cursor::prune(std::map<int64_t, Reference<Cursor>>& cursors) {
	time_t now = time(nullptr);
	int32_t pruned = 0;
	std::vector<Reference<Cursor>> to_be_pruned;

	for (auto it = cursors.begin(); it != cursors.end();) {
		if (it->second && now >= it->second->expiry) {
			to_be_pruned.push_back(it->second);
		}
		++it;
	}

	for (const auto& i : to_be_pruned) {
		(void)pluck(i);
		pruned++;
	}

	return pruned;
}

void Cursor::pluck(Reference<Cursor> cursor) {
	if (cursor) {
		cursor->siblings->erase(cursor->id);
		cursor->checkpoint->stop();
		DocumentLayer::metricReporter->captureGauge(DocLayerConstants::MT_GUAGE_ACTIVE_CURSORS,
		                                            cursor->siblings->size());
	}
}

Reference<Cursor> Cursor::add(std::map<int64_t, Reference<Cursor>>& siblings, Reference<Cursor> cursor) {
	cursor->siblings = &siblings;
	siblings[cursor->id] = cursor;
	DocumentLayer::metricReporter->captureGauge(DocLayerConstants::MT_GUAGE_ACTIVE_CURSORS, siblings.size());
	return cursor;
}
