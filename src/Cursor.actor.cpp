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

// Cursors for accessibility via difference connections.
// Dirty but fast fix.
std::map<int64_t, Reference<Cursor>> Cursor::allCursors;

int32_t Cursor::prune(std::map<int64_t, Reference<Cursor>>& cursors, bool pruneAll) {
	time_t now = time(nullptr);
	int32_t pruned = 0;
	std::vector<Reference<Cursor>> to_be_pruned;

	try {
		for (auto it = cursors.begin(); it != cursors.end();) {
			if (it->second && (pruneAll || now >= it->second->expiry)) {
				to_be_pruned.push_back(it->second);
			}
			++it;
		}

		for (const auto& i : to_be_pruned) {
			(void)pluck(i);
			pruned++;
		}
	} catch (Error& e) {
		TraceEvent(SevError, "BD_cursor_prune_failed").error(e);
		// Ignoring error just to keep the code consistent with previous behaviour.
		// Cursor design could be made lot better than this.
	}

	return pruned;
}

void Cursor::pluck(Reference<Cursor> cursor) {
	if (cursor) {
		allCursors.erase(cursor->id);
		cursor->siblings->erase(cursor->id);
		cursor->checkpoint->stop();
		DocumentLayer::metricReporter->captureGauge(DocLayerConstants::MT_GUAGE_ACTIVE_CURSORS,
		                                            cursor->siblings->size());
	}
}

Reference<Cursor> Cursor::add(std::map<int64_t, Reference<Cursor>>& siblings, Reference<Cursor> cursor) {
	cursor->siblings = &siblings;
	siblings[cursor->id] = cursor;
	allCursors[cursor->id] = cursor;
	DocumentLayer::metricReporter->captureGauge(DocLayerConstants::MT_GUAGE_ACTIVE_CURSORS, siblings.size());
	return cursor;
}

Reference<Cursor> Cursor::get(int64_t id) {
	return allCursors[id];
}
