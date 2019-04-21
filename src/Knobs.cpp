/*
 * Knobs.cpp
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

#include "Knobs.h"
#include "flow/flow.h"

DocLayerKnobs const* DOCLAYER_KNOBS = new DocLayerKnobs();

#define init(knob, value) initKnob(knob, value, #knob)

DocLayerKnobs::DocLayerKnobs(bool enable) {
	init(MAX_PROJECTION_READ_RANGES, 10);
	init(MULTI_MULTIKEY_INDEX_MAX, 1000);
	init(CONNECTION_MAX_PIPELINE_DEPTH, 50);

	init(FLOW_CONTROL_LOCK_PERMITS, 50);
	if (enable)
		FLOW_CONTROL_LOCK_PERMITS = 3;
	init(NONISOLATED_RW_INTERNAL_BUFFER_MAX, 1000);
	if (enable)
		NONISOLATED_RW_INTERNAL_BUFFER_MAX = 1;
	init(NONISOLATED_INTERNAL_TIMEOUT, 0.1);
	if (enable)
		NONISOLATED_INTERNAL_TIMEOUT = 0.04;

	init(MAX_RETURNABLE_DOCUMENTS, 101);
	init(MAX_RETURNABLE_DATA_SIZE, (1 << 20) * 16);
	init(CURSOR_EXPIRY, 60 * 10); /* seconds */
	init(DEFAULT_RETURNABLE_DATA_SIZE, (1 << 20) * 4);
	init(SLOW_QUERY_THRESHOLD_MICRO_SECONDS, 10 * 1000 * 1000);
}

bson::BSONObj DocLayerKnobs::dumpKnobs() const {
	bson::BSONObjBuilder bob;
	for (auto k : int_knobs) {
		bob.appendIntOrLL(k.first, *k.second);
	}
	for (auto k : int64_knobs) {
		bob.appendIntOrLL(k.first, *k.second);
	}
	for (auto k : double_knobs) {
		bob.appendNumber(k.first, *k.second);
	}
	bob << "ok" << 1;
	return bob.obj();
}
