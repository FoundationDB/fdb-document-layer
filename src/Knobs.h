/*
 * Knobs.h
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

#ifndef DOCLAYER_KNOBS_H
#define DOCLAYER_KNOBS_H
#pragma once

#include "bson.h"
#include "flow/Knobs.h"

class DocLayerKnobs : public Knobs {
public:
	int MAX_PROJECTION_READ_RANGES;
	int MULTI_MULTIKEY_INDEX_MAX;
	int FLOW_CONTROL_LOCK_PERMITS;
	int NONISOLATED_RW_INTERNAL_BUFFER_MAX;
	int CONNECTION_MAX_PIPELINE_DEPTH;
	double NONISOLATED_INTERNAL_TIMEOUT;
	int MAX_RETURNABLE_DOCUMENTS;
	int MAX_RETURNABLE_DATA_SIZE;
	int CURSOR_EXPIRY;
	int DEFAULT_RETURNABLE_DATA_SIZE;

	explicit DocLayerKnobs(bool randomize = false);

	bson::BSONObj dumpKnobs() const;
};

extern DocLayerKnobs const* DOCLAYER_KNOBS;

#endif
