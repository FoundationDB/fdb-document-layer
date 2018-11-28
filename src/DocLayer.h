/*
 * DocLayer.h
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

#ifndef _DOCLAYER_H_
#define _DOCLAYER_H_

#pragma once

#include "bindings/flow/DirectoryLayer.h"
#include "bindings/flow/DirectorySubspace.h"
#include "bindings/flow/fdb_flow.h"
#include "flow/ActorCollection.h"
#include "flow/flow.h"

#include "Cursor.h"
#include "IMetric.h"
#include "Knobs.h"
#include "MetadataManager.h"

struct ConnectionOptions {
	bool pipelineCompatMode;
	int64_t retryLimit;
	int64_t timeout;

	ConnectionOptions(bool pipelineCompatMode, int64_t retryLimit, int64_t timeout)
	    : pipelineCompatMode(pipelineCompatMode), retryLimit(retryLimit), timeout(timeout) {}
};

struct DocumentLayer : ReferenceCounted<DocumentLayer>, NonCopyable {
	DocumentLayer(ConnectionOptions defaultConnectionOptions,
	              Reference<FDB::DatabaseContext> database,
	              Reference<DirectorySubspace> rootDirectory)
	    : defaultConnectionOptions(defaultConnectionOptions),
	      database(database),
	      backgroundTasks(false),
	      rootDirectory(rootDirectory),
	      mm(new MetadataManager(this)) {}

	Reference<FDB::DatabaseContext> database;
	Reference<MetadataManager> mm;
	ConnectionOptions defaultConnectionOptions;
	ActorCollection backgroundTasks;
	Reference<DirectorySubspace> rootDirectory;
	static IMetricReporter* metricReporter;

	// Stats
	uint32_t nrConnections = 0;
};

Future<Void> wrapError(const Future<Void>& actorThatCouldThrow);

#endif /* _DOCLAYER_H_ */
