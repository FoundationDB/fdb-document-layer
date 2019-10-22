/*
 * OplogMonitor.h
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
#if defined(NO_INTELLISENSE) && !defined(_OPLOG_MONITOR_ACTOR_G_H_)
#define _OPLOG_MONITOR_ACTOR_G_H_
#include "OplogMonitor.actor.g.h"
#elif !defined(_OPLOG_MONITOR_ACTOR_H_)
#define _OPLOG_MONITOR_ACTOR_H_

#pragma once

#include "DocLayer.h"
#include "bindings/flow/fdb_flow.h"
#include "bindings/flow/DirectorySubspace.h"
#include "flow/flow.h"
#include "ExtStructs.h"
#include "flow/actorcompiler.h" // This must be the last #include.


void sendLogId(PromiseStream<std::string> logs, std::string oId);
ACTOR Future<Reference<DirectorySubspace>> logsDirectory(Reference<DocumentLayer> docLayer);
ACTOR Future<Void> outStream(Reference<DocumentLayer> docLayer, Deque<std::string> oIds, Reference<ExtChangeStream> output);
ACTOR void logStreamQuery(Reference<DocumentLayer> docLayer, Deque<std::string> oIds, PromiseStream<bson::BSONObj> output);
ACTOR void logStreamReaderActor(Reference<DocumentLayer> docLayer, FutureStream<std::string> idsStream);
ACTOR void logStreamWatcherActor(Reference<DocumentLayer> docLayer, PromiseStream<std::pair<std::string, std::string>> keysWriter);
ACTOR void logStreamScanActor(
    Reference<DocumentLayer> docLayer, 
    Reference<ExtChangeStream> output,
    FutureStream<std::pair<std::string, std::string>> keysReader
);

ACTOR void deleteExpiredLogs(Reference<DocumentLayer> docLayer, double ts);
void oplogMonitor(Reference<DocumentLayer> docLayer, double logsDeletionOffset);

#endif /* _OPLOG_MONITOR_ACTOR_H */