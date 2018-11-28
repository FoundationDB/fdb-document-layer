/*
 * StatusService.actor.cpp
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

#include "ExtUtil.actor.h"
#include "StatusService.h"
#include "bindings/flow/Tuple.h"
#include "bson.h"
#include "flow/Platform.h"

std::string getStatus(std::string version, std::string host, int32_t port, uint64_t startTime, std::string id) {
	double time = timer() * 1000;
	return BSON(
	           // clang-format off
		"name" << "Document Layer" <<
		"timestamp" << time <<
		"uptime" << time - startTime <<
		"version" << version <<
		"host" << host <<
		"port" << port <<
		"id" << id <<
		"memory_usage" << (long long)getMemoryUsage() <<
		"resident_size" << (long long)getResidentMemoryUsage() <<
		"main_thread_cpu_seconds" << getProcessorTimeThread() <<
		"process_cpu_seconds" << getProcessorTimeProcess()
	           // clang-format on
	           )
	    .jsonString();
}

ACTOR void statusUpdateActor(std::string version,
                             std::string host,
                             int32_t port,
                             Reference<DocumentLayer> docLayer,
                             uint64_t startTime) {
	Reference<DirectoryLayer> d = Reference<DirectoryLayer>(new DirectoryLayer());
	state Reference<DirectorySubspace> statusDirectory = wait(runRYWTransaction(
	    docLayer->database,
	    [d](Reference<DocTransaction> tr) {
		    return d->createOrOpen(tr->tr, {LiteralStringRef("Status Monitor"), LiteralStringRef("Layers")});
	    },
	    -1, 0));
	state std::string id = g_nondeterministic_random->randomUniqueID().toString();
	FDB::Tuple t;
	t.append(LiteralStringRef("Document Layer"), true).append(id, true);
	state FDB::Key instanceKey = statusDirectory->pack(t);

	loop {
		try {
			std::string newStatus = getStatus(version, host, port, startTime, id);
			// fprintf(stderr, "%s\n", newStatus.c_str());
			FDB::Tuple tuple;
			tuple.append(newStatus, true);
			FDB::Value v = tuple.pack();
			Future<Void> watch = wait(runTransactionAsync(docLayer->database,
			                                              [&, v](Reference<DocTransaction> tr) {
				                                              tr->tr->set(instanceKey, v);
				                                              return tr->tr->watch(instanceKey);
			                                              },
			                                              3, 5000));
			Void _ = wait(watch || delay(10.0));
		} catch (Error& e) {
			TraceEvent(SevWarnAlways, "Unable to write status").detail("Error", e.what());
			Void _ = wait(delay(5.0));
		}
	}
}
