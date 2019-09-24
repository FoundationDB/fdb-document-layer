/*
 * OplogMonitor.actor.cpp
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

#include "OplogMonitor.h"
#include "ExtUtil.actor.h"
#include "ExtMsg.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

using namespace FDB;

// Send timestamp to stream
void sendTimestamp(PromiseStream<double> times, double ts) {
	try {
		times.send(ts);
	} catch(Error &e) {
		fprintf(stdout, "Timestamp send error: %s\n", e.what());
	}
}

// Get timestamp directory storage
ACTOR Future<Reference<DirectorySubspace>> watcherGetTSDirectory(Reference<DocumentLayer> docLayer) {
	Reference<DirectoryLayer> d = Reference<DirectoryLayer>(new DirectoryLayer());
	Reference<DirectorySubspace> ds = wait(runRYWTransaction(docLayer->database, [d](Reference<DocTransaction> tr) {
		    return d->createOrOpen(tr->tr, {LiteralStringRef("Oplog"), LiteralStringRef("Updates")});
	    },
	    -1, 0));

	return ds;
}

// Get timestamp subspace key
FDB::Key watcherGetTSKey(Reference<DirectorySubspace> dir) {
	return dir->pack(LiteralStringRef("timestamp"), true);
}

// Run timestamp stream watcher
ACTOR void watcherTimestampUpdateActor(Reference<DocumentLayer> docLayer, FutureStream<double> times) {
	state Reference<DirectorySubspace> timestampDir = wait(watcherGetTSDirectory(docLayer));
	state FDB::Key tsKey = watcherGetTSKey(timestampDir);
	state double ts = 0.0;

	loop {
		try {
			try {
				state Future<Void> timeout = Never();
				state int cnt = 0;

				loop choose {
					when(double tmpTs = waitNext(times)) {																
						ts = tmpTs;
						cnt++;

						if (cnt == 1) {
							timeout = delay(0.1, TaskMaxPriority);
						}

						if (cnt == 5) {
							timeout = delay(0.3, TaskMaxPriority);
						}

						if (cnt >= 500) {
							throw end_of_stream();
						}
					}										
					when(wait(timeout)) {
						throw end_of_stream();
					}
				}
			} catch (Error& e) {
				if (e.code() != error_code_end_of_stream)
					throw;
			}

			state Reference<DocTransaction> updTr = NonIsolatedPlan::newTransaction(docLayer->database);
			updTr->tr->atomicOp(tsKey, StringRef((const uint8_t*)&ts, sizeof(double)), FDB_MUTATION_TYPE_MAX);
			wait(updTr->tr->commit());
		} catch(Error &e) {
			fprintf(stderr, "Watcher ts update error: %s\n", e.what());
		}
	}
}

// Scan updates from oplog and send to change stream
ACTOR void watcherScanUpdates(FutureStream<double> times, Reference<DocumentLayer> docLayer, Reference<ExtChangeStream> changeStream) {
	state double lastTs = -1.0;
	state Namespace ns = Namespace(DocLayerConstants::OPLOG_DB, DocLayerConstants::OPLOG_COL);

	loop {
		try {
			state double newTs = waitNext(times);
			if (lastTs == -1.0) {
				lastTs = newTs;
				continue;
			}
			if (lastTs == newTs) {
				continue;
			}

			if (changeStream->countConnections() > 0) {
				state Reference<DocTransaction> dtr = NonIsolatedPlan::newTransaction(docLayer->database);
				state Reference<UnboundCollectionContext> cx = wait(docLayer->mm->getUnboundCollectionContext(dtr, ns, true));				
				bson::BSONObj query = BSON("ts" << BSON("$gt" << lastTs << "$lte" << newTs));
		
				state Reference<PlanCheckpoint> checkpoint(new PlanCheckpoint);
				state Reference<Plan> plan = planQuery(cx, query);
				plan = Reference<Plan>(new NonIsolatedPlan(plan, true, cx, docLayer->database, docLayer->mm));
				state FutureStream<Reference<ScanReturnedContext>> docs = plan->execute(checkpoint.getPtr(), dtr);
				state FlowLock* flowControlLock = checkpoint->getDocumentFinishedLock();

				try {
					loop {
						choose {
							when(state Reference<ScanReturnedContext> doc = waitNext(docs)) {
								DataValue oDv = wait(doc->toDataValue());
								bson::BSONObj obj = oDv.getPackedObject().getOwned();
								flowControlLock->release();
								changeStream->writeMessage(StringRef((const uint8_t*)obj.objdata(), obj.objsize()));
							}
						}
					}
				} catch (Error& e) {
					checkpoint->stop();
					if (e.code() != error_code_end_of_stream) {						
						throw;
					}
				}
			}

			lastTs = newTs;
		} catch(Error &e) {
			fprintf(stderr, "Watcher scan error: %s\n", e.what());
		}
	}
}

// Run timestamp watcher
ACTOR void watcherTimestampWatchingActor(PromiseStream<double> times, Reference<DocumentLayer> docLayer) {
	state Reference<DirectorySubspace> timestampDir = wait(watcherGetTSDirectory(docLayer));
	state FDB::Key tsKey = watcherGetTSKey(timestampDir);
	state double ts = 0.0;
	state double prevTs = -1.0;

	loop {
		try {
			state Reference<DocTransaction> dtr = NonIsolatedPlan::newTransaction(docLayer->database);
			state Future<Void> watch = dtr->tr->watch(tsKey);
			state Future<Optional<FDBStandalone<StringRef>>> futureTs = dtr->tr->get(tsKey);			
			wait(dtr->tr->commit());
			dtr->tr->reset();

			Optional<FDBStandalone<StringRef>> tsBefore = wait(futureTs);
			if (tsBefore.present()) {
				ts = *(double*)tsBefore.get().begin();
			}

			if (ts == prevTs) {
				wait(watch || delay(5.0));
				futureTs = dtr->tr->get(tsKey);
				wait(dtr->tr->commit());

				Optional<FDBStandalone<StringRef>> tsAfter = wait(futureTs);
				if (tsAfter.present()) {
					ts = *(double*)tsAfter.get().begin();
				}

				if (ts == prevTs) {
					continue;
				}
			}
	
			prevTs = ts;
			sendTimestamp(times, ts);
		} catch(Error &e) {
			fprintf(stderr, "Watcher watching error: %s\n", e.what());
			wait(delay(1.0, TaskMaxPriority));
		}
	}
}

// Delete expired oplogs by timestamp
ACTOR void deleteExpiredLogs(Reference<DocumentLayer> docLayer, double ts) {
	state Reference<DocTransaction> dtr = NonIsolatedPlan::newTransaction(docLayer->database);
	state Reference<UnboundCollectionContext> cx;
	state Namespace ns = Namespace(DocLayerConstants::OPLOG_DB, DocLayerConstants::OPLOG_COL);

	try {
			Reference<UnboundCollectionContext> _cx = wait(docLayer->mm->getUnboundCollectionContext(dtr, ns, false, false));
			cx = _cx;
		} catch (Error& e) {
			if (e.code() == error_code_collection_not_found)
				return;
			throw e;
		}
	
	try {		
			bson::BSONObj query = BSON("ts" << BSON("$lte" << ts));
			Reference<Plan> plan = planQuery(cx, query);
			plan = deletePlan(plan, cx, std::numeric_limits<int64_t>::max());
			plan = Reference<Plan>(new NonIsolatedPlan(plan, false, cx, docLayer->database, docLayer->mm));
			int64_t _ = wait(executeUntilCompletionTransactionally(plan, dtr));
		} catch (Error& e) {
			fprintf(stderr, "Unable to delete oplog by ts %f\n", ts);
		}
}

// sendTimestamp helper
void oplogSendTimestamp(PromiseStream<double> times, double ts) {
    sendTimestamp(times, ts);
}

// watcherScanUpdates helper
void oplogRunUpdateScanner(FutureStream<double> times, Reference<DocumentLayer> docLayer, Reference<ExtChangeStream> changeStream) {
    watcherScanUpdates(times, docLayer, changeStream);
}

// watcherTimestampUpdateActor helper
void oplogRunStreamWatcher(Reference<DocumentLayer> docLayer, FutureStream<double> times) {
    watcherTimestampUpdateActor(docLayer, times);
}

// watcherTimestampWatchingActor helper
void oplogRunTimestampWatcher(PromiseStream<double> times, Reference<DocumentLayer> docLayer) {
    watcherTimestampWatchingActor(times, docLayer);
}

// Run oplog size monitor
void oplogMonitor(Reference<DocumentLayer> docLayer, double logsDeletionOffset) {
	deleteExpiredLogs(docLayer, timer() * 1000 - logsDeletionOffset);
}