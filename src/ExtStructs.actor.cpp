/*
 * ExtStructs.actor.cpp
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

#include "ExtStructs.h"
#include "ExtUtil.actor.h"
#include "ExtMsg.actor.h"
#include "QLPlan.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

ACTOR Future<Reference<DirectorySubspace>> watcherGetTSDirectory(Reference<DocumentLayer> docLayer) {
	Reference<DirectoryLayer> d = Reference<DirectoryLayer>(new DirectoryLayer());
	Reference<DirectorySubspace> ds = wait(runRYWTransaction(docLayer->database, [d](Reference<DocTransaction> tr) {
		    return d->createOrOpen(tr->tr, {LiteralStringRef("Oplog"), LiteralStringRef("Updates")});
	    },
	    -1, 0));

	return ds;
}

FDB::Key watcherGetTSKey(Reference<DirectorySubspace> dir) {
	return dir->pack(LiteralStringRef("timestamp"), true);
}

void sendTimestamp(PromiseStream<double> times, double ts) {
	try {
		times.send(ts);
	} catch(Error &e) {
		fprintf(stdout, "Timestamp send error: %s\n", e.what());
	}
}

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
				bson::BSONObj selectors = BSON("$gt" << lastTs << "$lte" << newTs);
				bson::BSONObj query = BSON("ts" << selectors);
		
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

ACTOR void watcherTimestampWatchingActor(
	PromiseStream<double> times, 
	Reference<DocumentLayer> docLayer) {
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

// Create connection change stream
FutureStream<Standalone<StringRef>> ExtChangeStream::newConnection(int64_t connectionId) {
	PromiseStream<Standalone<StringRef>> changeStream;
	connections[connectionId] = changeStream;
	return changeStream.getFuture();
}

// Delete connection change stream
void ExtChangeStream::deleteConnection(int64_t connectionId) {
	connections[connectionId].sendError(end_of_stream());
	connections.erase(connectionId);
}

// Write message to change stream
void ExtChangeStream::writeMessage(Standalone<StringRef> msg) {
	for(auto &c : connections) {
		c.second.send(msg);
	}
}

// Delete all connections
void ExtChangeStream::clear() {
	connections.clear();
}

// Get connections count
int ExtChangeStream::countConnections() {
	return connections.size();
}

// Update timestamp key
void ExtChangeWatcher::update(double timestamp) {
	sendTimestamp(tsStreamWriter, timestamp);
}

// Watching for updates
void ExtChangeWatcher::watch() {
	watcherScanUpdates(tsScanFuture, docLayer, changeStream);	
	watcherTimestampUpdateActor(docLayer, tsStreamReader);
	watcherTimestampWatchingActor(tsScanPromise, docLayer);	
}

Reference<DocTransaction> ExtConnection::getOperationTransaction() {
	return NonIsolatedPlan::newTransaction(docLayer->database);
}

Reference<Plan> ExtConnection::wrapOperationPlanOplog(Reference<Plan> plan,
												   	  Reference<IOplogInserter> oplogInserter,
                                                      Reference<UnboundCollectionContext> cx) {
   return Reference<Plan>(new NonIsolatedPlan(plan, false, cx, docLayer->database, oplogInserter, mm));
}

Reference<Plan> ExtConnection::wrapOperationPlan(Reference<Plan> plan,
                                                 bool isReadOnly,
                                                 Reference<UnboundCollectionContext> cx) {
	return Reference<Plan>(new NonIsolatedPlan(plan, isReadOnly, cx, docLayer->database, mm));
}

Reference<Plan> ExtConnection::isolatedWrapOperationPlan(Reference<Plan> plan) {
	return isolatedWrapOperationPlan(plan, options.timeoutMillies, options.retryLimit);
}

Reference<Plan> ExtConnection::isolatedWrapOperationPlan(Reference<Plan> plan, int64_t timeout, int64_t retryLimit) {
	return Reference<Plan>(new RetryPlan(plan, timeout, retryLimit, docLayer->database));
}

ACTOR Future<Void> housekeeping_impl(Reference<ExtConnection> ec) {
	loop {
		wait(delay(DOCLAYER_KNOBS->CURSOR_EXPIRY));
		try {
			Cursor::prune(ec->cursors);
		} catch (Error& e) {
			TraceEvent(SevError, "BD_Cursor_housekeeping").error(e);
		}
	}
}

void ExtConnection::startHousekeeping() {
	housekeeping = housekeeping_impl(Reference<ExtConnection>::addRef(this));
}

ACTOR Future<WriteResult> lastErrorOrLastResult(Future<WriteResult> previous,
                                                Future<WriteResult> next,
                                                FlowLock* lock,
                                                int releasePermits) {
	try {
		state WriteResult next_wr = wait(next); // might throw
		wait(success(previous)); // might throw
		lock->release(releasePermits);
		return next_wr;
	} catch (...) {
		lock->release(releasePermits);
		throw;
	}
}

/**
 * beforeWrite's future is set when the current write may *begin* while satisfying all ordering and pipelining
 * constraints The return value has no impact on when *subsequent* writes may be consumed from the network and/or
 * started; that is controlled by afterWrite().  But this code stashes some information which the next call to
 * afterWrite() uses.
 */
Future<Void> ExtConnection::beforeWrite(int desiredPermits) {
	if (options.pipelineCompatMode)
		return ready(lastWrite);
	currentWriteLocked =
	    lock->take(TaskDefaultYield, std::min(desiredPermits, DOCLAYER_KNOBS->CONNECTION_MAX_PIPELINE_DEPTH / 2));
	return currentWriteLocked;
}

/**
 * afterWrite()'s return future is set when the next write may be consumed from the network (and beforeWrite()
 * called).
 */
Future<Void> ExtConnection::afterWrite(Future<WriteResult> writeResult, int releasePermits) {
	if (options.pipelineCompatMode) {
		lastWrite = writeResult;
		return ready(lastWrite);
	} else {
		lastWrite = lastErrorOrLastResult(lastWrite, writeResult, lock.getPtr(),
		                                  std::min(releasePermits, DOCLAYER_KNOBS->CONNECTION_MAX_PIPELINE_DEPTH / 2));
		return currentWriteLocked;
	}
}
