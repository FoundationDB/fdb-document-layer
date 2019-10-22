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

#include "OplogMonitor.actor.h"
#include "ExtUtil.actor.h"
#include "ExtMsg.actor.h"
#include "flow/flow.h"
#include "flow/actorcompiler.h" // This must be the last #include.

using namespace FDB;

static const Key LOGS_ITEM = LiteralStringRef("logs/item");
static const Key LOGS_COUNT = LiteralStringRef("logs/count");

// Add versionstamp with sequence index to the end
StringRef versionStampAtEnd(StringRef const& str, Arena& arena, uint16_t index) {
	int32_t size = str.size();
	uint16_t lIdx = bigEndian16(index);
	uint8_t* s = new (arena) uint8_t[size + 16];
	memcpy(s, str.begin(), size);
	memset(&s[size], 0, 12);
	memcpy(&s[size+12], &size, 4);
	memcpy(&s[size+10], &lIdx, 2);
	return StringRef(s,size + 16);
}

// Add versionstamp with sequence index to the end
Standalone<StringRef> versionStampAtEnd(StringRef const& str, uint16_t index) {
	Standalone<StringRef> r;
	((StringRef &)r) = versionStampAtEnd(str, r.arena(), index);
	return r;
}

// Send log id to logs stream
void sendLogId(PromiseStream<std::string> logs, std::string oId) {
	try {
		logs.send(oId);
	} catch(Error &e) {
		fprintf(stdout, "Log id send error: %s\n", e.what());
	}
}

// Get virtual logs directory storage
ACTOR Future<Reference<DirectorySubspace>> logsDirectory(Reference<DocumentLayer> docLayer) {
	Reference<DirectoryLayer> d = Reference<DirectoryLayer>(new DirectoryLayer());
	Reference<DirectorySubspace> ds = wait(runRYWTransaction(docLayer->database, [d](Reference<DocTransaction> tr) {
		    return d->createOrOpen(tr->tr, {LiteralStringRef("Oplog"), LiteralStringRef("Stream")});
	    },
	    -1, 0));

	return ds;
}

// Read range with ids
ACTOR void readIdsRange(
	Reference<DocumentLayer> docLayer,
	std::string begin,
	std::string end,
	PromiseStream<std::pair<std::string, std::string>> idsWriter) {
	begin = keyAfter(begin).toString();
	end = end + '\xFF';

	state Reference<DocTransaction> tr = NonIsolatedPlan::newTransaction(docLayer->database);
	state Future<FDBStandalone<RangeResultRef>> nextRead = tr->tr->getRange(KeyRangeRef(begin, end));

	loop {
		state FDBStandalone<RangeResultRef> rr = wait(nextRead);

		if (rr.more) {
			begin = keyAfter(rr.back().key).toString();
			nextRead = tr->tr->getRange(KeyRangeRef(begin, end));
		}

		while (!rr.empty()) {
			FDB::KeyValueRef fdbKV = rr.front();				
			FDB::ValueRef vr = fdbKV.value;
			idsWriter.send(std::pair<std::string, std::string>(fdbKV.key.toString(), vr.toString()));
			rr.pop_front(1);
		}

		if (!rr.more) {
			break;
		}
	}

	idsWriter.sendError(end_of_stream());
}

// Writes ids to fdb
ACTOR Future<Void> writeIds(Reference<DocumentLayer> docLayer, std::vector<std::string> ids) {
	state Reference<DirectorySubspace> dir = wait(logsDirectory(docLayer));
	state FDB::Key logsKey = dir->pack(LOGS_ITEM);
	state FDB::Key logsCount = dir->pack(LOGS_COUNT);

	int64_t retryLimit = -1;
	int64_t timeout = 0;

	state Reference<DocTransaction> tr = NonIsolatedPlan::newTransaction(docLayer->database);
	tr->tr->setOption(FDB_TR_OPTION_RETRY_LIMIT, StringRef((uint8_t*)&retryLimit, sizeof(retryLimit)));
	tr->tr->setOption(FDB_TR_OPTION_TIMEOUT, StringRef((uint8_t*)&timeout, sizeof(timeout)));
	tr->tr->setOption(FDB_TR_OPTION_READ_YOUR_WRITES_DISABLE);
	tr->tr->setOption(FDB_TR_OPTION_CAUSAL_READ_RISKY);

	// Clear whole range
	// tr->tr->clear(KeyRangeRef(logsKey.toString(), logsKey.toString() + '\xFF'));
	// wait(tr->tr->commit());
	// tr->tr->reset();

	state Value logsVS;
	state uint16_t idx = 0;
	state uint64_t cnt = ids.size();

	while(!ids.empty()) {
		std::string id = ids.front();
		ids.erase(ids.begin());

		logsVS = versionStampAtEnd(logsKey, idx);

		tr->tr->atomicOp(logsVS, StringRef(id),  FDB_MUTATION_TYPE_SET_VERSIONSTAMPED_KEY);		
		idx++;
	}

	tr->tr->atomicOp(logsKey, logsVS, FDB_MUTATION_TYPE_SET_VERSIONSTAMPED_VALUE);
	tr->tr->atomicOp(logsCount, StringRef((const uint8_t*)&cnt, sizeof(uint64_t)), FDB_MUTATION_TYPE_ADD);
	wait(tr->tr->commit());	

	return Void();
}

// Run logs stream watcher
ACTOR void logStreamWatcherActor(Reference<DocumentLayer> docLayer, PromiseStream<std::pair<std::string, std::string>> keysWriter) {
	state Reference<DirectorySubspace> dir = wait(logsDirectory(docLayer));
	state FDB::Key observable = dir->pack(LOGS_ITEM);
	state Reference<DocTransaction> tr = NonIsolatedPlan::newTransaction(docLayer->database);
	state std::string prev = "";
	state std::string curr = "";

	loop {
		try {			
			state Future<Void> watch = tr->tr->watch(observable);
			state Optional<FDBStandalone<StringRef>> obsStrRef = wait(tr->tr->get(observable, true));

			if (obsStrRef.present()) {
				curr = obsStrRef.get().toString();
			}

			if (curr.compare(prev) == 0) {
				wait(watch || delay(DocLayerConstants::CHNG_WATCH_TIMEOUT));

				tr->tr->reset();
				state Optional<FDBStandalone<StringRef>> newObsStrRef = wait(tr->tr->get(observable, true));

				if (newObsStrRef.present()) {
					curr = newObsStrRef.get().toString();
				}

				if (curr.compare(prev) == 0) {
					continue;
				}
			}

			if (prev.length() == 0) {
				prev = curr;
				continue;
			}

			keysWriter.send(std::pair<std::string, std::string>(prev, curr));
			prev = curr;
		} catch(Error &e) {
			fprintf(stderr, "logStreamWatcherActor error: %s\n", e.what());
			wait(delay(1.0, TaskMaxPriority));
		}
	}
}

// Run logs stream reader
ACTOR void logStreamReaderActor(Reference<DocumentLayer> docLayer, FutureStream<std::string> idsStream) {		
	state std::vector<std::string> ids;	
	state Future<Void> timeout = Never();

	loop {
		try {
			timeout = Never();
			ids.clear();

			try {
				loop choose {
					when(std::string id = waitNext(idsStream)) {						
						ids.push_back(id);

						if (ids.size() == DocLayerConstants::CHNG_WALL_FIRST_CNT) {
							timeout = delay(DocLayerConstants::CHNG_WALL_FIRST_TIMEOUT, TaskMaxPriority);
						}

						if (ids.size() == DocLayerConstants::CHNG_WALL_SECOND_CNT) {
							timeout = delay(DocLayerConstants::CHNG_WALL_SECOND_TIMEOUT, TaskMaxPriority);
						}

						if (ids.size() >= DocLayerConstants::CHNG_WALL_HARD_CNT) {
							throw end_of_stream();
						}
					}
					when(wait(timeout)) {
						// while(idsStream.isReady()) {
						// 	std::string id = waitNext(idsStream);							
						// 	ids.push_back(id);

						// 	if (ids.size() >= DocLayerConstants::CHNG_WALL_HARD_CNT) {
						// 		throw end_of_stream();
						// 	}
						// }

						throw end_of_stream();
					}
				}
			} catch (Error& e) {
				if (e.code() != error_code_end_of_stream)
					throw;
			}

			if (verboseLogging) {
				fprintf(stdout, "[Debug] Collected ids %lu\n", ids.size());
			}

			wait(writeIds(docLayer, ids));
		} catch (Error& e) {
			fprintf(stderr, "Log stream reader error: %s\n", e.what());
		}
	}
}

// Stream out documents from oplog by _ids
ACTOR Future<Void> outStream(Reference<DocumentLayer> docLayer, Deque<std::string> oIds, Reference<ExtChangeStream> output) {
	state PromiseStream<bson::BSONObj> bsonWriter;
	state FutureStream<bson::BSONObj> bsonReader = bsonWriter.getFuture();
	logStreamQuery(docLayer, oIds, bsonWriter);

	state int cnt = 0;
	try {
		loop choose {
			when(bson::BSONObj obj = waitNext(bsonReader)) {
				cnt++;
				output->writeMessage(StringRef((const uint8_t*)obj.objdata(), obj.objsize()));
			}
		}
	} catch (Error &e) {
		if (e.code() != error_code_end_of_stream)
			throw;
	}

	if (verboseLogging) {
		fprintf(stdout, "[Debug] Cnt found: %d\n", cnt);
	}

	return Void();
}

// Scan and streamout virtual log with _ids from oplog
ACTOR void logStreamScanActor(
    Reference<DocumentLayer> docLayer, 
    Reference<ExtChangeStream> output,
    FutureStream<std::pair<std::string, std::string>> keysReader
) {
	loop {
		try {
			try {
				loop choose {
					when(std::pair<std::string, std::string> rangeKeys = waitNext(keysReader)) {
						if (output->countConnections() == 0) {
							continue;
						}

						state PromiseStream<std::pair<std::string, std::string>> idsWriter;
						state FutureStream<std::pair<std::string, std::string>> idsReader = idsWriter.getFuture();
						state Deque<std::string> oIds;

						readIdsRange(docLayer, rangeKeys.first, rangeKeys.second, idsWriter);

						if (verboseLogging) {
							fprintf(stdout, "[Debug] Read range started...\n");
						}

						try {
							loop choose {
								when(std::pair<std::string, std::string> kv = waitNext(idsReader)) {
									oIds.push_back(kv.second);
								}								
							}
						} catch (Error& e) {
							if (e.code() != error_code_end_of_stream)
								throw;
						}

						if (verboseLogging) {
							fprintf(stdout, "[Debug] Read range finished %d.\n", oIds.size());
						}		
						
						if (oIds.size() > 1) {
							wait(outStream(docLayer, oIds, output));
						}
					}
				}
			} catch (Error& e) {
				if (e.code() != error_code_end_of_stream)
					throw;
			}			
		} catch(Error &e) {
			fprintf(stderr, "logStreamScanActor error: %s\n", e.what());
		}
	}	
}

// Get documents from oplog
ACTOR void logStreamQuery(Reference<DocumentLayer> docLayer, Deque<std::string> oIds, PromiseStream<bson::BSONObj> output) {
	state Namespace ns = Namespace(DocLayerConstants::OPLOG_DB, DocLayerConstants::OPLOG_COL);
	state Reference<DocTransaction> dtr = NonIsolatedPlan::newTransaction(docLayer->database);
	state Reference<UnboundCollectionContext> cx = wait(docLayer->mm->getUnboundCollectionContext(dtr, ns, true));
	state Reference<PlanCheckpoint> checkpoint(new PlanCheckpoint);

	std::vector<bson::OID> ids;
	while (!oIds.empty()) {
		ids.push_back(bson::OID(oIds.front()));
		oIds.pop_front();
	}
	std::sort(ids.begin(), ids.end(),  std::less<bson::OID>());	
	
	DataValue begin = DataValue(bson::OID(ids.front()));
	DataValue end = DataValue(bson::OID(ids.back()));

	state Reference<Plan> plan = ref(new PrimaryKeyLookupPlan(cx, begin, end));
	plan = Reference<Plan>(new NonIsolatedPlan(plan, true, cx, docLayer->database, docLayer->mm));
	state FlowLock* flowControlLock = checkpoint->getDocumentFinishedLock();
	state FutureStream<Reference<ScanReturnedContext>> docs = plan->execute(checkpoint.getPtr(), dtr);

	state Deque<Future<DataValue>> bufferedObjects;

	try {
		loop {
			choose {
				when(state Reference<ScanReturnedContext> doc = waitNext(docs)) {
					bufferedObjects.push_back(doc->toDataValue());
				}
				when(DataValue dv = wait(bufferedObjects.empty() ? Never() : bufferedObjects.front())) {
					bson::BSONObj obj = dv.getPackedObject().getOwned();
					output.send(obj);
					bufferedObjects.pop_front();
					flowControlLock->release();
				}
			}
		}
	} catch (Error& e) {		
		checkpoint->stop();
		if (e.code() != error_code_end_of_stream) {
			throw;
		}
	}

	while (!bufferedObjects.empty()) {
		DataValue dv = wait(bufferedObjects.front());
		bson::BSONObj obj = dv.getPackedObject().getOwned();
		output.send(obj);
		bufferedObjects.pop_front();
		flowControlLock->release();
	}

	output.sendError(end_of_stream());
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

// Run oplog size monitor
void oplogMonitor(Reference<DocumentLayer> docLayer, double logsDeletionOffset) {
	deleteExpiredLogs(docLayer, timer() * 1000 - logsDeletionOffset);
}