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

static const Key LOGS_OBJS = LiteralStringRef("logs/object");
static const Key LOGS_COUNT = LiteralStringRef("logs/count");
typedef std::function<bool(std::pair<bson::OID, bson::BSONObj>, std::pair<bson::OID, bson::BSONObj>)> ObjsCmp;
static Reference<DirectorySubspace> logsDir;


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
void sendLogId(PromiseStream<std::map<std::string, bson::BSONObj>> logsWriter, std::map<std::string, bson::BSONObj> objs) {
	try {
		logsWriter.send(objs);
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
ACTOR void readRange(
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
ACTOR Future<Void> writeIds(Reference<DocumentLayer> docLayer, std::set<std::pair<bson::OID, bson::BSONObj>, ObjsCmp> objs) {
	state Reference<DirectorySubspace> dir = logsDir;
	state FDB::Key logsObjects = dir->pack(LOGS_OBJS);
	state FDB::Key logsCount = dir->pack(LOGS_COUNT);

	int64_t retryLimit = -1;
	int64_t timeout = 0;

	state Reference<DocTransaction> tr = NonIsolatedPlan::newTransaction(docLayer->database);
	tr->tr->setOption(FDB_TR_OPTION_RETRY_LIMIT, StringRef((uint8_t*)&retryLimit, sizeof(retryLimit)));
	tr->tr->setOption(FDB_TR_OPTION_TIMEOUT, StringRef((uint8_t*)&timeout, sizeof(timeout)));
	tr->tr->setOption(FDB_TR_OPTION_READ_YOUR_WRITES_DISABLE);
	tr->tr->setOption(FDB_TR_OPTION_CAUSAL_READ_RISKY);

	state Value logsObjVS;
	state uint16_t idx = 0;
	state uint64_t cnt = objs.size();

	for (std::pair<bson::OID, bson::BSONObj> obj : objs) {		
		logsObjVS = versionStampAtEnd(logsObjects, idx);
		tr->tr->atomicOp(
			logsObjVS, 
			StringRef((const uint8_t*)obj.second.objdata(), obj.second.objsize()), 
			FDB_MUTATION_TYPE_SET_VERSIONSTAMPED_KEY
		);
		idx++;
	}
	
	tr->tr->atomicOp(logsObjects, logsObjVS, FDB_MUTATION_TYPE_SET_VERSIONSTAMPED_VALUE);
	tr->tr->atomicOp(logsCount, StringRef((const uint8_t*)&cnt, sizeof(uint64_t)), FDB_MUTATION_TYPE_ADD);
	wait(tr->tr->commit());	

	return Void();
}

// Run logs stream watcher
ACTOR void logStreamWatcherActor(Reference<DocumentLayer> docLayer, PromiseStream<std::pair<std::string, std::string>> keysWriter) {
	state Reference<DirectorySubspace> dir = logsDir;
	state FDB::Key observable = dir->pack(LOGS_OBJS);
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
ACTOR void logStreamReaderActor(Reference<DocumentLayer> docLayer, FutureStream<std::map<std::string, bson::BSONObj>> objsReader) {
	state std::map<bson::OID, bson::BSONObj> out;	
	state Future<Void> timeout = Never();
	state ObjsCmp cmpFunc = [](std::pair<bson::OID, bson::BSONObj> el1, std::pair<bson::OID, bson::BSONObj> el2)
			{
				return el1.first < el2.first;
			};

	loop {
		try {
			timeout = Never();
			out.clear();

			try {
				loop choose {
					when(std::map<std::string, bson::BSONObj> objs = waitNext(objsReader)) {												
						for(std::pair<std::string, bson::BSONObj> el : objs) {
							out.insert(std::make_pair(bson::OID(el.first), el.second));
						}

						if (out.size() == DocLayerConstants::CHNG_WALL_FIRST_CNT) {
							timeout = delay(DocLayerConstants::CHNG_WALL_FIRST_TIMEOUT, TaskMaxPriority);
						}

						if (out.size() == DocLayerConstants::CHNG_WALL_SECOND_CNT) {
							timeout = delay(DocLayerConstants::CHNG_WALL_SECOND_TIMEOUT, TaskMaxPriority);
						}

						if (out.size() >= DocLayerConstants::CHNG_WALL_HARD_CNT) {
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

			if (verboseLogging) {
				fprintf(stdout, "[Debug] Collected ids %lu\n", out.size());
			}

			std::set<std::pair<bson::OID, bson::BSONObj>, ObjsCmp> sortedObjs(out.begin(), out.end(), cmpFunc);		

			wait(writeIds(docLayer, sortedObjs));
		} catch (Error& e) {			
			fprintf(stderr, "Log stream reader error: %s\n", e.what());

			if (e.code() == error_code_broken_promise) 
				throw;
		}
	}
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

						state int finished = 0;						
						state PromiseStream<std::pair<std::string, std::string>> rangeWriter;
						state FutureStream<std::pair<std::string, std::string>> rangeReader = rangeWriter.getFuture();						
						state Deque<std::string> buffered;

						readRange(docLayer, rangeKeys.first, rangeKeys.second, rangeWriter);

						if (verboseLogging) {
							fprintf(stdout, "[Debug] Read range started...\n");
						}

						try {
							loop choose {
								when(std::pair<std::string, std::string> kv = waitNext(rangeReader)) {									
									buffered.push_back(kv.second);									
								}
								when(std::string obj = wait(buffered.empty() ? Never() : Future<std::string>(buffered.front()))) {
									output->writeMessage(StringRef(obj));
									buffered.pop_front();
									finished++;
								}
							}
						} catch (Error& e) {
							if (e.code() != error_code_end_of_stream)
								throw;
						}				

						while (!buffered.empty()) {
							output->writeMessage(StringRef(buffered.front()));
							buffered.pop_front();
							finished++;
						}

						if (verboseLogging) {
							fprintf(stdout, "[Debug] Read range finished %d.\n", finished);
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
			int64_t c = wait(executeUntilCompletionTransactionally(plan, dtr));

			if (verboseLogging) {
				fprintf(stdout, "Cleared local.oplog.rs collection %ld\n", c);
			}
		} catch (Error& e) {
			fprintf(stderr, "Unable to delete oplog by ts %f\n", ts);
		}
}

ACTOR void clearVirtualRanges(Reference<DocumentLayer> docLayer) {
	state Reference<DirectorySubspace> dir = logsDir;
	state FDB::Key logsObjects = dir->pack(LOGS_OBJS);
	state FDB::Key logsCount = dir->pack(LOGS_COUNT);
	state uint64_t cntClean = DocLayerConstants::CHNG_VIRT_CLEAN;
	state uint64_t cntLimiter = DocLayerConstants::CHNG_VIRT_SIZE;

	state Reference<DocTransaction> tr = NonIsolatedPlan::newTransaction(docLayer->database);
	tr->tr->addReadConflictKey(logsCount);	
	state Optional<FDBStandalone<StringRef>> curCntRef = wait(tr->tr->get(logsCount));

	if (curCntRef.present()) {
		uint64_t curr = *(uint64_t*)curCntRef.get().begin();
		if (verboseLogging) {
			fprintf(stdout, "Current virtual log size: %lu\n", curr);
		}

		if (curr > cntLimiter) {
			state FDB::Key begin = logsObjects;
			state FDB::Key end = KeyRef(logsObjects.toString() + '\xFF');
			state FDB::Key limitedEnd;
			state uint64_t removed = 0;

			state Future<FDBStandalone<RangeResultRef>> nextRead = tr->tr->getRange(KeyRangeRef(begin, end), cntClean);

			try {
				loop {
					state FDBStandalone<RangeResultRef> rr = wait(nextRead);
					if (!rr.empty()) {
						limitedEnd = rr.back().key;	
					}

					removed += rr.size();
					cntClean -= rr.size();

					if (!rr.more || cntClean <= 0) {
						break;
					}
									
					begin = keyAfter(rr.back().key).toString();
					nextRead = tr->tr->getRange(KeyRangeRef(begin, end), cntClean);
				}
			} catch (Error &e) {
				fprintf(stdout, "clearVirtualRanges error %s\n", e.what());
			}

			if (limitedEnd.size() > 0) {				
				uint64_t dec = -removed;

				tr->tr->clear(KeyRangeRef(logsObjects.toString(), limitedEnd.toString() + '\xFF'));
				tr->tr->atomicOp(logsCount, StringRef((const uint8_t*)&dec, sizeof(uint64_t)), FDB_MUTATION_TYPE_ADD);				
				wait(tr->tr->commit());	

				if (verboseLogging) {
					fprintf(stdout, "Removed from virtual log: %lu\n", removed);
					fprintf(
						stdout, 
						"Cleared range %s - %s\n", 
						logsObjects.printable().c_str(), (limitedEnd.printable() + "\\xFF").c_str()
					);
				}
			}
		}
	}	
}

// Initialize dirs for stream
ACTOR Future<Void> initVirtualDirs(Reference<DocumentLayer> docLayer) {
	Reference<DirectorySubspace> ds = wait(logsDirectory(docLayer));
	logsDir = ds;

	return Void();
}

// Run oplog size monitor
void oplogMonitor(Reference<DocumentLayer> docLayer, double logsDeletionOffset) {
	deleteExpiredLogs(docLayer, timer() * 1000 - logsDeletionOffset);
	clearVirtualRanges(docLayer);
}