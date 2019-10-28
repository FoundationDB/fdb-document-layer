/*
 * DocLayer.actor.cpp
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

#include "flow/ActorCollection.h"
#include "flow/DeterministicRandom.h"
#include "flow/SignalSafeUnwind.h"
#include "flow/SimpleOpt.h"
#include "flow/UnitTest.h"
#include "flow/flow.h"
#include "flow/network.h"

#ifndef TLS_DISABLED
#include "fdbrpc/TLSConnection.h"
#endif

#include <boost/bind.hpp>
#include <boost/function.hpp>

#include "BufferedConnection.h"
#include "ConsoleMetric.h"
#include "Cursor.h"
#include "DocLayer.h"
#include "ExtMsg.actor.h"
#include "IMetric.h"
#include "StatusService.actor.h"
#include "OplogMonitor.actor.h"

#include "flow/SystemMonitor.h"

#include <fstream>

#ifndef WIN32
#include "gitVersion.h"
#endif

#ifdef __linux__
#include <sys/prctl.h>
#endif
#ifdef __APPLE__
#include <pthread.h>
#endif

#include "flow/actorcompiler.h" // This must be the last #include.

#define STORAGE_VERSION 4

enum {
	OPT_CONNFILE,
	OPT_HELP,
	OPT_PROXYPORTS,
	OPT_LISTEN,
	OPT_LOGFOLDER,
	OPT_LOGGROUP,
	OPT_ROLLSIZE,
	OPT_MAXLOGSSIZE,
	OPT_VERBOSE,
	OPT_SUPERVERBOSE,
	OPT_RETRYLIMIT,
	OPT_TIMEOUT,
	OPT_PIPELINECOMPATMODE,
	OPT_SLOWQUERYLOG,
	OPT_DIRECTORY,
	OPT_VERSION,
	OPT_UNIT_TEST,
	OPT_KNOB,
	OPT_CLIENT_KNOB,
	OPT_CRASHONERROR,
	OPT_BUGGIFY,
	OPT_BUGGIFY_INTENSITY,
	OPT_METRIC_PLUGIN,
	OPT_METRIC_CONFIG,
	OPT_FDB_DC_ID,
	OPT_NSNOTIFYLISTEN
};
CSimpleOpt::SOption g_rgOptions[] = {{OPT_CONNFILE, "-C", SO_REQ_SEP},
                                     {OPT_CONNFILE, "--cluster_file", SO_REQ_SEP},
                                     {OPT_HELP, "-h", SO_NONE},
                                     {OPT_VERSION, "-v", SO_NONE},
                                     {OPT_VERSION, "--version", SO_NONE},
                                     {OPT_PROXYPORTS, "-p", SO_MULTI},
                                     {OPT_PROXYPORTS, "--proxy-ports", SO_MULTI},
                                     {OPT_LISTEN, "-l", SO_REQ_SEP},
                                     {OPT_LISTEN, "--listen_address", SO_REQ_SEP},
                                     {OPT_LOGFOLDER, "-L", SO_REQ_SEP},
                                     {OPT_LOGFOLDER, "--logdir", SO_REQ_SEP},
                                     {OPT_LOGGROUP, "--loggroup", SO_REQ_SEP},
                                     {OPT_ROLLSIZE, "-Rs", SO_REQ_SEP},
                                     {OPT_ROLLSIZE, "--logsize", SO_REQ_SEP},
                                     {OPT_MAXLOGSSIZE, "--maxlogssize", SO_REQ_SEP},
                                     {OPT_VERBOSE, "-V", SO_NONE},
                                     {OPT_VERBOSE, "--verbose", SO_NONE},
                                     {OPT_SUPERVERBOSE, "-VV", SO_NONE},
                                     {OPT_PIPELINECOMPATMODE, "--pipeline", SO_REQ_SEP},
                                     {OPT_RETRYLIMIT, "--implicit-transaction-max-retries", SO_REQ_SEP},
                                     {OPT_TIMEOUT, "--implicit-transaction-timeout", SO_REQ_SEP},
                                     {OPT_SLOWQUERYLOG, "--slow-query-log", SO_REQ_SEP},
                                     {OPT_DIRECTORY, "-d", SO_REQ_SEP},
                                     {OPT_DIRECTORY, "--root-directory", SO_REQ_SEP},
                                     {OPT_UNIT_TEST, "--run-unit-tests", SO_REQ_SEP},
                                     {OPT_KNOB, "--knob_", SO_REQ_SEP},
                                     {OPT_CLIENT_KNOB, "--client_knob_", SO_REQ_SEP},
                                     {OPT_CRASHONERROR, "--crash", SO_NONE},
                                     {OPT_BUGGIFY, "--buggify", SO_NONE},
                                     {OPT_BUGGIFY_INTENSITY, "--buggify_intensity", SO_REQ_SEP},
                                     {OPT_METRIC_PLUGIN, "--metric_plugin", SO_OPT},
                                     {OPT_METRIC_CONFIG, "--metric_plugin_config", SO_OPT},
                                     {OPT_FDB_DC_ID, "--fdb_datacenter_id", SO_OPT},
									 {OPT_NSNOTIFYLISTEN, "-nl", SO_REQ_SEP},
									 {OPT_NSNOTIFYLISTEN, "--ns_listen", SO_REQ_SEP},
#ifndef TLS_DISABLED
                                     TLS_OPTION_FLAGS
#endif
                                         SO_END_OF_OPTIONS};

using namespace FDB;

bool verboseLogging = false;
bool verboseConsoleOutput = false;
bool slowQueryLogging = true;
IMetricReporter* DocumentLayer::metricReporter;

extern const char* getGitVersion();
extern const char* getFlowGitVersion();
extern bool g_crashOnError;

Future<Void> processRequest(Reference<ExtConnection> ec,
                            ExtMsgHeader* header,
                            const uint8_t* body,
                            Promise<Void> finished) {
	try {
		Reference<ExtMsg> msg = ExtMsg::create(header, body, finished);
		if (verboseLogging)
			TraceEvent("BD_processRequest").detail("Message", msg->toString()).detail("connId", ec->connectionId);
		if (verboseConsoleOutput)
			fprintf(stderr, "C -> S: %s\n\n", msg->toString().c_str());
		return msg->run(ec);
	} catch (Error& e) {
		TraceEvent(SevError, "UnhandledRequestFailure").detail("opcode", header->opCode).error(e);
		return Void();
	}
}

ACTOR Future<Void> popDisposedMessages(Reference<BufferedConnection> bc,
                                       FutureStream<std::pair<int, Future<Void>>> msg_size_inuse) {
	loop {
		state std::pair<int, Future<Void>> s = waitNext(msg_size_inuse);
		try {
			wait(s.second);
		} catch (...) {
		}
		bc->pop(s.first);
	}
}

// Splitting the delay into smaller delay(). For each delay(), DelayedTask is created and added
// to the queue. This task stays in queue until delay is expired, even if all the futures are
// cancelled. In the connection churn it is possible we are leaving too many long delays
// in the queues.
ACTOR Future<Void> delayCursorExpiry() {
	state int remaining = DOCLAYER_KNOBS->CURSOR_EXPIRY;
	while (remaining > 0) {
		wait(delay(1.0));
		remaining--;
	}
	return Void();
}

ACTOR Future<Void> housekeeping(Reference<ExtConnection> ec) {
	try {
		loop {
			wait(delayCursorExpiry());
			Cursor::prune(ec->cursors, false);
		}
	} catch (Error& e) {
		// This is the only actor responsible for all the cursors created
		// through this connection. Prune all the cursors before cancelling
		// this actor.
		if (e.code() == error_code_actor_cancelled)
			Cursor::prune(ec->cursors, true);
		throw;
	}
}

ACTOR Future<int32_t> processMessage(Reference<ExtConnection> ec, Promise<Void> finished) {
	wait(ec->bc->onBytesAvailable(sizeof(ExtMsgHeader)));
	auto headerBytes = ec->bc->peekExact(sizeof(ExtMsgHeader));

	state ExtMsgHeader* header = (ExtMsgHeader*)headerBytes.begin();

	wait(ec->bc->onBytesAvailable(header->messageLength));
	auto messageBytes = ec->bc->peekExact(header->messageLength);

	DocumentLayer::metricReporter->captureHistogram(DocLayerConstants::MT_HIST_MESSAGE_SZ, header->messageLength);

	/* We don't use hdr in this call because the second peek may
	   have triggered a copy that the first did not, but it's nice
	   for everything at and below processRequest to assume that
	   body - header == sizeof(ExtMsgHeader) */
	ec->updateMaxReceivedRequestID(header->requestID);
	wait(
	    processRequest(ec, (ExtMsgHeader*)messageBytes.begin(), messageBytes.begin() + sizeof(ExtMsgHeader), finished));

	ec->bc->advance(header->messageLength);

	return header->messageLength;
}

ACTOR Future<Void> extServerConnection(Reference<DocumentLayer> docLayer,
                                       Reference<BufferedConnection> bc,
                                       int64_t connectionId,
									   Reference<ExtChangeWatcher> watcher) {
	if (verboseLogging)
		TraceEvent("BD_serverNewConnection").detail("connId", connectionId);

	state Reference<ExtConnection> ec = Reference<ExtConnection>(new ExtConnection(docLayer, bc, connectionId, watcher));
	state PromiseStream<std::pair<int, Future<Void>>> msg_size_inuse;
	state Future<Void> onError = ec->bc->onClosed() || popDisposedMessages(bc, msg_size_inuse.getFuture());
	state Future<Void> connHousekeeping = housekeeping(ec);

	DocumentLayer::metricReporter->captureGauge(DocLayerConstants::MT_GUAGE_ACTIVE_CONNECTIONS,
	                                            ++docLayer->nrConnections);
	DocumentLayer::metricReporter->captureMeter(DocLayerConstants::MT_RATE_NEW_CONNECTIONS, 1);

	try {
		try {
			loop {
				// Will be broken (or set or whatever) only when the memory we are passing to processRequest is no
				// longer needed and can be popped
				state Promise<Void> finished;
				choose {
					when(wait(onError)) {
						if (verboseLogging)
							TraceEvent("BD_serverClosedConnection").detail("connId", connectionId);
						throw connection_failed();
					}
					when(int32_t messageLength = wait(processMessage(ec, finished))) {
						msg_size_inuse.send(std::make_pair(messageLength, finished.getFuture()));
					}
				}
			}
		} catch (Error& e) {
			if (e.code() != error_code_connection_failed)
				TraceEvent(SevError, "BD_unexpectedConnFailure").detail("connId", connectionId).error(e);

			DocumentLayer::metricReporter->captureGauge(DocLayerConstants::MT_GUAGE_ACTIVE_CONNECTIONS,
			                                            --docLayer->nrConnections);
			return Void();
		}
	} catch (...) {
		TraceEvent(SevError, "BD_unknownConnFailure").detail("connId", connectionId).error(unknown_error());
		DocumentLayer::metricReporter->captureGauge(DocLayerConstants::MT_GUAGE_ACTIVE_CONNECTIONS,
		                                            --docLayer->nrConnections);
		return Void();
	}
}

ACTOR void extServer(Reference<DocumentLayer> docLayer, NetworkAddress addr, Reference<ExtChangeWatcher> watcher) {
	state ActorCollection connections(false);
	state int64_t nextConnectionId = 1;
	try {
		state Reference<IListener> listener = INetworkConnections::net()->listen(addr);

		TraceEvent("BD_server").detail("version", FDB_DOC_VT_PACKAGE_NAME).detail("address", addr.toString());
		fprintf(stdout, "FdbDocServer (%s): listening on %s\n", FDB_DOC_VT_PACKAGE_NAME, addr.toString().c_str());

		loop choose {
			when(Reference<IConnection> conn = wait(listener->accept())) {
				Reference<BufferedConnection> bc(new BufferedConnection(conn));				
				connections.add(extServerConnection(docLayer, bc, nextConnectionId, watcher));
				nextConnectionId++;
			}
			when(wait(connections.getResult())) { ASSERT(false); }
		}
	} catch (Error& e) {
		TraceEvent(SevError, "BD_server").error(e);
		fprintf(stderr, "FdbDocServer: fatal error: %s\n", e.what());
		g_network->stop();
		throw;
	}
}

ACTOR Future<Void> extProxyHandler(Reference<BufferedConnection> src,
                                   Reference<BufferedConnection> dest,
                                   std::string label) {
	loop {
		choose {
			when(wait(src->onBytesAvailable(sizeof(ExtMsgHeader)))) {
				auto headerBytes = src->peekExact(sizeof(ExtMsgHeader));

				state ExtMsgHeader* header = (ExtMsgHeader*)headerBytes.begin();

				wait(src->onBytesAvailable(header->messageLength) && dest->onWritable());
				auto messageBytes = src->peekExact(header->messageLength);

				Promise<Void> finished;

				Reference<ExtMsg> msg = ExtMsg::create((ExtMsgHeader*)messageBytes.begin(),
				                                       messageBytes.begin() + sizeof(ExtMsgHeader), finished);
				fprintf(stderr, "\n%s: %s\n", label.c_str(), msg->toString().c_str());

				dest->write(messageBytes);

				src->advance(header->messageLength);
				src->pop(header->messageLength);
			}
			when(wait(src->onClosed())) {
				fprintf(stderr, "\n%s: connection closed\n", label.c_str());
				return Void();
			}
		}
	}
}

ACTOR Future<Void> extProxyConnection(Reference<BufferedConnection> serverConn, NetworkAddress connectAddr) {
	fprintf(stderr, "\nFdbDocProxy: connection from client\n");
	Reference<IConnection> conn = wait(INetworkConnections::net()->connect(connectAddr));
	fprintf(stderr, "FdbDocProxy: connected to server\n");
	state Reference<BufferedConnection> clientConn(new BufferedConnection(conn));

	wait(extProxyHandler(serverConn, clientConn, "C -> S") || extProxyHandler(clientConn, serverConn, "S -> C"));
	return Void();
}

ACTOR void extProxy(NetworkAddress listenAddr, NetworkAddress connectAddr) {
	state ActorCollection connections(false);

	try {
		state Reference<IListener> listener = INetworkConnections::net()->listen(listenAddr);

		loop choose {
			when(Reference<IConnection> conn = wait(listener->accept())) {
				Reference<BufferedConnection> bc(new BufferedConnection(conn));
				connections.add(extProxyConnection(bc, connectAddr));
			}
			when(wait(connections.getResult())) { ASSERT(false); }
		}
	} catch (Error& e) {
		fprintf(stderr, "FdbDocProxy: fatal error: %s\n", e.what());
		g_network->stop();
		throw;
	}
}

THREAD_FUNC networkThread(void* api) {
	((FDB::API*)api)->runNetwork();
	THREAD_RETURN;
}

ACTOR Future<Void> validateStorageVersion(Reference<DocumentLayer> docLayer) {
	state Reference<Transaction> tr = docLayer->database->createTransaction();
	int64_t timeout = 5000;
	tr->setOption(FDB_TR_OPTION_TIMEOUT, StringRef((uint8_t*)&(timeout), sizeof(int64_t)));
	tr->setOption(FDB_TR_OPTION_CAUSAL_READ_RISKY);
	state FDB::Key versionKey = docLayer->rootDirectory->pack(StringRef(DocLayerConstants::VERSION_KEY));
	Optional<FDB::FDBStandalone<StringRef>> version = wait(tr->get(versionKey));
	if (version.present()) {
		DataValue vv = DataValue::decode_value(version.get());
		if (vv.getSortType() != DVTypeCode::NUMBER) {
			throw bad_version();
		} else if (vv.getInt() < STORAGE_VERSION) {
			throw low_version();
		} else if (vv.getInt() > STORAGE_VERSION) {
			throw high_version();
		}
	} else {
		try {
			FDB::KeyRange directoryRange = docLayer->rootDirectory->range();
			Future<FDB::FDBStandalone<FDB::RangeResultRef>> frrr = tr->getRange(directoryRange);
			state Future<Standalone<VectorRef<StringRef>>> fSubDirs = docLayer->rootDirectory->list(tr);
			FDB::FDBStandalone<FDB::RangeResultRef> rrr = wait(frrr);
			if (!rrr.empty()) {
				throw directory_prefix_not_empty();
			}
			Standalone<VectorRef<StringRef>> subDirs = wait(fSubDirs);
			if (!subDirs.empty()) {
				throw directory_prefix_not_empty();
			}
		} catch (Error& e) {
			fprintf(stderr, "There is already data in the \"%s\" directory\n",
			        docLayer->rootDirectory->getPath()[0].toString().c_str());
			_exit(FDB_EXIT_ERROR);
		}
		tr->set(versionKey, DataValue(STORAGE_VERSION).encode_value());
		wait(tr->commit());
	}
	return Void();
}

namespace Tests {

Reference<DocumentLayer> g_docLayer;

ACTOR static void runUnitTests(StringRef testPattern) {
	state int testRunLimit = -1; //< Parameter?
	state std::vector<UnitTest*> tests;
	state int testsAvailable = 0;
	state int testsFailed = 0;
	state int testsExecuted = 0;

	for (auto t = g_unittests.tests; t != nullptr; t = t->next) {
		printf("Test: '%s' pattern: '%s'\n", t->name, testPattern.toString().c_str());
		if (StringRef(t->name).startsWith(testPattern)) {
			++testsAvailable;
			tests.push_back(t);
		}
	}
	g_random->randomShuffle(tests);
	if (testRunLimit > 0 && tests.size() > testRunLimit)
		tests.resize(testRunLimit);

	state std::vector<UnitTest*>::iterator t;
	for (t = tests.begin(); t != tests.end(); ++t) {
		auto test = *t;
		printf("Testing %s\n", test->name);

		state Error result = success();
		state double start_now = now();
		state double start_timer = timer();

		try {
			wait(test->func());
		} catch (Error& e) {
			++testsFailed;
			result = e;
			printf("  FAILED: %s\n", e.what());
		}
		++testsExecuted;
		double wallTime = timer() - start_timer;
		double simTime = now() - start_now;

		// self->totalWallTime += wallTime;
		// self->totalSimTime += simTime;

		auto unitTest = *t;
		TraceEvent(result.code() != error_code_success ? SevError : SevInfo, "UnitTest")
		    .detail("Name", unitTest->name)
		    .detail("File", unitTest->file)
		    .detail("Line", unitTest->line)
		    .error(result, true)
		    .detail("WallTime", wallTime)
		    .detail("FlowTime", simTime);
	}

	printf("Tests available: %d\n", testsAvailable);
	printf("Tests executed: %d\n", testsExecuted);
	printf("Tests failed: %d\n", testsFailed);

	_exit((testsFailed || !testsExecuted) ? 1 : 0);
}

} // namespace Tests

ACTOR void publishProcessMetrics() {
	// Give time to systemMonitor to log events.
	wait(delay(5.0));

	TraceEvent("BD_processMetricsPublisher");
	try {
		loop {
			// Update metrics at high priority.
			wait(delay(5.0, TaskMaxPriority));

			auto processMetrics = latestEventCache.get("ProcessMetrics");
			double processMetricsElapsed = processMetrics.getDouble("Elapsed");
			double cpuSeconds = processMetrics.getDouble("CPUSeconds");
			double mainThreadCPUSeconds = processMetrics.getDouble("MainThreadCPUSeconds");

			double cpuUsage = std::max(0.0, cpuSeconds / processMetricsElapsed) * 100;
			double mainThreadCPUUsage = std::max(0.0, mainThreadCPUSeconds / processMetricsElapsed) * 100;

			DocumentLayer::metricReporter->captureGauge(DocLayerConstants::MT_GUAGE_CPU_PERCENTAGE, cpuUsage);
			DocumentLayer::metricReporter->captureGauge(DocLayerConstants::MT_GUAGE_MAIN_THREAD_CPU_PERCENTAGE,
			                                            mainThreadCPUUsage);
			DocumentLayer::metricReporter->captureGauge(DocLayerConstants::MT_GUAGE_MEMORY_USAGE,
			                                            processMetrics.getInt64("Memory"));
		};
	} catch (...) {
		TraceEvent("BD_processMetricsPublisherException");
		throw;
	}
}

typedef std::vector<std::pair<FDBNetworkOption, Standalone<StringRef>>> NetworkOptionsT;

ACTOR void setup(NetworkAddress na,
                 Optional<uint16_t> proxyto,
                 std::string clusterFile,
                 ConnectionOptions options,
                 const char* rootDirectory,
                 std::string unitTestPattern,
                 std::vector<std::pair<std::string, std::string>> client_knobs,
                 NetworkOptionsT client_network_options,
                 std::string fdbDatacenterID,
				 Reference<ExtChangeStream> changeStream) {
	state FDB::API* fdb;
	try {
		fdb = FDB::API::selectAPIVersion(610);
		for (auto& knob : client_knobs)
			fdb->setNetworkOption(FDBNetworkOption::FDB_NET_OPTION_KNOB, knob.first + "=" + knob.second);
		for (auto& opt : client_network_options)
			fdb->setNetworkOption(opt.first, opt.second);
		fdb->setupNetwork();

		// These are setting up slow task back traces for Flow run loop. Nothing much to do with client but for reasons
		// it has to be present after setupNetwork() and before runNetwork()
		initSignalSafeUnwind();
		setupSlowTaskProfiler();

		startThread(networkThread, fdb);
	} catch (Error& e) {
		fprintf(stderr, "Failed to setup FDB! Error: %s\n", e.what());
		_exit(FDB_EXIT_ERROR);
	}

	if (!unitTestPattern.empty())
		Tests::runUnitTests(unitTestPattern);

	if (!proxyto.present()) {
		state Reference<DocumentLayer> docLayer;
		state Reference<Database> db;
		try {
			db = fdb->createDatabase(clusterFile);
			if (!fdbDatacenterID.empty()) {
				db->setDatabaseOption(FDBDatabaseOption::FDB_DB_OPTION_DATACENTER_ID,
				                      Optional<StringRef>(fdbDatacenterID));
			}
			try {
				state Reference<Transaction> tr3 = db->createTransaction();
				Optional<FDB::FDBStandalone<StringRef>> clusterFilePath =
				    wait(tr3->get(LiteralStringRef("\xff\xff/cluster_file_path")));
				TraceEvent("StartupConfig").detail("clusterFile", clusterFilePath.get().toString());
			} catch (Error& e) {
				if (e.code() != error_code_key_outside_legal_range) // KV-store 2.0
					throw;
			}
		} catch (Error& e) {
			fprintf(stderr, "Failed to setup FDB cluster! Error: %s\n", e.what());
			_exit(FDB_EXIT_ERROR);
		}

		state Reference<Transaction> tr2 = db->createTransaction();
		state int soFar = 0;
		loop {
			soFar += 5;
			state Future<Version> frv = tr2->getReadVersion();
			state Future<Void> t = delay(5.0);
			try {
				choose {
					when(wait(success(frv))) {
						TraceEvent("ClusterConnected");
						break;
					}
					when(wait(t)) {
						TraceEvent(SevError, "StartupFailure")
						    .detail("phase", "ConnectToCluster")
						    .detail("timeout", soFar)
						    .error(timed_out());
					}
				}
			} catch (Error& e) {
				try {
					wait(tr2->onError(e));
				} catch (Error& e) {
					TraceEvent(SevError, "ConnectionFailure").error(e);
					fprintf(stderr, "Failed to connect to FDB connection! Error: %s\n", e.what());
					_exit(FDB_EXIT_ERROR);
				}
			}
		}

		try {
			Reference<DirectoryLayer> d = ref(new DirectoryLayer());
			state Reference<DirectorySubspace> rootDir =
			    wait(d->createOrOpen(tr2, {StringRef(rootDirectory)}, LiteralStringRef("document")));
			wait(tr2->commit());
			docLayer = Reference<DocumentLayer>(new DocumentLayer(options, db, rootDir));
		} catch (Error& e) {
			try {
				// try once more in case we raced with another instance on startup
				state Reference<Transaction> tr = db->createTransaction();
				Reference<DirectoryLayer> d = ref(new DirectoryLayer());
				state Reference<DirectorySubspace> rootDir2 =
				    wait(d->createOrOpen(tr, {StringRef(rootDirectory)}, LiteralStringRef("document")));
				wait(tr->commit());
				docLayer = Reference<DocumentLayer>(new DocumentLayer(options, db, rootDir2));
			} catch (Error& e) {
				fprintf(stderr, "Failed to create of open directory! Error: %s\n", e.what());
				_exit(FDB_EXIT_ERROR);
			}
		}

		try {
			wait(validateStorageVersion(docLayer));
		} catch (Error& e) {
			// try again in case we raced with another instance
			try {
				wait(validateStorageVersion(docLayer));
			} catch (Error& e) {
				fprintf(stderr, "Failed to validate storage version! Error: %s\n", e.what());
				_exit(FDB_EXIT_ERROR);
			}
		}
		
		// Init dirs
		wait(initVirtualDirs(docLayer));

		// Changes watcher
		state Reference<ExtChangeWatcher> watcher = Reference<ExtChangeWatcher>(new ExtChangeWatcher(docLayer, changeStream));
		watcher->watch();

		// Oplog monitor		
		state std::function<void()> opMon = [=]{
			static Reference<DocumentLayer> dl = docLayer;
			oplogMonitor(dl, DocLayerConstants::OPLOG_EXPIRATION_TIME);
		};
		opMon();
		uncancellable(recurring(opMon, DocLayerConstants::OPLOG_CLEAN_INTERVAL, TaskMaxPriority));

		statusUpdateActor(FDB_DOC_VT_PACKAGE_NAME, na.ip.toString(), na.port, docLayer, timer() * 1000);
		extServer(docLayer, na, watcher);

		if (!unitTestPattern.empty())
			Tests::g_docLayer = docLayer;
	} else {
		extProxy(na, NetworkAddress::parse(format("127.0.0.1:%d", proxyto.get())));
	}
	publishProcessMetrics();
}

// Change stream connection handler
ACTOR Future<Void> extChangeConnection(Reference<BufferedConnection> bc,
                                       int64_t connectionId,
									   Reference<ExtChangeStream> changeStream) {
	state Future<Void> onError = bc->onClosed();
	state FutureStream<Standalone<StringRef>> messages = changeStream->newConnection(connectionId);

	try {
		loop {			
			choose {
				when(wait(onError)) {
					throw success();
				}
				when(Standalone<StringRef> msg = waitNext(messages)) {
					int64_t mSize = msg.size();
					auto sizePart = StringRef((uint8_t*)&mSize, sizeof(int64_t));
					bc->write(sizePart.withSuffix(LiteralStringRef("\n")).withSuffix(msg));
				}
			}
		}
	} catch (Error& e) {
		changeStream->deleteConnection(connectionId);
		return Void();
	}
}

// Change stream server
ACTOR void extChangeServer(NetworkAddress addr, Reference<ExtChangeStream> changeStream) {
	state ActorCollection connections(false);
	state int64_t nextConnectionId = 1;
	try {
		state Reference<IListener> listener = INetworkConnections::net()->listen(addr);

		fprintf(stdout, "FdbChangeServer: listening on %s\n", addr.toString().c_str());

		loop choose {
			when(Reference<IConnection> conn = wait(listener->accept())) {
				Reference<BufferedConnection> bc(new BufferedConnection(conn));
				connections.add(extChangeConnection(bc, nextConnectionId, changeStream));
				nextConnectionId++;
			}
			when(wait(connections.getResult())) { ASSERT(false); }
		}
	} catch (Error& e) {
		fprintf(stderr, "FdbChangeServer: fatal error: %s\n", e.what());
		g_network->stop();
		throw;
	}
}

// Change stream setup
void setupChangeListener(NetworkAddress na, Reference<ExtChangeStream> changeStream) {
	extChangeServer(na, changeStream);
}

static void printVersion() {
	fprintf(stderr, "FoundationDB Document Layer " FDB_DOC_VT_PACKAGE_NAME " (v" FDB_DOC_VT_VERSION ")\n");
	fprintf(stderr, "source version %s\n", getGitVersion());
	fprintf(stderr, "Flow source version %s\n", getFlowGitVersion());
}

static void printHelpTeaser(const char* name) {
	fprintf(stderr, "Try `%s --help' for more information.\n", name);
}

void printHelp(const char* name) {
	fprintf(stderr, "FoundationDB Document Layer " FDB_DOC_VT_PACKAGE_NAME " (v" FDB_DOC_VT_VERSION ")\n");
	fprintf(stderr, R"HELPTEXT(Usage: %s -l [IP_ADDRESS:]PORT [OPTIONS]

  -l ADDRESS Listen address, specified as `[IP_ADDRESS:]PORT' (defaults
             to 127.0.0.1:27016).
  -C CONNFILE
             The path of a file containing the connection string for the
             FoundationDB cluster. The default is first the value of the
             FDB_CLUSTER_FILE environment variable, then `./fdb.cluster',
             then `/etc/foundationdb/fdb.cluster'.
  -d NAME    Name of the directory (managed by the Directory Layer) in the
             Key-Value Store which the Document Layer will use to store all
             of its state.
  -L PATH    Store log files in the given folder (default is `.').
  --loggroup LOGGROUP
             Log Group to be used for logs (default is `default').
  -Rs SIZE   Roll over to a new log file after the current log file
             exceeds SIZE bytes. The default value is 10MiB.
  --maxlogssize SIZE
             Delete the oldest log file when the total size of all log
             files exceeds SIZE bytes. If set to 0, old log files will not
             be deleted. The default value is 100MiB.
  --pipeline OPTION
             Set to `compat' to enable pipelining compatibility mode. This
             mode is both slower and more difficult to use correctly than
             the default. It should only be turned on for compatibility purposes.
  --implicit-transaction-max-retries NUMBER
             Set the maximum number of times that transactions will be retried.
             Defaults to 3. If set to -1, will disable the retry limit.
  --implicit-transaction-timeout NUMBER
             Set a timeout in milliseconds for transactions. Defaults to 7000.
             If set to 0, will disable all timeouts.
  --metric_plugin PATH
             The path of the metric plugin dynamic library to load during runtime.
  --metric_plugin_config PATH
             The path to the configuration file of the plugin.
  --proxy-ports LISTEN_PORT MONGODB_PORT
             Runs Document Layer in proxy mode on LISTEN_PORT proxying all commands
             to MongoDB server running on MONGODB_PORT.
  --fdb_datacenter_id DC_ID
             The id of the preferred datacenter to use when connecting to a FoundationDB cluster
             that's run in multi-dc mode
)HELPTEXT",
	        name);
#ifndef TLS_DISABLED
	fprintf(stderr, TLS_HELP);
#endif
	fprintf(stderr, R"HELPTEXT(
  -V         Enable verbose logging.
  -h         Display this help message and exit.
  -v         Print version information and exit.

SIZE parameters may use one of the multiplicative suffixes B=1, KB=10^3,
KiB=2^10, MB=10^6, MiB=2^20, GB=10^9, GiB=2^30, TB=10^12, or TiB=2^40.
)HELPTEXT");
}

void setThreadName(const char* name) {
#ifdef __linux__
	prctl(PR_SET_NAME, name);
#endif
#ifdef __APPLE__
	pthread_setname_np(name);
#endif
}

// Determine network address for listening
NetworkAddress parseAddress(const std::string addr) {
	bool autoAddress = (addr.find(':') == std::string::npos);
	if (autoAddress) {
		return NetworkAddress::parse("127.0.0.1:" + addr);
	}

	return NetworkAddress::parse(addr);
}

int main(int argc, char** argv) {
	CSimpleOpt args(argc, argv, g_rgOptions, SO_O_EXACT);

	std::string commandLine;
	Optional<uint16_t> proxyfrom, proxyto;
	char* endptr;
	char** proxyports;
	std::string logFolder = ".";
	std::string logGroup = "default";
	std::string connFile;
	std::string listenAddr;
	std::string listenChangesAddr;
	std::string unitTestPattern;
	NetworkAddress na = NetworkAddress::parse("127.0.0.1:27016");
	NetworkAddress changeStreamNa = NetworkAddress::parse("127.0.0.1:8081");
	uint64_t rollsize = 10 << 20;
	uint64_t maxLogsSize = rollsize * 10;
	bool pipelineCompatMode = false;
	int retryLimit = 3;
	int timeoutMillies = 7000;
	const char* rootDirectory = "document";
	std::vector<std::pair<std::string, std::string>> knobs, client_knobs;
	NetworkOptionsT client_network_options;
	std::string metricReporterConfig;
	char* metricPluginPath = nullptr;
	std::string fdbDatacenterID;
#ifndef TLS_DISABLED
	Reference<TLSOptions> tlsOptions = Reference<TLSOptions>(new TLSOptions);
#endif
	std::string tlsCertPath, tlsKeyPath, tlsCAPath, tlsPassword;
	std::vector<std::string> tlsVerifyPeers;

	for (int a = 0; a < argc; a++) {
		if (a)
			commandLine += ' ';
		commandLine += argv[a];
	}

	while (args.Next()) {
		if (args.LastError() == SO_ARG_INVALID_DATA) {
			fprintf(stderr, "ERROR: invalid argument to option `%s'\n", args.OptionText());
			printHelp(argv[0]);
			return -1;
		}
		if (args.LastError() == SO_ARG_INVALID) {
			fprintf(stderr, "ERROR: argument given for option `%s'\n", args.OptionText());
			printHelp(argv[0]);
			return -1;
		}
		if (args.LastError() == SO_ARG_MISSING) {
			fprintf(stderr, "ERROR: missing argument for option `%s'\n", args.OptionText());
			printHelp(argv[0]);
			return -1;
		}
		if (args.LastError() == SO_OPT_INVALID) {
			fprintf(stderr, "ERROR: unknown option: `%s'\n", args.OptionText());
			printHelp(argv[0]);
			return -1;
		}
		if (args.LastError() != SO_SUCCESS) {
			fprintf(stderr, "ERROR: error parsing options\n");
			printHelp(argv[0]);
			return -1;
		}

		Optional<uint64_t> ti;

		switch (args.OptionId()) {
		case OPT_LISTEN:
			listenAddr = args.OptionArg();
			break;
		case OPT_NSNOTIFYLISTEN:
			listenChangesAddr = args.OptionArg();
			break;	
		case OPT_PROXYPORTS:
			proxyports = args.MultiArg(1);

			if (!proxyports) { // Couldn't find any arguments
				fprintf(stderr, "ERROR: Could not find proxy from and to ports\n");
				printHelp(argv[0]);
				return -1;
			}

			proxyfrom = strtol(proxyports[0], &endptr, 10);
			if (*endptr) {
				fprintf(stderr, "ERROR: Could not parse proxy from port `%s'\n", proxyports[0]);
				printHelp(argv[0]);
				return -1;
			}

			proxyports = args.MultiArg(1);

			if (proxyports) {
				proxyto = strtol(proxyports[0], &endptr, 10);
				if (*endptr) {
					fprintf(stderr, "ERROR: Could not parse proxy to port `%s'\n", proxyports[0]);
					printHelp(argv[0]);
					return -1;
				}
			} else {
				proxyto = 27017;
			}

			break;

		case OPT_CONNFILE:
			connFile = args.OptionArg();
			break;

		case OPT_LOGFOLDER:
			logFolder = args.OptionArg();
			break;

		case OPT_LOGGROUP:
			logGroup = args.OptionArg();
			break;

		case OPT_DIRECTORY:
			rootDirectory = args.OptionArg();
			break;

		case OPT_VERBOSE:
			verboseLogging = true;
			break;

		case OPT_SUPERVERBOSE:
			verboseLogging = true;
			verboseConsoleOutput = true;
			break;

		case OPT_ROLLSIZE: {
			const char* a = args.OptionArg();
			ti = parse_with_suffix(a);
			if (!ti.present()) {
				fprintf(stderr, "ERROR: Could not parse logsize `%s'\n", a);
				printHelpTeaser(argv[0]);
				return FDB_EXIT_ERROR;
			}
			rollsize = ti.get();
			break;
		}
		case OPT_MAXLOGSSIZE: {
			const char* a = args.OptionArg();
			ti = parse_with_suffix(a);
			if (!ti.present()) {
				fprintf(stderr, "ERROR: Could not parse maxlogssize `%s'\n", a);
				printHelpTeaser(argv[0]);
				return FDB_EXIT_ERROR;
			}
			maxLogsSize = ti.get();
			break;
		}

		case OPT_PIPELINECOMPATMODE: {
			const char* a = args.OptionArg();
			if (strcmp(a, "compat") == 0) {
				pipelineCompatMode = true;
			}
			break;
		}

		case OPT_SLOWQUERYLOG: {
			const char* a = args.OptionArg();
			if (strcmp(a, "off") == 0) {
				slowQueryLogging = false;
			}
			break;
		}

		case OPT_RETRYLIMIT: {
			const char* retryStr = args.OptionArg();
			int ret = static_cast<int>(strtol(retryStr, &endptr, 10));

			if (endptr == retryStr) {
				fprintf(stderr, "ERROR: could not parse retry limit `%s'\n", retryStr);
				printHelpTeaser(argv[0]);
				return FDB_EXIT_ERROR;
			}

			retryLimit = ret;
			break;
		}

		case OPT_TIMEOUT: {
			const char* timeoutStr = args.OptionArg();

			int ret = static_cast<int>(strtol(timeoutStr, &endptr, 10));

			if (endptr == timeoutStr) {
				fprintf(stderr, "ERROR: could not parse timeout `%s'\n", timeoutStr);
				printHelpTeaser(argv[0]);
				return FDB_EXIT_ERROR;
			}

			timeoutMillies = ret;
			break;
		}

		case OPT_HELP:
			printHelp(argv[0]);
			return 0;

		case OPT_VERSION:
			printVersion();
			return 0;

		case OPT_UNIT_TEST:
			unitTestPattern = args.OptionArg();
			break;

		case OPT_KNOB: {
			std::string syn = args.OptionSyntax();
			if (!StringRef(syn).startsWith(LiteralStringRef("--knob_"))) {
				fprintf(stderr, "ERROR: unable to parse knob option '%s'\n", syn.c_str());
				return FDB_EXIT_ERROR;
			}
			syn = syn.substr(7);
			knobs.emplace_back(syn, args.OptionArg());
			break;
		}

		case OPT_CLIENT_KNOB: {
			std::string syn = args.OptionSyntax();
			if (!StringRef(syn).startsWith(LiteralStringRef("--client_knob_"))) {
				fprintf(stderr, "ERROR: unable to parse client knob option '%s'\n", syn.c_str());
				return FDB_EXIT_ERROR;
			}
			syn = syn.substr(14);
			client_knobs.emplace_back(syn, args.OptionArg());
			break;
		}

		case OPT_CRASHONERROR:
			g_crashOnError = true;

		case OPT_BUGGIFY:
			client_network_options.push_back(
			    std::make_pair(FDBNetworkOption::FDB_NET_OPTION_BUGGIFY_ENABLE, StringRef()));
			break;

		case OPT_BUGGIFY_INTENSITY: {
			int64_t intensity = -1;
			const char* intensityStr = args.OptionArg();
			if (intensityStr)
				intensity = strtoll(intensityStr, nullptr, 10);
			if (intensity > 0 && intensity <= 100)
				client_network_options.push_back(
				    std::make_pair(FDBNetworkOption::FDB_NET_OPTION_BUGGIFY_SECTION_FIRED_PROBABILITY,
				                   StringRef((uint8_t*)&intensity, sizeof(intensity))));
			else {
				fprintf(stderr, "ERROR: buggify_intensity must be >= 0 and <= 100\n");
				return FDB_EXIT_ERROR;
			}
			break;
		}
		case OPT_METRIC_PLUGIN: {
			metricPluginPath = args.OptionArg();
			break;
		}
		case OPT_METRIC_CONFIG: {
			const char* metricPluginConfigPath = args.OptionArg();
			if (metricPluginConfigPath) {
				std::ifstream configFile(metricPluginConfigPath);
				metricReporterConfig.assign((std::istreambuf_iterator<char>(configFile)),
				                            (std::istreambuf_iterator<char>()));
			}
			break;
		}
		case OPT_FDB_DC_ID: {
			fdbDatacenterID = args.OptionArg();
			break;
		}
#ifndef TLS_DISABLED
		case TLSOptions::OPT_TLS_PLUGIN:
			args.OptionArg();
			break;
		case TLSOptions::OPT_TLS_CERTIFICATES:
			tlsCertPath = args.OptionArg();
			break;
		case TLSOptions::OPT_TLS_PASSWORD:
			tlsPassword = args.OptionArg();
			break;
		case TLSOptions::OPT_TLS_CA_FILE:
			tlsCAPath = args.OptionArg();
			break;
		case TLSOptions::OPT_TLS_KEY:
			tlsKeyPath = args.OptionArg();
			break;
		case TLSOptions::OPT_TLS_VERIFY_PEERS:
			tlsVerifyPeers.push_back(args.OptionArg());
			break;
#endif
		default:
			fprintf(stderr, "ERROR: Unexpected option\n");
			return FDB_EXIT_ERROR;
		}
	}

	if (listenAddr.empty() && !proxyfrom.present()) {
		printHelp(argv[0]);
		return 0;
	} else if (!listenAddr.empty() && proxyfrom.present()) {
		printHelp(argv[0]);
		return 0;
	}

	int randomSeed = platform::getRandomSeed();

	g_random = new DeterministicRandom(static_cast<uint32_t>(randomSeed));
	g_nondeterministic_random = new DeterministicRandom(static_cast<uint32_t>(platform::getRandomSeed()));

	init_document_error();

	g_network = newNet2(false);
#ifndef TLS_DISABLED
	try {
		if (tlsCertPath.size())
			tlsOptions->set_cert_file(tlsCertPath);
		if (tlsCAPath.size())
			tlsOptions->set_ca_file(tlsCAPath);
		if (tlsKeyPath.size()) {
			if (tlsPassword.size())
				tlsOptions->set_key_password(tlsPassword);

			tlsOptions->set_key_file(tlsKeyPath);
		}
		if (tlsVerifyPeers.size())
			tlsOptions->set_verify_peers(tlsVerifyPeers);

		tlsOptions->register_network();
	} catch (Error& e) {
		fprintf(stderr, "Error: %s \n", e.what());
		throw e;
	}
#endif
	if (metricPluginPath && metricPluginPath[0]) {
		TraceEvent(SevInfo, "MetricsInit")
		    .detail("pluginPath", metricPluginPath)
		    .detail("config", metricReporterConfig);
		DocumentLayer::metricReporter = IMetricReporter::init(metricPluginPath, metricReporterConfig.c_str());
	} else {
		// default to use `ConsoleMetric` plugin
		DocumentLayer::metricReporter = new ConsoleMetric(metricReporterConfig.c_str());
	}

	if (proxyfrom.present()) {
		na.port = proxyfrom.get();
	} else if (!listenAddr.empty()) {		
		try {
			na = parseAddress(listenAddr);
		} catch (Error&) {
			fprintf(stderr, "ERROR: Could not parse network address `%s' (specify as [IP_ADDRESS:]PORT)\n",
					listenAddr.c_str());
			printHelpTeaser(argv[0]);
			return FDB_EXIT_ERROR;
		}
	}

	if (!listenChangesAddr.empty()) {
		try {
			changeStreamNa = parseAddress(listenChangesAddr);
		} catch (Error&) {
			fprintf(stderr, 
					"ERROR: Could not parse notify network address `%s' (specify as [IP_ADDRESS:]PORT)\n",
					listenChangesAddr.c_str());
			printHelpTeaser(argv[0]);
			return FDB_EXIT_ERROR;
		}
	}

	ConnectionOptions options(pipelineCompatMode, retryLimit, timeoutMillies);

	delete FLOW_KNOBS;
	delete DOCLAYER_KNOBS;
	auto* flowKnobs = new FlowKnobs(false);
	auto* docLayerKnobs = new DocLayerKnobs(false);
	FLOW_KNOBS = flowKnobs;
	DOCLAYER_KNOBS = docLayerKnobs;

	for (auto const& k : knobs) {
		try {
			if (!flowKnobs->setKnob(k.first, k.second) && !docLayerKnobs->setKnob(k.first, k.second)) {
				fprintf(stderr, "Unrecognized knob option '%s'\n", k.first.c_str());
				_exit(FDB_EXIT_ERROR);
			}
		} catch (Error& e) {
			if (e.code() == error_code_invalid_option_value) {
				fprintf(stderr, "Invalid value '%s' for option '%s'\n", k.second.c_str(), k.first.c_str());
				_exit(FDB_EXIT_ERROR);
			}
			throw;
		}
	}

	// Setup Trace events
	setThreadName("fdbdoc-main");
	TraceEvent::setNetworkThread();
	openTraceFile(na, rollsize, maxLogsSize, logFolder, "fdbdoc-trace", logGroup);

	TraceEvent("StartingServer")
	    .detail("DocLayerPkgName", FDB_DOC_VT_PACKAGE_NAME)
	    .detail("DocLayerVersion", FDB_DOC_VT_VERSION)
	    .detail("DocLayerSourceVersion", getGitVersion())
	    .detail("FlowSourceVersion", getFlowGitVersion())
	    .detail("CommandLine", commandLine);

	Reference<ExtChangeStream> changeStream = ref(new ExtChangeStream());
	if (!listenChangesAddr.empty()) setupChangeListener(changeStreamNa, changeStream);
	setup(na, proxyto, connFile, options, rootDirectory, unitTestPattern, client_knobs,
		  client_network_options, fdbDatacenterID, changeStream);

	systemMonitor();
	uncancellable(recurring(&systemMonitor, 5.0, TaskMaxPriority));

	g_network->run();
}
