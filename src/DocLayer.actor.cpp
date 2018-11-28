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

#include <boost/bind.hpp>
#include <boost/function.hpp>

#include "BufferedConnection.h"
#include "ConsoleMetric.h"
#include "Cursor.h"
#include "DocLayer.h"
#include "ExtMsg.h"
#include "IMetric.h"
#include "StatusService.h"

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
	OPT_METRIC_CONFIG
};
CSimpleOpt::SOption g_rgOptions[] = {{OPT_CONNFILE, "-C", SO_REQ_SEP},
                                     {OPT_CONNFILE, "--cluster_file", SO_REQ_SEP},
                                     {OPT_HELP, "-h", SO_NONE},
                                     {OPT_VERSION, "-v", SO_NONE},
                                     {OPT_VERSION, "--version", SO_NONE},
                                     {OPT_PROXYPORTS, "-p", SO_MULTI},
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

                                     SO_END_OF_OPTIONS};

using namespace FDB;

bool verboseLogging = false;
bool verboseConsoleOutput = false;
bool slowQueryLogging = true;
IMetricReporter* DocumentLayer::metricReporter;

extern const char* getHGVersion();
extern const char* getFlowHGVersion();
extern bool g_crashOnError;

ACTOR Future<Void> wrapError(Future<Void> actorThatCouldThrow) {
	try {
		Void _ = wait(actorThatCouldThrow);
	} catch (Error& e) {
		TraceEvent(SevError, "BackgroundTask").detail("Error", e.what()).backtrace();
	}
	return Void();
}

Future<Void> processRequest(Reference<ExtConnection> ec,
                            ExtMsgHeader* header,
                            const uint8_t* body,
                            Promise<Void> finished) {
	try {
		Reference<ExtMsg> msg = ExtMsg::create(header, body, finished);
		if (verboseLogging)
			TraceEvent("BD_processRequest").detail("Message", msg->toString());
		if (verboseConsoleOutput)
			fprintf(stderr, "C -> S: %s\n\n", msg->toString().c_str());
		return msg->run(ec);
	} catch (Error& e) {
		TraceEvent(SevWarnAlways, "BD_processRequest").detail("errorUnknownOpCode", header->opCode);
		return Void();
	}
}

ACTOR Future<Void> popDisposedMessages(Reference<BufferedConnection> bc,
                                       FutureStream<std::pair<int, Future<Void>>> msg_size_inuse) {
	loop {
		state std::pair<int, Future<Void>> s = waitNext(msg_size_inuse);
		try {
			Void _ = wait(s.second);
		} catch (...) {
		}
		bc->pop(s.first);
	}
}

ACTOR Future<Void> extServerConnection(Reference<DocumentLayer> docLayer,
                                       Reference<BufferedConnection> bc,
                                       int64_t connectionId) {
	if (verboseLogging)
		TraceEvent("BD_serverNewConnection");

	state Reference<ExtConnection> ec = Reference<ExtConnection>(new ExtConnection(docLayer, bc, connectionId));
	state PromiseStream<std::pair<int, Future<Void>>> msg_size_inuse;
	state Future<Void> onError = ec->bc->onClosed() || popDisposedMessages(bc, msg_size_inuse.getFuture());

	DocumentLayer::metricReporter->captureGauge("activeConnections", ++docLayer->nrConnections);
	try {
		ec->startHousekeeping();

		loop {
			state Promise<Void> finished; // will be broken (or set or whatever) only when the memory we are passing to
			                              // processRequest is no longer needed and can be popped
			choose {
				when(Void _ = wait(onError)) {
					if (verboseLogging)
						TraceEvent("BD_serverClosedConnection");
					throw connection_failed();
				}
				when(Void _ = wait(ec->bc->onBytesAvailable(sizeof(ExtMsgHeader)))) {
					auto sr = ec->bc->peekExact(sizeof(ExtMsgHeader));

					state ExtMsgHeader* header = (ExtMsgHeader*)sr.begin();

					// FIXME: Check for unreasonable lengths

					Void _ = wait(ec->bc->onBytesAvailable(header->messageLength));
					auto sr = ec->bc->peekExact(header->messageLength);

					DocumentLayer::metricReporter->captureHistogram("messageLength", header->messageLength);
					DocumentLayer::metricReporter->captureMeter("messageRate", 1);

					/* We don't use hdr in this call because the second peek may
					   have triggered a copy that the first did not, but it's nice
					   for everything at and below processRequest to assume that
					   body - header == sizeof(ExtMsgHeader) */
					ec->updateMaxReceivedRequestID(header->requestID);
					Void _ = wait(
					    processRequest(ec, (ExtMsgHeader*)sr.begin(), sr.begin() + sizeof(ExtMsgHeader), finished));

					ec->bc->advance(header->messageLength);
					msg_size_inuse.send(std::make_pair(header->messageLength, finished.getFuture()));
				}
			}
		}
	} catch (Error& e) {
		DocumentLayer::metricReporter->captureGauge("activeConnections", --docLayer->nrConnections);
		return Void();
	}
}

ACTOR void extServer(Reference<DocumentLayer> docLayer, NetworkAddress addr) {
	state ActorCollection connections(false);
	state int64_t nextConnectionId = 1;
	try {
		state Reference<IListener> listener = INetworkConnections::net()->listen(addr);

		TraceEvent("BD_server").detail("version", FDB_DOC_VT_PACKAGE_NAME).detail("address", addr.toString());
		fprintf(stdout, "FdbDocServer (%s): listening on %s\n", FDB_DOC_VT_PACKAGE_NAME, addr.toString().c_str());

		loop choose {
			when(Reference<IConnection> conn = wait(listener->accept())) {
				Reference<BufferedConnection> bc(new BufferedConnection(conn));
				connections.add(extServerConnection(docLayer, bc, nextConnectionId));
				nextConnectionId++;
			}
			when(Void _ = wait(connections.getResult())) { ASSERT(false); }
		}
	} catch (Error& e) {
		TraceEvent(SevError, "BD_server").detail("fatal_error", e.what());
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
			when(Void _ = wait(src->onBytesAvailable(sizeof(ExtMsgHeader)))) {
				auto sr = src->peekExact(sizeof(ExtMsgHeader));

				state ExtMsgHeader* header = (ExtMsgHeader*)sr.begin();

				Void _ = wait(src->onBytesAvailable(header->messageLength) && dest->onWritable());
				auto sr = src->peekExact(header->messageLength);

				Promise<Void> finished;

				Reference<ExtMsg> msg =
				    ExtMsg::create((ExtMsgHeader*)sr.begin(), sr.begin() + sizeof(ExtMsgHeader), finished);
				fprintf(stderr, "\n%s: %s\n", label.c_str(), msg->toString().c_str());

				dest->write(sr);

				src->advance(header->messageLength);
				src->pop(header->messageLength);
			}
			when(Void _ = wait(src->onClosed())) {
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

	Void _ =
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
			when(Void _ = wait(connections.getResult())) { ASSERT(false); }
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
	state Reference<Transaction> tr(new Transaction(docLayer->database));
	int64_t timeout = 5000;
	tr->setOption(FDB_TR_OPTION_TIMEOUT, StringRef((uint8_t*)&(timeout), sizeof(int64_t)));
	tr->setOption(FDB_TR_OPTION_CAUSAL_READ_RISKY);
	state FDB::Key versionKey = docLayer->rootDirectory->pack(LiteralStringRef("version"));
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
		Void _ = wait(tr->commit());
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
			Void _ = wait(test->func());
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

		auto test = *t;
		TraceEvent(result.code() != error_code_success ? SevError : SevInfo, "UnitTest")
		    .detail("Name", test->name)
		    .detail("File", test->file)
		    .detail("Line", test->line)
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

typedef std::vector<std::pair<FDBNetworkOption, Standalone<StringRef>>> NetworkOptionsT;

ACTOR void setup(NetworkAddress na,
                 Optional<uint16_t> proxyto,
                 std::string clusterFile,
                 ConnectionOptions options,
                 const char* rootDirectory,
                 std::string unitTestPattern,
                 std::vector<std::pair<std::string, std::string>> client_knobs,
                 NetworkOptionsT client_network_options) {
	state FDB::API* fdb;
	try {
		fdb = FDB::API::selectAPIVersion(510);
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
		state Reference<DatabaseContext> db;
		try {
			auto cluster = fdb->createCluster(clusterFile);
			Reference<DatabaseContext> database = cluster->createDatabase();
			db = database;
			try {
				state Reference<Transaction> tr3(new Transaction(db));
				Optional<FDB::FDBStandalone<StringRef>> clusterFilePath =
				    wait(tr3->get(LiteralStringRef("\xff\xff/cluster_file_path")));
				TraceEvent("StartupConfig").detail("clusterfile", clusterFilePath.get().toString());
			} catch (Error& e) {
				if (e.code() != error_code_key_outside_legal_range) // KV-store 2.0
					throw;
			}
		} catch (Error& e) {
			fprintf(stderr, "Failed to setup FDB cluster! Error: %s\n", e.what());
			_exit(FDB_EXIT_ERROR);
		}

		state Reference<Transaction> tr2(new Transaction(db));
		state int soFar = 0;
		loop {
			soFar += 5;
			state Future<Version> frv = tr2->getReadVersion();
			state Future<Void> t = delay(5.0);
			try {
				choose {
					when(Version rv = wait(frv)) {
						TraceEvent("ClusterConnected");
						break;
					}
					when(Void _ = wait(t)) {
						TraceEvent(SevError, "StartupFailure")
						    .detail("phase", "ConnectToCluster")
						    .detail("timeout", soFar);
					}
				}
			} catch (Error& e) {
				try {
					Void _ = wait(tr2->onError(e));
				} catch (Error& e) {
					fprintf(stderr, "Failed to connect to FDB connection! Error: %s\n", e.what());
					_exit(FDB_EXIT_ERROR);
				}
			}
		}

		try {
			Reference<DirectoryLayer> d = ref(new DirectoryLayer());
			state Reference<DirectorySubspace> rootDir =
			    wait(d->createOrOpen(tr2, {StringRef(rootDirectory)}, LiteralStringRef("document")));
			Void _ = wait(tr2->commit());
			docLayer = Reference<DocumentLayer>(new DocumentLayer(options, db, rootDir));
		} catch (Error& e) {
			try {
				// try once more in case we raced with another instance on startup
				state Reference<Transaction> tr(new Transaction(db));
				Reference<DirectoryLayer> d = ref(new DirectoryLayer());
				state Reference<DirectorySubspace> rootDir2 =
				    wait(d->createOrOpen(tr, {StringRef(rootDirectory)}, LiteralStringRef("document")));
				Void _ = wait(tr->commit());
				docLayer = Reference<DocumentLayer>(new DocumentLayer(options, db, rootDir2));
			} catch (Error& e) {
				fprintf(stderr, "Failed to create of open directory! Error: %s\n", e.what());
				_exit(FDB_EXIT_ERROR);
			}
		}

		try {
			Void _ = wait(validateStorageVersion(docLayer));
		} catch (Error& e) {
			// try again in case we raced with another instance
			try {
				Void _ = wait(validateStorageVersion(docLayer));
			} catch (Error& e) {
				fprintf(stderr, "Failed to validate storage version! Error: %s\n", e.what());
				_exit(FDB_EXIT_ERROR);
			}
		}
		statusUpdateActor(FDB_DOC_VT_PACKAGE_NAME, toIPString(na.ip), na.port, docLayer, timer() * 1000);
		extServer(docLayer, na);

		if (!unitTestPattern.empty())
			Tests::g_docLayer = docLayer;
	} else {
		extProxy(na, NetworkAddress::parse(format("127.0.0.1:%d", proxyto.get())));
	}
}

static void printVersion() {
	fprintf(stderr, "FoundationDB Document Layer " FDB_DOC_VT_PACKAGE_NAME " (v" FDB_DOC_VT_VERSION ")\n");
	fprintf(stderr, "source version %s\n", getHGVersion());
	fprintf(stderr, "Flow source version %s\n", getFlowHGVersion());
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
  -V         Enable verbose logging.
  -h         Display this help message and exit.
  -v         Print version information and exit.

SIZE parameters may use one of the multiplicative suffixes B=1, KB=10^3,
KiB=2^10, MB=10^6, MiB=2^20, GB=10^9, GiB=2^30, TB=10^12, or TiB=2^40.
)HELPTEXT",
	        name);
}

void setThreadName(const char* name) {
#ifdef __linux__
	prctl(PR_SET_NAME, name);
#endif
#ifdef __APPLE__
	pthread_setname_np(name);
#endif
}

int main(int argc, char** argv) {
	CSimpleOpt args(argc, argv, g_rgOptions, SO_O_EXACT);

	Optional<uint16_t> proxyfrom, proxyto;
	char* endptr;
	char** proxyports;
	std::string logFolder = ".";
	std::string logGroup = "default";
	std::string connFile;
	std::string listenAddr;
	std::string unitTestPattern;
	NetworkAddress na = NetworkAddress::parse("127.0.0.1:27016");
	uint64_t rollsize = 10 << 20;
	uint64_t maxLogsSize = rollsize * 10;
	bool pipelineCompatMode = false;
	int retryLimit = 3;
	int timeout = 7000;
	const char* rootDirectory = "document";
	std::vector<std::pair<std::string, std::string>> knobs, client_knobs;
	NetworkOptionsT client_network_options;
	std::string metricReporterConfig;
	char* metricPluginPath = nullptr;

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

			timeout = ret;
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

	g_network = newNet2(NetworkAddress(), false);

	if (metricPluginPath && metricPluginPath[0]) {
		DocumentLayer::metricReporter = IMetricReporter::init(metricPluginPath, metricReporterConfig);
	} else {
		// default to use `ConsoleMetric` plugin
		DocumentLayer::metricReporter = new ConsoleMetric(metricReporterConfig);
	}

	if (proxyfrom.present()) {
		na.port = proxyfrom.get();
	} else if (!listenAddr.empty()) {
		bool autoAddress = (listenAddr.find(':') == std::string::npos);
		if (autoAddress) {
			na = NetworkAddress::parse("127.0.0.1:" + listenAddr);
		} else {
			try {
				na = NetworkAddress::parse(listenAddr);
			} catch (Error&) {
				fprintf(stderr, "ERROR: Could not parse network address `%s' (specify as [IP_ADDRESS:]PORT)\n",
				        listenAddr.c_str());
				printHelpTeaser(argv[0]);
				return FDB_EXIT_ERROR;
			}
		}
	}

	ConnectionOptions options(pipelineCompatMode, retryLimit, timeout);

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

	setup(na, proxyto, connFile, options, rootDirectory, unitTestPattern, client_knobs, client_network_options);

	systemMonitor();
	uncancellable(recurring(&systemMonitor, 5.0, TaskMaxPriority));

	g_network->run();
}
