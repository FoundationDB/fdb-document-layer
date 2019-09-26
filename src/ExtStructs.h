/*
 * ExtStructs.h
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

#ifndef _EXT_STRUCTS_H_
#define _EXT_STRUCTS_H_

#pragma once

#include "flow/flow.h"

#include "bindings/flow/fdb_flow.h"

#include "BufferedConnection.h"
#include "DocLayer.h"
#include "MetadataManager.h"
#include "Oplogger.h"

#include "QLPlan.actor.h"

extern bool verboseLogging;
extern bool verboseConsoleOutput;
extern bool slowQueryLogging;

/**
 * This holds the result for all kinds of write operations - Insert, Remove and Update.
 * This also supports bulk writes. A Bulk write responds with aggregate counters and
 * upserted list but separate error information for each write.
 */
struct WriteCmdResult {
	/**
	 * Remove -> number of documents deleted.
	 * Update -> number of documents updated or upserted.
	 */
	int64_t n;

	// Only for Update. Number of documents updated. Upserted documents are not included.
	int64_t nModified;

	/**
	 * ObjectID of upserted document. For a single write it would be just one upsert.
	 * For bulk write, its just list of ObjectIDs. Unlike writeErrors, for upsertedOIDList
	 * we don't maintain any mapping from command index to upsertedOID, its just a list of
	 * OIDs.
	 */
	std::vector<Standalone<StringRef>> upsertedOIDList;

	/**
	 * Array of write errors. Each entry has fields - 'index', 'code' '$err' and 'errmsg'. 'index'
	 * points to write command in the request. So, separate write error for each write failed.
	 */
	std::vector<bson::BSONObj> writeErrors;

	WriteCmdResult() : WriteCmdResult(0, 0) {}
	explicit WriteCmdResult(int64_t n) : WriteCmdResult(n, 0) {}
	WriteCmdResult(int64_t n, std::vector<bson::BSONObj> const& writeErrors) : WriteCmdResult(n, 0) {
		this->writeErrors = writeErrors;
	}
	WriteCmdResult(int64_t n, int64_t nModified) : n(n), nModified(nModified) {}
};

enum WriteType { INSERT, UPDATE, REMOVAL, NONE };

/**
 * Write result for old style writes.
 */
struct WriteResult : public WriteCmdResult {
	WriteType type;

	WriteResult(WriteCmdResult const& cmdResult, WriteType type) : WriteCmdResult(cmdResult), type(type) {}
	WriteResult() : WriteCmdResult(), type(WriteType::NONE) {}
};

enum QueryOptions {
	TAILABLE_CURSOR = 1 << 1,
	SLAVE_OK = 1 << 2,
	OPLOG_REPLAY = 1 << 3,
	AWAIT_DATA = 1 << 4,
	NO_CURSOR_TIMEOUT = 1 << 5,
	EXHAUST = 1 << 6,
	PARTIAL = 1 << 7
};

Future<WriteResult> lastErrorOrLastResult(Future<WriteResult> const& previous,
                                          Future<WriteResult> const& next,
                                          FlowLock* const& lock);

struct ExtChangeStream : ReferenceCounted<ExtChangeStream>, NonCopyable {
	std::map<int64_t, PromiseStream<Standalone<StringRef>>> connections;

	FutureStream<Standalone<StringRef>> newConnection(int64_t connectionId);
	void deleteConnection(int64_t connectionId);
	void writeMessage(Standalone<StringRef> msg);
	void clear();
	int countConnections();
};

struct ExtChangeWatcher: ReferenceCounted<ExtChangeWatcher>, NonCopyable {
	Reference<DocumentLayer> docLayer;
	Reference<ExtChangeStream> changeStream;
	FutureStream<double> tsStreamReader;
	PromiseStream<double> tsStreamWriter;
	PromiseStream<double> tsScanPromise;
	FutureStream<double> tsScanFuture;

	ExtChangeWatcher(Reference<DocumentLayer> docLayer, Reference<ExtChangeStream> changeStream): 
	docLayer(docLayer), changeStream(changeStream) {
		tsStreamReader = tsStreamWriter.getFuture();
		tsScanFuture = tsScanPromise.getFuture();
	};

	void update(double timestamp);
	void watch();
};

struct ExtConnection : ReferenceCounted<ExtConnection>, NonCopyable {
	Reference<DocumentLayer> docLayer;
	Reference<MetadataManager> mm;
	ConnectionOptions options;
	std::map<int64_t, Reference<Cursor>> cursors;
	int64_t connectionId;
	Reference<BufferedConnection> bc;
	Future<WriteResult> lastWrite;
	Reference<ExtChangeWatcher> watcher;

	Reference<DocTransaction> getOperationTransaction();
	Reference<Plan> wrapOperationPlan(Reference<Plan> plan, bool isReadOnly, Reference<UnboundCollectionContext> cx);
	Reference<Plan> isolatedWrapOperationPlan(Reference<Plan> plan);
	Reference<Plan> isolatedWrapOperationPlan(Reference<Plan> plan, int64_t timeout, int64_t retryLimit);
	Reference<Plan> wrapOperationPlanOplog(Reference<Plan> plan,
										Reference<IOplogInserter> oplogInserter, 
										Reference<UnboundCollectionContext> cx);
	Future<Void> beforeWrite(int desiredPermits = 1);
	Future<Void> afterWrite(Future<WriteResult> result, int releasePermits = 1);

	ExtConnection(Reference<DocumentLayer> docLayer, 
				  Reference<BufferedConnection> bc, 
				  int64_t connectionId, 
				  Reference<ExtChangeWatcher> watcher)
	    : docLayer(docLayer),
	      bc(bc),
	      lastWrite(WriteResult()),
	      options(docLayer->defaultConnectionOptions),
	      lock(Reference<FlowLock>(new FlowLock(DOCLAYER_KNOBS->CONNECTION_MAX_PIPELINE_DEPTH))),
	      cursors(),
	      mm(docLayer->mm),
	      connectionId(connectionId),
		  watcher(watcher),
	      maxReceivedRequestID(0),
	      nextServerGeneratedRequestID(0) {}

	/**
	 * Get a new server-generated request ID for this connection.  Used in Exhaust mode
	 * where server generates multiple replies to a single Query.  Will probably be used
	 * in other situations as well in which server generates Replies not specifically solicited.
	 */
	int32_t getNewRequestID() { return nextServerGeneratedRequestID++; }

	/**
	 * Update the max RequestID we have seen received on this connection.
	 */
	void updateMaxReceivedRequestID(int32_t id) {
		if (id > maxReceivedRequestID)
			maxReceivedRequestID = id;

		// Now make sure the RequestIDs the server will generate are very far ahead of the ones seen from the client.
		int32_t minGeneratedID = maxReceivedRequestID + 1000000;
		if (minGeneratedID > nextServerGeneratedRequestID)
			nextServerGeneratedRequestID = minGeneratedID;
	}

private:
	Future<Void> currentWriteLocked;
	Reference<FlowLock> lock;
	int32_t maxReceivedRequestID;
	int32_t nextServerGeneratedRequestID;
};

struct ExtMsgHeader : NonCopyable {
	int32_t messageLength;
	int32_t requestID;
	int32_t responseTo;
	int32_t opCode;
	ExtMsgHeader() : messageLength(0), requestID(0), responseTo(0), opCode(0) {}
	std::string toString() {
		return format("HEADER: messageLength=%d, requestID=%d, responseTo=%d, opCode=%d", messageLength, requestID,
		              responseTo, opCode);
	}
};

#pragma pack(push, 4)
struct ExtReplyHeader : ExtMsgHeader {
	int32_t responseFlags;
	int64_t cursorID;
	int32_t startingFrom;
	int32_t documentCount;
	ExtReplyHeader() : responseFlags(0), cursorID(0), startingFrom(0), documentCount(0) {
		static_assert(sizeof(ExtReplyHeader) == 36, "ExtReplyHeader size mismatch");
	}
};
#pragma pack(pop)

#endif /* _EXT_STRUCTS_H_ */
