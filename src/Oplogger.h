/*
 * Oplogger.h
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

#ifndef _OPLOGGER_H_
#define _OPLOGGER_H_

#pragma once

#include <map>
#include "bson.h"
#include "flow/flow.h"
#include "flow/FastRef.h"
#include "MetadataManager.h"
#include "QLContext.h"

struct IOplogInserter {
	virtual void addref() = 0;
	virtual void delref() = 0;
    virtual Future<Reference<IReadWriteContext>> insert(Reference<CollectionContext> cx, bson::BSONObj obj) = 0;
};

// OploagActor - documents inserter
struct OplogActor: ReferenceCounted<OplogActor>, NonCopyable {
	

	OplogActor(Reference<IOplogInserter> inserter) : inserter(inserter) {}

	Future<Reference<IReadWriteContext>> insertOp(Reference<CollectionContext> cx, 
												 std::string ns,
												 bson::BSONObj obj);
	Future<Reference<IReadWriteContext>> updateOp(Reference<CollectionContext> cx, 
												  std::string ns, 
												  std::string id,
												  bson::BSONObj obj);
	Future<Reference<IReadWriteContext>> deleteOp(Reference<CollectionContext> cx, 
												  std::string ns,
												  std::string id);

	private:
		void prepareBuilder(bson::BSONObjBuilder* builder, std::string op, std::string ns);
    	Reference<IOplogInserter> inserter;
};

// Oplogger - documents processor
struct Oplogger: ReferenceCounted<Oplogger>, NonCopyable {
    Oplogger(Namespace operationNs, Reference<IOplogInserter> oplogInserter);

	// Clean operations
    void reset();

	// Checks if oplog is enabled for given namespace
    bool isEnabled();

	Future<Reference<UnboundCollectionContext>> getUnboundContext(Reference<MetadataManager> mm, Reference<DocTransaction> tr);
    Deque<Future<Reference<IReadWriteContext>>> buildOplogs(Reference<CollectionContext> ctx);
    void addOriginalDoc(const DataValue *dv);
    void addUpdatedDoc(const DataValue *dv);
	void addOriginalDoc(bson::BSONObj doc);
	void addUpdatedDoc(bson::BSONObj doc);

	private:
		void gatherObjectsInfo(bson::BSONObj oObj, bool isSource);
		bool isValidNamespace(std::string ns);

		Namespace oplogNs;
		std::string ns;
		Reference<OplogActor> actor;
		std::map<std::string, std::pair<bson::BSONObj, bson::BSONObj>> operations;
		bool enabled;
};

#endif