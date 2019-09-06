/*
 * ExtMsg.h
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

#if defined(NO_INTELLISENSE) && !defined(_EXT_MSG_ACTOR_G_H_)
#define _EXT_MSG_ACTOR_G_H_
#include "ExtMsg.actor.g.h"
#elif !defined(_EXT_MSG_ACTOR_H_)
#define _EXT_MSG_ACTOR_H_

#pragma once

#include "Constants.h"
#include "Ext.h"
#include "ExtOperator.h"
#include "ExtStructs.h"
#include "ExtUtil.actor.h"
#include "IDispatched.h"
#include "QLPlan.actor.h"
#include "QLPredicate.h"
#include "QLOperations.h"
#include "Oplogger.h"

#include "flow/flow.h"

#include "bson.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct ExtMsg : IDispatched<ExtMsg, int32_t, std::function<ExtMsg*(ExtMsgHeader*, const uint8_t*)>>,
                ReferenceCounted<ExtMsg> {
	static Reference<ExtMsg> create(ExtMsgHeader* header, const uint8_t* body, Promise<Void> finished) {
		Reference<ExtMsg> r(dispatch(header->opCode)(header, body));
		r->break_when_finished = finished;
		return r;
	}

	/**
	 * This promise will be broken only when this class is destructed, which triggers
	 * the release of the underlying memory
	 */
	Promise<Void> break_when_finished;

	virtual ~ExtMsg() = default;

	virtual std::string toString() = 0;
	virtual Future<Void> run(Reference<ExtConnection>) = 0;

	template <class ExtMsgType>
	struct Factory {
		static ExtMsg* create(ExtMsgHeader* header, const uint8_t* body) {
			return (ExtMsg*)(new ExtMsgType(header, body));
		}
	};
};

#define REGISTER_MSG(Msg) REGISTER_FACTORY(ExtMsg, Msg, opcode)

struct ExtMsgQuery : ExtMsg, FastAllocated<ExtMsgQuery> {
	enum { opcode = 2004 };

	ExtMsgHeader* header;
	int32_t flags;
	Namespace ns;
	int32_t numberToSkip;
	int32_t numberToReturn;
	bson::BSONObj query;
	bson::BSONObj returnFieldSelector;
	bool isCmd;

	std::string toString() override;
	Future<Void> run(Reference<ExtConnection>) override;

	std::string getDBName();

private:
	ExtMsgQuery(ExtMsgHeader*, const uint8_t*);
	friend struct ExtMsg::Factory<ExtMsgQuery>;
};

struct ExtMsgReply : ExtMsg, FastAllocated<ExtMsgReply> {
	enum { opcode = 1 };

	ExtMsgHeader* header;
	bson::BSONObj query;
	std::vector<bson::BSONObj> documents;

	ExtReplyHeader replyHeader;

	// For constructing our own replies, rather than parsing a reply
	// off of the wire
	ExtMsgReply(ExtMsgHeader*, bson::BSONObj const& query);
	explicit ExtMsgReply(ExtMsgHeader*);

	std::string toString() override;
	Future<Void> run(Reference<ExtConnection>) override { UNREACHABLE(); }

	void addDocument(bson::BSONObj doc) {
		documents.push_back(doc.getOwned());
		replyHeader.documentCount++;
	}

	void setError(std::string msg, int code) {
		// Clear if response contains any outstanding documents
		documents.clear();
		replyHeader.documentCount = 0;

		// Set query failure flag
		addResponseFlag(2);

		// clang-format off
		addDocument(BSON("ok" << 0 <<
		                 "$err" << msg <<
		                 "code" << code));
		// clang-format on
	}

	void setError(Error e) { setError(e.what(), e.code()); }

	void setResponseFlags(int32_t flags) { replyHeader.responseFlags = flags; }

	void addResponseFlag(int32_t flag) { replyHeader.responseFlags = replyHeader.responseFlags | flag; }

	void write(Reference<ExtConnection>);

private:
	ExtMsgReply(ExtMsgHeader*, const uint8_t*);
	friend struct ExtMsg::Factory<ExtMsgReply>;
};

struct ExtUpdateCmd {
	bson::BSONObj selector;
	bson::BSONObj update;
	bool upsert;
	bool multi;

	ExtUpdateCmd(bson::BSONObj selector, bson::BSONObj update, bool upsert, bool multi)
	    : selector(selector), update(update), upsert(upsert), multi(multi) {}
};

struct ExtMsgUpdate : ExtMsg, FastAllocated<ExtMsgUpdate> {
	enum { opcode = 2001 };
	enum Flags { UPSERT = 0x01, MULTI = 0x02 };

	ExtMsgHeader* header;
	Namespace ns;
	int32_t flags;
	bson::BSONObj selector;
	bson::BSONObj update;

	bool upsert;
	bool multi;

	std::string toString() override;
	Future<Void> run(Reference<ExtConnection>) override;

private:
	ExtMsgUpdate(ExtMsgHeader*, const uint8_t*);
	friend struct ExtMsg::Factory<ExtMsgUpdate>;
};

struct ExtMsgInsert : ExtMsg, FastAllocated<ExtMsgInsert> {
	enum { opcode = 2002 };

	ExtMsgHeader* header;
	int32_t flags;
	Namespace ns;
	std::list<bson::BSONObj> documents;

	std::string toString() override;
	Future<Void> run(Reference<ExtConnection>) override;

private:
	ExtMsgInsert(ExtMsgHeader*, const uint8_t*);
	friend struct ExtMsg::Factory<ExtMsgInsert>;
};

struct ExtMsgGetMore : ExtMsg, FastAllocated<ExtMsgGetMore> {
	enum { opcode = 2005 };

	ExtMsgHeader* header;
	Namespace ns;
	int32_t numberToReturn;
	int64_t cursorID;

	std::string toString() override;
	Future<Void> run(Reference<ExtConnection>) override;

private:
	ExtMsgGetMore(ExtMsgHeader*, const uint8_t*);
	friend struct ExtMsg::Factory<ExtMsgGetMore>;
};

struct ExtMsgDelete : ExtMsg, FastAllocated<ExtMsgDelete> {
	enum { opcode = 2006 };

	ExtMsgHeader* header;
	Namespace ns;
	int32_t flags;
	std::vector<bson::BSONObj> selectors;

	std::string toString() override;
	Future<Void> run(Reference<ExtConnection>) override;

private:
	ExtMsgDelete(ExtMsgHeader*, const uint8_t*);
	friend struct ExtMsg::Factory<ExtMsgDelete>;
};

struct ExtMsgKillCursors : ExtMsg, FastAllocated<ExtMsgKillCursors> {
	enum { opcode = 2007 };

	ExtMsgHeader* header;
	int32_t numberOfCursorIDs;
	int64_t* cursorIDs;

	std::string toString() override;
	Future<Void> run(Reference<ExtConnection>) override;

	~ExtMsgKillCursors() override { delete[] cursorIDs; }

private:
	ExtMsgKillCursors(ExtMsgHeader*, const uint8_t*);
	friend struct ExtMsg::Factory<ExtMsgKillCursors>;
};

struct DocInserter: IOplogInserter, ReferenceCounted<DocInserter>, FastAllocated<DocInserter> {
	Reference<ExtChangeWatcher> watcher;

	void addref() override { ReferenceCounted<DocInserter>::addref(); }
	void delref() override { ReferenceCounted<DocInserter>::delref(); }
	
	DocInserter(Reference<ExtChangeWatcher> watcher): watcher(watcher) {};
	Future<Reference<IReadWriteContext>> insert(Reference<CollectionContext> cx, bson::BSONObj obj) override;
};

Reference<Plan> planQuery(Reference<UnboundCollectionContext> cx, const bson::BSONObj& query);
std::vector<std::string> staticValidateUpdateObject(bson::BSONObj update, bool multi, bool upsert);
ACTOR Future<WriteCmdResult> attemptIndexInsertion(bson::BSONObj firstDoc,
                                                   Reference<ExtConnection> ec,
                                                   Reference<DocTransaction> tr,
                                                   Namespace ns);
ACTOR Future<WriteCmdResult> doInsertCmd(Namespace ns,
                                         std::list<bson::BSONObj>* documents,
                                         Reference<ExtConnection> ec);
ACTOR Future<WriteCmdResult> doDeleteCmd(Namespace ns,
                                         bool ordered,
                                         std::vector<bson::BSONObj>* selectors,
                                         Reference<ExtConnection> ec);
ACTOR Future<WriteCmdResult> doUpdateCmd(Namespace ns,
                                         bool ordered,
                                         std::vector<ExtUpdateCmd>* updateCmds,
                                         Reference<ExtConnection> ec);

// FIXME: these don't really belong here either
Reference<IUpdateOp> operatorUpdate(bson::BSONObj const& msgUpdate);
Reference<IUpdateOp> replaceUpdate(bson::BSONObj const& replaceWith);
Reference<IInsertOp> simpleUpsert(bson::BSONObj const& selector, bson::BSONObj const& update);
Reference<IInsertOp> operatorUpsert(bson::BSONObj const& selector, bson::BSONObj const& update);

#endif /* _EXT_MSG_ACTOR_H_ */
