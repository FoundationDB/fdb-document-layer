/*
 * ExtCmd.actor.cpp
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
 *
 * MongoDB is a registered trademark of MongoDB, Inc.
 */

#include "bson.h"
#include "ordering.h"

#include "ExtCmd.h"
#include "ExtMsg.actor.h"
#include "ExtUtil.actor.h"

#include "QLPlan.actor.h"
#include "QLProjection.actor.h"

#ifndef WIN32
#include "gitVersion.h"
#endif
#include "flow/actorcompiler.h" // This must be the last #include.

using namespace FDB;

extern const char* getGitVersion();
extern const char* getFlowGitVersion();

ACTOR static Future<std::pair<int, int>> dropIndexMatching(Reference<DocTransaction> tr,
                                                           Namespace ns,
                                                           std::string field,
                                                           DataValue value,
                                                           Reference<MetadataManager> mm) {
	state Reference<UnboundCollectionContext> indexesCollection = wait(mm->indexesCollection(tr, ns.first));
	state Reference<UnboundCollectionContext> targetedCollection = wait(mm->getUnboundCollectionContext(tr, ns));
	state Reference<Plan> indexesPlan = getIndexesForCollectionPlan(indexesCollection, ns);
	state std::vector<bson::BSONObj> indexes = wait(getIndexesTransactionally(indexesPlan, tr));
	state int count = 0;
	state bool any = false;
	state bson::BSONObj matchingIndexObj;
	state Reference<QueryContext> matchingIndex;
	state std::string matchingName;

	// This next ugly block of code needs to stick around for now because the unconventional way in which we are
	// representing the 'key' field in an index document (which was done because this is one place we can't get away
	// with ignoring the ordering of documents) breaks the conventional query pipeline. So we need this specialized
	// handling.

	for (const auto& indexObj : indexes) {
		count++;
		if (value.getBSONType() == bson::BSONType::String) {
			if (value.getString() == indexObj.getStringField(field.c_str())) {
				any = true;
				matchingIndexObj = indexObj;
				matchingIndex = indexesCollection->bindCollectionContext(tr)->cx->getSubContext(
				    DataValue(indexObj.getField(DocLayerConstants::ID_FIELD)).encode_key_part());
				matchingName = std::string(indexObj.getStringField(DocLayerConstants::NAME_FIELD));
			}
		} else if (value.getBSONType() == bson::BSONType::Object) {
			if (value.getPackedObject().woCompare(indexObj.getObjectField(field.c_str())) == 0) {
				any = true;
				matchingIndexObj = indexObj;
				matchingIndex = indexesCollection->bindCollectionContext(tr)->cx->getSubContext(
				    DataValue(indexObj.getField(DocLayerConstants::ID_FIELD)).encode_key_part());
				matchingName = std::string(indexObj.getStringField(DocLayerConstants::NAME_FIELD));
			}
		}
	}
	if (!any)
		return std::make_pair(count, 0);

	matchingIndex->clearDescendants();
	matchingIndex->clearRoot();

	TraceEvent(SevInfo, "BumpMetadataVersion")
	    .detail("reason", "dropIndex")
	    .detail("ns", fullCollNameToString(ns))
	    .detail("index", matchingIndexObj.toString());

	wait(matchingIndex->commitChanges());

	Key indexKey = targetedCollection->getIndexesSubspace().withSuffix(encodeMaybeDotted(matchingName));
	tr->tr->clear(FDB::KeyRangeRef(indexKey, strinc(indexKey)));

	targetedCollection->bindCollectionContext(tr)->bumpMetadataVersion();

	return std::make_pair(count, 1);
}

ACTOR static Future<Void> Internal_doDropDatabase(Reference<DocTransaction> tr,
                                                  Reference<ExtMsgQuery> query,
                                                  Reference<DirectorySubspace> rootDirectory) {
	wait(success(rootDirectory->removeIfExists(tr->tr, {StringRef(query->ns.first)})));
	return Void();
}

ACTOR static Future<Reference<ExtMsgReply>> doDropDatabase(Reference<ExtConnection> ec,
                                                           Reference<ExtMsgQuery> query,
                                                           Reference<ExtMsgReply> reply) {
	try {
		// No need to wait on lastWrite. The ranges we write ensure that this will conflict with anything it needs to
		// conflict with.
		wait(runRYWTransaction(ec->docLayer->database,
		                       [=](Reference<DocTransaction> tr) {
			                       return Internal_doDropDatabase(tr, query, ec->docLayer->rootDirectory);
		                       },
		                       ec->options.retryLimit, ec->options.timeoutMillies));

		reply->addDocument(BSON("ok" << 1.0));
		return reply;
	} catch (Error& e) {
		// clang-format off
		reply->addDocument(BSON("ok" << 1.0 <<
		                        "err" << e.what() <<
		                        "code" << e.code()));
		// clang-format on
		return reply;
	}
}

struct DropDatabaseCmd {
	static const char* name;
	static Future<Reference<ExtMsgReply>> call(Reference<ExtConnection> ec,
	                                           Reference<ExtMsgQuery> query,
	                                           Reference<ExtMsgReply> reply) {
		return doDropDatabase(ec, query, reply);
	}
};
REGISTER_CMD(DropDatabaseCmd, "dropdatabase");

struct WhatsmyuriCmd {
	static const char* name;
	static Future<Reference<ExtMsgReply>> call(Reference<ExtConnection> nmc,
	                                           Reference<ExtMsgQuery> query,
	                                           Reference<ExtMsgReply> reply) {
		// clang-format off
		reply->addDocument(BSON("you" << nmc->bc->getPeerAddress().toString() <<
		                        "ok" << 1.0));
		// clang-format on
		return Future<Reference<ExtMsgReply>>(reply);
	}
};
REGISTER_CMD(WhatsmyuriCmd, "whatsmyuri");

struct GetCmdLineOptsCmd {
	static const char* name;
	static Future<Reference<ExtMsgReply>> call(Reference<ExtConnection> nmc,
	                                           Reference<ExtMsgQuery> query,
	                                           Reference<ExtMsgReply> reply) {
		// clang-format off
		reply->addDocument(BSON("argv" << BSON_ARRAY("fdbdoc") <<
		                        "ok" << 1.0));
		// clang-format on

		return Future<Reference<ExtMsgReply>>(reply);
	}
};
REGISTER_CMD(GetCmdLineOptsCmd, "getcmdlineopts");

bson::NullLabeler BSONNULL;

struct GetlasterrorCmd {
	static const char* name;
	ACTOR static Future<Reference<ExtMsgReply>> call(Reference<ExtConnection> ec,
	                                                 Reference<ExtMsgQuery> query,
	                                                 Reference<ExtMsgReply> reply) {
		state bson::BSONObjBuilder bob;

		Future<WriteResult> lastWrite = ec->lastWrite;

		bob.appendNumber("connectionId", (long long)ec->connectionId);

		try {
			WriteResult res = wait(lastWrite);
			bob.appendNumber("n", (long long)res.n); //< FIXME: ???
			bob << "err" << BSONNULL << "ok" << 1.0;
			if (!res.upsertedOIDList.empty()) {
				bob.appendElements(DataValue::decode_key_part(res.upsertedOIDList[0]).wrap("upserted"));
			}
			if (res.type == WriteType::UPDATE) {
				bob << "updatedExisting" << (res.nModified > 0 && res.upsertedOIDList.empty());
			}
		} catch (Error& e) {
			bob.append("err", e.what());
			bob.append("code", e.code());
			bob.appendNumber("n", (long long)0);
			bob.append("ok", 1.0);
		}

		reply->addDocument(bob.obj());
		reply->setResponseFlags(8);

		ec->lastWrite = WriteResult();

		return reply;
	}
};
REGISTER_CMD(GetlasterrorCmd, "getlasterror");

struct FakeErrorCmd {
	static const char* name;
	static Future<Reference<ExtMsgReply>> call(Reference<ExtConnection> nmc,
	                                           Reference<ExtMsgQuery> query,
	                                           Reference<ExtMsgReply> reply) {
		throw operation_failed();
	}
};
REGISTER_CMD(FakeErrorCmd, "fakeerror");

struct GetLogCmd {
	static const char* name;
	static Future<Reference<ExtMsgReply>> call(Reference<ExtConnection> nmc,
	                                           Reference<ExtMsgQuery> query,
	                                           Reference<ExtMsgReply> reply) {
		bson::BSONObjBuilder bob;

		if (query->ns.first != "admin") {
			// clang-format off
			reply->addDocument((bob << "ok" << 0.0 <<
			                           "errmsg" << "access denied; use admin db" <<
			                           "$err" << "access denied; use admin db").obj());
			// clang-format on
			return reply;
		}

		std::string log = query->query["getLog"].String();

		if (log == "*") {
			reply->addDocument(BSON("names" << BSON_ARRAY("global"
			                                              << "startupWarnings")
			                                << "ok" << 1));
		} else if (log == "startupWarnings") {
			reply->addDocument(BSON("totalLinesWritten" << 3 << "log"
			                                            << BSON_ARRAY(""
			                                                          << "WARNING: This is not really mongodb."
			                                                          << "")
			                                            << "ok" << 1));
		} else if (log == "global") {
			reply->addDocument(BSON("totalLinesWritten" << 1 << "log" << BSON_ARRAY("foo") << "ok" << 1.0));
		}

		return reply;
	}
};
REGISTER_CMD(GetLogCmd, "getlog");

struct ServerStatusCmd {
	static const char* name;
	static Future<Reference<ExtMsgReply>> call(Reference<ExtConnection> nmc,
	                                           Reference<ExtMsgQuery> query,
	                                           Reference<ExtMsgReply> reply) {
		// bson::BSONObjBuilder bob;

		// bob.append( "opcounters" << BSON( "query" << queries ) );
		// bob.append( "ok", 1 );

		// reply->addDocument( bob.obj() );

		// reply->addDocument( BSON( "opcounters" << BSON( "query" << queries ) << "ok" << 1 ) );
		reply->addDocument(BSON("ok" << 1.0));

		return reply;
	}
};
REGISTER_CMD(ServerStatusCmd, "serverstatus");

struct ReplSetGetStatusCmd {
	static const char* name;
	static Future<Reference<ExtMsgReply>> call(Reference<ExtConnection> nmc,
	                                           Reference<ExtMsgQuery> query,
	                                           Reference<ExtMsgReply> reply) {
		bson::BSONObjBuilder bob;

		// FIXME: what do we really want to report here?
		bob.append("ok", 0.0);
		bob.append("errmsg", "not really talking to mongodb");
		bob.append("$err", "not really talking to mongodb");

		reply->addDocument(bob.obj());

		return reply;
	}
};
REGISTER_CMD(ReplSetGetStatusCmd, "replsetgetstatus");

struct PingCmd {
	static const char* name;
	static Future<Reference<ExtMsgReply>> call(Reference<ExtConnection> nmc,
	                                           Reference<ExtMsgQuery> query,
	                                           Reference<ExtMsgReply> reply) {
		reply->addDocument(BSON("ok" << 1.0));

		return reply;
	}
};
REGISTER_CMD(PingCmd, "ping");

struct IsMasterCmd {
	static const char* name;
	static Future<Reference<ExtMsgReply>> call(Reference<ExtConnection> nmc,
	                                           Reference<ExtMsgQuery> query,
	                                           Reference<ExtMsgReply> reply) {
		reply->addDocument(BSON(
		    // clang-format off
		    "ismaster" << true <<
		    "maxBsonObjectSize" << 16777216 <<
		    "maxMessageSizeBytes" << 48000000 <<
		    // FIXME: BM: Just like other fields, going with defaults. We should revisit these numbers.
		    "maxWriteBatchSize" << 1000 <<
		    "localTime" << bson::Date_t(timer() * 1000) <<
		    "minWireVersion" << EXT_MIN_WIRE_VERSION <<
		    "maxWireVersion" << EXT_MAX_WIRE_VERSION <<
		    "ok" << 1.0
		    // clang-format on
		    ));

		return reply;
	}
};
REGISTER_CMD(IsMasterCmd, "ismaster");

struct GetNonceCmd {
	static const char* name;
	static Future<Reference<ExtMsgReply>> call(Reference<ExtConnection> nmc,
	                                           Reference<ExtMsgQuery> query,
	                                           Reference<ExtMsgReply> reply) {
		reply->addDocument(BSON("nonce"
		                        << "0000000000000000"
		                        << "ok" << 1.0));

		return reply;
	}
};

REGISTER_CMD(GetNonceCmd, "getnonce");

ACTOR static Future<int> internal_doDropIndexesActor(Reference<DocTransaction> tr,
                                                     Namespace ns,
                                                     Reference<MetadataManager> mm) {
	state Reference<UnboundCollectionContext> indexesCollection = wait(mm->indexesCollection(tr, ns.first));
	state Reference<Plan> plan = getIndexesForCollectionPlan(indexesCollection, ns);
	state Reference<UnboundCollectionContext> unbound = wait(mm->getUnboundCollectionContext(tr, ns));
	plan = flushChanges(deletePlan(plan, indexesCollection, std::numeric_limits<int64_t>::max()));

	state int64_t count = wait(executeUntilCompletionTransactionally(plan, tr));

	Key indexes = unbound->getIndexesSubspace();
	tr->tr->clear(FDB::KeyRangeRef(indexes, strinc(indexes)));
	unbound->bindCollectionContext(tr)->bumpMetadataVersion();
	TraceEvent(SevInfo, "BumpMetadataVersion")
	    .detail("reason", "dropAllIndexes")
	    .detail("ns", fullCollNameToString(ns));

	return count;
}

ACTOR static Future<Void> Internal_doDropCollection(Reference<DocTransaction> tr,
                                                    Reference<ExtMsgQuery> query,
                                                    Reference<MetadataManager> mm) {
	state Reference<UnboundCollectionContext> unbound = wait(mm->getUnboundCollectionContext(tr, query->ns));
	wait(success(internal_doDropIndexesActor(tr, query->ns, mm)));
	wait(unbound->collectionDirectory->remove(tr->tr));
	return Void();
}

ACTOR static Future<Reference<ExtMsgReply>> doDropCollection(Reference<ExtConnection> ec,
                                                             Reference<ExtMsgQuery> query,
                                                             Reference<ExtMsgReply> reply) {
	try {
		// No need to wait on lastWrite in either case. The ranges we write ensure that this will conflict with
		// anything it needs to conflict with.
		wait(runRYWTransaction(
		    ec->docLayer->database,
		    [=](Reference<DocTransaction> tr) { return Internal_doDropCollection(tr, query, ec->mm); },
		    ec->options.retryLimit, ec->options.timeoutMillies));

		reply->addDocument(BSON("ok" << 1.0));
		return reply;
	} catch (Error& e) {
		reply->addDocument(BSON("ok" << 1.0 << "err" << e.what() << "code" << e.code()));
		return reply;
	}
}

struct DropCollectionCmd {
	static const char* name;
	static Future<Reference<ExtMsgReply>> call(Reference<ExtConnection> ec,
	                                           Reference<ExtMsgQuery> query,
	                                           Reference<ExtMsgReply> reply) {
		return doDropCollection(ec, query, reply);
	}
};
REGISTER_CMD(DropCollectionCmd, "drop");

ACTOR static Future<Void> internal_doRenameCollectionIndexesActor(Reference<DocTransaction> tr,
                                                                  Namespace ns,
                                                                  Reference<MetadataManager> mm,
                                                                  std::string destinationCollection) {
	state Reference<UnboundCollectionContext> indexesCollection = wait(mm->indexesCollection(tr, ns.first));
	state Reference<Plan> indexesPlan = wait(getIndexesForCollectionPlan(ns, tr, mm));
	state std::vector<bson::BSONObj> indexes = wait(getIndexesTransactionally(indexesPlan, tr));
	state Reference<QueryContext> matchingIndex;

	for (const auto& indexObj : indexes) {
		matchingIndex = indexesCollection->bindCollectionContext(tr)->cx->getSubContext(
		    DataValue(indexObj.getField(DocLayerConstants::ID_FIELD)).encode_key_part());
		matchingIndex->set(DataValue(DocLayerConstants::NS_FIELD, DVTypeCode::STRING).encode_key_part(),
		                   DataValue(ns.first + "." + destinationCollection, DVTypeCode::STRING).encode_value());
		wait(matchingIndex->commitChanges());
	}

	state Reference<UnboundCollectionContext> unbound = wait(mm->getUnboundCollectionContext(tr, ns));
	unbound->bindCollectionContext(tr)->bumpMetadataVersion();
	TraceEvent(SevInfo, "BumpMetadataVersion")
	    .detail("reason", "renameCollection")
	    .detail("ns", fullCollNameToString(ns));
	return Void();
}

ACTOR static Future<Void> Internal_doRenameCollection(Reference<DocTransaction> tr,
                                                      Reference<ExtMsgQuery> query,
                                                      Reference<ExtConnection> ec) {
	state Namespace ns;
	ns.first = upOneLevel(query->ns.second);
	state std::string sourceCollection = getLastPart(query->ns.second);
	state std::string destinationCollection = getLastPart(query->query.getStringField("to"));
	state bool dropTarget = query->query.getBoolField("dropTarget");

	state Future<bool> exists_destinationCollectionF =
	    ec->docLayer->rootDirectory->exists(tr->tr, {StringRef(ns.first), StringRef(destinationCollection)});
	state Future<bool> exists_sourceCollectionF =
	    ec->docLayer->rootDirectory->exists(tr->tr, {StringRef(ns.first), StringRef(sourceCollection)});
	wait(success(exists_destinationCollectionF) && success(exists_sourceCollectionF));

	if (exists_sourceCollectionF.get()) {
		if (exists_destinationCollectionF.get()) {
			if (dropTarget) {
				if (sourceCollection == destinationCollection) {
					throw old_and_new_collection_name_cannot_be_same();
				}
				ns.second = destinationCollection;
				state Reference<UnboundCollectionContext> unbound = wait(ec->mm->getUnboundCollectionContext(tr, ns));
				wait(success(internal_doDropIndexesActor(tr, ns, ec->mm)));
				wait(unbound->collectionDirectory->remove(tr->tr));
			} else {
				throw collection_name_already_exist();
			}
		}
	} else {
		throw collection_name_does_not_exist();
	}

	wait(success(ec->docLayer->rootDirectory->move(tr->tr, {StringRef(ns.first), StringRef(sourceCollection)},
	                                               {StringRef(ns.first), StringRef(destinationCollection)})));
	ns.second = sourceCollection;
	wait(success(internal_doRenameCollectionIndexesActor(tr, ns, ec->mm, destinationCollection)));
	return Void();
}

ACTOR static Future<Reference<ExtMsgReply>> doRenameCollection(Reference<ExtConnection> ec,
                                                               Reference<ExtMsgQuery> query,
                                                               Reference<ExtMsgReply> reply) {
	try {
		// No need to wait on lastWrite in either case. The ranges we write ensure that this will conflict with
		// anything it needs to conflict with.
		wait(runRYWTransaction(ec->docLayer->database,
		                       [=](Reference<DocTransaction> tr) { return Internal_doRenameCollection(tr, query, ec); },
		                       ec->options.retryLimit, ec->options.timeoutMillies));
		reply->addDocument(BSON("ok" << 1.0));
		return reply;
	} catch (Error& e) {
		reply->addDocument(BSON("ok" << 0.0 << "errmsg" << e.what() << "code" << e.code()));
		return reply;
	}
}

struct RenameCollectionCmd {
	static const char* name;
	static Future<Reference<ExtMsgReply>> call(Reference<ExtConnection> ec,
	                                           Reference<ExtMsgQuery> query,
	                                           Reference<ExtMsgReply> reply) {
		return doRenameCollection(ec, query, reply);
	}
};
REGISTER_CMD(RenameCollectionCmd, "renamecollection");

ACTOR static Future<Reference<ExtMsgReply>> getStreamCount(Reference<ExtConnection> ec,
                                                           Reference<ExtMsgQuery> query,
                                                           Reference<ExtMsgReply> reply) {
	try {
		state Reference<DocTransaction> dtr = ec->getOperationTransaction();
		Reference<UnboundCollectionContext> cx = wait(ec->mm->getUnboundCollectionContext(dtr, query->ns, true));
		Reference<Plan> plan = planQuery(cx, query->query.getObjectField(DocLayerConstants::QUERY_FIELD));
		plan = ec->wrapOperationPlan(plan, true, cx);

		// Rather than use a SkipPlan, we subtract "skipped" documents from the final count (and add them to any limit)
		state int64_t skip = query->query.hasField("skip") ? query->query.getField("skip").numberLong() : 0;
		state int64_t limitPlusSkip = query->query.hasField("limit")
		                                  ? skip + query->query.getField("limit").numberLong()
		                                  : std::numeric_limits<int64_t>::max();
		// SOMEDAY: We can optimize this by only counting the first limitPlusSkip documents
		int64_t count = wait(executeUntilCompletionTransactionally(plan, dtr));

		reply->addDocument(BSON("n" << (double)std::max<int64_t>(count - skip, 0) << "ok" << 1.0));
		return reply;
	} catch (Error& e) {
		reply->addDocument(BSON("$err" << e.what() << "code" << e.code() << "ok" << 1.0));
		reply->setResponseFlags(2);
		return reply;
	}
}

struct GetCountCmd {
	static const char* name;
	static Future<Reference<ExtMsgReply>> call(Reference<ExtConnection> ec,
	                                           Reference<ExtMsgQuery> query,
	                                           Reference<ExtMsgReply> reply) {
		return getStreamCount(ec, query, reply);
	}
};
REGISTER_CMD(GetCountCmd, "count");

ACTOR static Future<Reference<ExtMsgReply>> doFindAndModify(Reference<ExtConnection> ec,
                                                            Reference<ExtMsgQuery> query,
                                                            Reference<ExtMsgReply> reply) {
	try {
		state bool issort = query->query.hasField("sort");
		state bool isremove = query->query.hasField("remove") && query->query.getField("remove").trueValue();
		state bool isnew = query->query.hasField("new") && query->query.getField("new").trueValue() && !(isremove);
		state bool isupdate = query->query.hasField("update");
		state bool isupsert =
		    isupdate && query->query.hasField("upsert") && query->query.getField("upsert").trueValue();
		state bson::BSONObj selector = query->query.hasField(DocLayerConstants::QUERY_FIELD)
		                                   ? query->query.getObjectField(DocLayerConstants::QUERY_FIELD).getOwned()
		                                   : bson::BSONObj();
		state Reference<Projection> projection = query->query.hasField("fields")
		                                             ? parseProjection(query->query.getObjectField("fields").getOwned())
		                                             : Reference<Projection>(new Projection());
		state Optional<bson::BSONObj> ordering =
		    issort ? query->query.getObjectField("sort") : Optional<bson::BSONObj>();

		state bson::BSONObj updateDoc;
		state bson::BSONObj retval;

		if (issort) {
			throw not_implemented();
		}

		if (isupdate) {
			updateDoc = query->query.getObjectField("update");
		}
		state bool isoperatorUpdate = isupdate && hasOperatorFieldnames(updateDoc);

		if ((isremove && isupdate) || !(isremove || isupdate))
			throw bad_find_and_modify();

		if (isoperatorUpdate) {
			staticValidateUpdateObject(updateDoc, false, isupsert);
		}

		state Reference<DocTransaction> tr = ec->getOperationTransaction();
		state Reference<UnboundCollectionContext> ucx = wait(ec->mm->getUnboundCollectionContext(tr, query->ns));

		state Reference<IUpdateOp> updater;
		state Reference<IInsertOp> upserter;
		if (isremove)
			updater = ref(new DeleteDocument());
		else if (isoperatorUpdate)
			updater = operatorUpdate(updateDoc);
		else
			updater = replaceUpdate(updateDoc);
		if (isupsert) {
			if (isoperatorUpdate)
				upserter = operatorUpsert(selector, updateDoc);
			else
				upserter = simpleUpsert(selector, updateDoc);
		}

		state Reference<Plan> plan = planQuery(ucx, selector);
		plan = ref(new FindAndModifyPlan(plan, updater, upserter, projection, ordering, isnew, ucx,
		                                 ref(new DocInserter(ec->watcher)), ec->docLayer->database, ec->mm));

		state std::pair<int64_t, Reference<ScanReturnedContext>> pair =
		    wait(executeUntilCompletionAndReturnLastTransactionally(plan, tr));
		state int64_t i = pair.first;
		state bool returnedUpserted = i && pair.second->scanId() == -1;

		if (i) {
			retval = pair.second->toDataValue().get().getPackedObject().getOwned();
		}

		bson::BSONObjBuilder bob;
		if (!isnew) {
			if (i) {
				bob.appendObject("value", retval.objdata());
			} else if (!issort) {
				bob.appendNull("value");
			} else {
				bob.appendObject("value", bson::BSONObj().objdata());
			}
		} else {
			if (i) {
				bob.appendObject("value", retval.objdata());
			} else {
				bob.appendNull("value");
			}
		}

		bson::BSONObjBuilder lastErrorObject;
		lastErrorObject.appendBool("updatedExisting", i && !returnedUpserted);
		lastErrorObject.appendNumber("n", (int)i);
		if (returnedUpserted)
			lastErrorObject.append(retval.getField(DocLayerConstants::ID_FIELD));
		bson::BSONObj lastErrorObj = lastErrorObject.obj().getOwned();
		bob.appendObject("lastErrorObject", lastErrorObj.objdata());
		bob.appendNumber("ok", 1.0);
		bson::BSONObj replyObj = bob.obj().getOwned();
		reply->addDocument(replyObj);
	} catch (Error& e) {
		// clang-format off
		reply->addDocument(BSON("errmsg" << e.what() <<
		                        "$err" << e.what() <<
		                        "code" << e.code() <<
		                        "ok" << 1.0));
		// clang-format on
	}

	return reply;
}

struct FindAndModifyCmd {
	static const char* name;
	static Future<Reference<ExtMsgReply>> call(Reference<ExtConnection> ec,
	                                           Reference<ExtMsgQuery> query,
	                                           Reference<ExtMsgReply> reply) {
		return doFindAndModify(ec, query, reply);
	}
};
REGISTER_CMD(FindAndModifyCmd, "findandmodify");

ACTOR static Future<Reference<ExtMsgReply>> doDropIndexesActor(Reference<ExtConnection> ec,
                                                               Reference<ExtMsgQuery> query,
                                                               Reference<ExtMsgReply> reply) {
	state int dropped;

	try {
		if (query->query.hasField("index")) {
			bson::BSONElement el = query->query.getField("index");
			if (el.type() == bson::BSONType::String) {
				if (el.String() == "*") {
					// No need to wait on lastWrite in either case. The ranges we write ensure that this will conflict
					// with anything it needs to conflict with.
					int result = wait(runRYWTransaction(ec->docLayer->database,
					                                    [=](Reference<DocTransaction> tr) {
						                                    return internal_doDropIndexesActor(tr, query->ns, ec->mm);
					                                    },
					                                    ec->options.retryLimit, ec->options.timeoutMillies));
					dropped = result;

					reply->addDocument(BSON("nIndexesWas" << dropped + 1 << "msg"
					                                      << "non-_id indexes dropped for collection"
					                                      << "ok" << 1.0));
					return reply;
				} else {
					std::pair<int, int> result = wait(runRYWTransaction(
					    ec->docLayer->database,
					    [=](Reference<DocTransaction> tr) {
						    return dropIndexMatching(tr, query->ns, DocLayerConstants::NAME_FIELD,
						                             DataValue(el.String(), DVTypeCode::STRING), ec->mm);
					    },
					    ec->options.retryLimit, ec->options.timeoutMillies));
					dropped = result.first;

					reply->addDocument(BSON("nIndexesWas" << dropped + 1 << "ok" << 1.0));
					return reply;
				}
			} else if (el.type() == bson::BSONType::Object) {
				std::pair<int, int> result =
				    wait(runRYWTransaction(ec->docLayer->database,
				                           [=](Reference<DocTransaction> tr) {
					                           return dropIndexMatching(tr, query->ns, DocLayerConstants::KEY_FIELD,
					                                                    DataValue(el.Obj()), ec->mm);
				                           },
				                           ec->options.retryLimit, ec->options.timeoutMillies));
				dropped = result.first;

				reply->addDocument(BSON("nIndexesWas" << dropped + 1 << "ok" << 1.0));
				return reply;
			} else {
				// clang-format off
				reply->addDocument(BSON("ok" << 0.0 <<
				"$err" << "'index' must be a string or an object" <<
				"errmsg" << "'index' must be a string or an object"));
				// clang-format on
				return reply;
			}
		} else {
			int result = wait(runRYWTransaction(
			    ec->docLayer->database,
			    [=](Reference<DocTransaction> tr) { return internal_doDropIndexesActor(tr, query->ns, ec->mm); },
			    ec->options.retryLimit, ec->options.timeoutMillies));
			dropped = result;

			// clang-format off
			reply->addDocument(BSON("nIndexesWas" << dropped + 1 <<
			                        "msg" << "non-_id indexes dropped for collection" <<
			                        "ok" << 1.0));
			// clang-format on
			return reply;
		}
	} catch (Error& e) {
		// clang-format off
		reply->addDocument(BSON("ok" << 0.0 <<
		                        "err" << e.what() <<
		                        "code" << e.code()));
		// clang-format on
		return reply;
	}
}

struct DropIndexesCmd {
	static const char* name;
	static Future<Reference<ExtMsgReply>> call(Reference<ExtConnection> nmc,
	                                           Reference<ExtMsgQuery> query,
	                                           Reference<ExtMsgReply> reply) {
		return doDropIndexesActor(nmc, query, reply);
	}
};
REGISTER_CMD(DropIndexesCmd, "dropindexes");

// Hooray for "compatibility"
struct DeleteIndexesCmd {
	static const char* name;
	static Future<Reference<ExtMsgReply>> call(Reference<ExtConnection> nmc,
	                                           Reference<ExtMsgQuery> query,
	                                           Reference<ExtMsgReply> reply) {
		return doDropIndexesActor(nmc, query, reply);
	}
};
REGISTER_CMD(DeleteIndexesCmd, "deleteindexes");

ACTOR static Future<Reference<ExtMsgReply>> doCreateIndexes(Reference<ExtConnection> ec,
                                                            Reference<ExtMsgQuery> query,
                                                            Reference<ExtMsgReply> reply) {
	std::vector<Future<WriteCmdResult>> f;
	std::vector<bson::BSONElement> arr = query->query.getField("indexes").Array();
	for (auto el : arr) {
		bson::BSONObj indexDoc = el.Obj();
		f.push_back(attemptIndexInsertion(indexDoc, ec, ec->getOperationTransaction(), query->ns));
	}
	try {
		wait(waitForAll(f));
		reply->addDocument(BSON("ok" << 1.0));
	} catch (Error& e) {
		// clang-format off
		reply->addDocument(BSON("ok" << 0.0 <<
		                        "$err" << e.what() <<
		                        "errmsg" << e.what() <<
		                        "code" << e.code()));
		// clang-format on
	}
	return reply;
}

struct CreateIndexesCmd {
	static const char* name;
	static Future<Reference<ExtMsgReply>> call(Reference<ExtConnection> ec,
	                                           Reference<ExtMsgQuery> query,
	                                           Reference<ExtMsgReply> reply) {
		return doCreateIndexes(ec, query, reply);
	}
};
REGISTER_CMD(CreateIndexesCmd, "createindexes");

struct BuildInfoCmd {
	static const char* name;
	static Future<Reference<ExtMsgReply>> call(Reference<ExtConnection> ec,
	                                           Reference<ExtMsgQuery> query,
	                                           Reference<ExtMsgReply> reply) {
		// clang-format off
		reply->addDocument(BSON(
				"version"          << EXT_SERVER_VERSION       <<
				"gitVersion"       << getGitVersion()          <<
				"OpenSSLVersion"   << ""                       <<
				"sysInfo"          << "<string>"               <<
				"loaderFlags"      << "<string>"               <<
				"compilerFlags"    << "<string>"               <<
				"allocator"        << "<string>"               <<
				"versionArray"     << EXT_SERVER_VERSION_ARRAY <<
				"javascriptEngine" << "<string>"               <<
				"bits"             << 64                       <<
				"debug"            << false                    <<
				"maxBsonObjectSize"<< 16777216                 <<
				"ok"               << 1.0
				));
		// clang-format on
		return reply;
	}
};
REGISTER_CMD(BuildInfoCmd, "buildinfo");

ACTOR static Future<Reference<ExtMsgReply>> listDatabases(Reference<DocTransaction> tr,
                                                          Reference<ExtMsgReply> reply,
                                                          Reference<DirectorySubspace> rootDirectory) {
	state bson::BSONObjBuilder bob;
	state bson::BSONArrayBuilder bab;
	state std::string dbName;
	state Standalone<VectorRef<StringRef>> dbs = wait(rootDirectory->list(tr->tr));

	for (auto db : dbs) {
		dbName = db.toString();
		Standalone<VectorRef<StringRef>> colls = wait(rootDirectory->list(tr->tr, {db}));
		bool empty = colls.empty();
		bab.append(
		    BSON(DocLayerConstants::NAME_FIELD << dbName << "sizeOnDisk" << (empty ? 0 : 1000000) << "empty" << empty));
	}

	bob.appendArray("databases", bab.arr());
	bob.append("totalSize", -1);
	bob.append("ok", 1.0);
	reply->addDocument(bob.obj());

	return reply;
}

struct ListDatabasesCmd {
	static const char* name;
	static Future<Reference<ExtMsgReply>> call(Reference<ExtConnection> ec,
	                                           Reference<ExtMsgQuery> query,
	                                           Reference<ExtMsgReply> reply) {
		return listDatabases(ec->getOperationTransaction(), reply, ec->docLayer->rootDirectory);
	}
};
REGISTER_CMD(ListDatabasesCmd, "listdatabases");

ACTOR static Future<Reference<ExtMsgReply>> getDBStats(Reference<ExtConnection> ec,
                                                       Reference<ExtMsgQuery> query,
                                                       Reference<ExtMsgReply> reply) {
	state Reference<DocTransaction> tr = ec->getOperationTransaction();
	state Standalone<VectorRef<StringRef>> collections =
	    wait(ec->docLayer->rootDirectory->list(tr->tr, {StringRef(query->ns.first)}));
	state Reference<UnboundCollectionContext> cx = wait(ec->mm->indexesCollection(tr, query->ns.first));
	state int64_t indexes = wait(executeUntilCompletionTransactionally(ref(new TableScanPlan(cx)), tr));
	reply->addDocument(BSON("db" << query->ns.first << "collections" << collections.size() << "indexes"
	                             << (int)(indexes + collections.size()) << "ok" << 1.0));
	return reply;
}

// FIXME: Add more to this command
struct DBStatsCmd {
	static const char* name;
	static Future<Reference<ExtMsgReply>> call(Reference<ExtConnection> ec,
	                                           Reference<ExtMsgQuery> query,
	                                           Reference<ExtMsgReply> reply) {
		return getDBStats(ec, query, reply);
	}
};
REGISTER_CMD(DBStatsCmd, "dbstats");

// FIXME: Add more here too
ACTOR static Future<Reference<ExtMsgReply>> getCollectionStats(Reference<ExtConnection> ec,
                                                               Reference<ExtMsgQuery> query,
                                                               Reference<ExtMsgReply> reply) {
	state Reference<DocTransaction> tr = ec->getOperationTransaction();
	state Reference<Plan> plan = wait(getIndexesForCollectionPlan(query->ns, tr, ec->mm));
	state int64_t indexesCount = wait(executeUntilCompletionTransactionally(plan, tr));
	reply->addDocument(BSON(DocLayerConstants::NS_FIELD << query->ns.first + "." + query->ns.second << "nindexes"
	                                                    << (int)indexesCount + 1 << "ok" << 1.0));
	return reply;
}

struct CollectionStatsCmd {
	static const char* name;
	static Future<Reference<ExtMsgReply>> call(Reference<ExtConnection> ec,
	                                           Reference<ExtMsgQuery> query,
	                                           Reference<ExtMsgReply> reply) {
		return getCollectionStats(ec, query, reply);
	}
};
REGISTER_CMD(CollectionStatsCmd, "collstats");

ACTOR static Future<Void> Internal_doCreateCollection(Reference<DocTransaction> tr,
                                                      Reference<ExtMsgQuery> query,
                                                      Reference<MetadataManager> mm) {
	state Reference<UnboundCollectionContext> unbound = wait(mm->getUnboundCollectionContext(tr, query->ns));
	return Void();
}

ACTOR static Future<Reference<ExtMsgReply>> doCreateCollection(Reference<ExtConnection> ec,
                                                               Reference<ExtMsgQuery> query,
                                                               Reference<ExtMsgReply> reply) {
	try {
		if (query->query.getBoolField("capped") || query->query.hasField("storageEngine")) {
			TraceEvent(SevWarn, "CreateUnsupportedOption").detail("query", query->toString());
			throw unsupported_cmd_option();
		}

		wait(runRYWTransaction(
		    ec->docLayer->database,
		    [=](Reference<DocTransaction> tr) { return Internal_doCreateCollection(tr, query, ec->mm); },
		    ec->options.retryLimit, ec->options.timeoutMillies));

		reply->addDocument(BSON("ok" << 1.0));
		return reply;
	} catch (Error& e) {
		reply->addDocument(BSON("ok" << 1.0 << "err" << e.what() << "code" << e.code()));
		return reply;
	}
}

struct CreateCollectionCmd {
	static const char* name;
	static Future<Reference<ExtMsgReply>> call(Reference<ExtConnection> ec,
	                                           Reference<ExtMsgQuery> query,
	                                           Reference<ExtMsgReply> reply) {
		return doCreateCollection(ec, query, reply);
	}
};
REGISTER_CMD(CreateCollectionCmd, "create");

ACTOR static Future<Reference<ExtMsgReply>> doGetKVStatusActor(Reference<ExtConnection> ec,
                                                               Reference<ExtMsgReply> reply) {
	try {
		state Reference<DocTransaction> dtr = ec->getOperationTransaction();
		state StringRef statusKey = LiteralStringRef("\xff\xff/status/json");
		Optional<FDB::FDBStandalone<StringRef>> status = wait(dtr->tr->get(statusKey));
		if (status.present()) {
			const DataValue vv = DataValue::decode_value(status.get());
			reply->addDocument(
			    BSON("ok" << 1.0 << "jsonValue"
			              << vv.encode_value().toString()) /*bson::fromjson(vv.encode_value().c_str())*/);
		}
	} catch (Error& e) {
		reply->addDocument(
		    BSON("ok" << 1.0 << "err"
		              << "This command is supported only with version 3.0 and above of the KV Store, if you are using "
		                 "an older FDB version please use the fdbcli utility to check its status."
		              << "code" << e.code()));
	}

	return reply;
}

struct GetKVStatusCmd {
	static const char* name;
	static Future<Reference<ExtMsgReply>> call(Reference<ExtConnection> ec,
	                                           Reference<ExtMsgQuery> query,
	                                           Reference<ExtMsgReply> reply) {
		return doGetKVStatusActor(ec, reply);
	}
};
REGISTER_CMD(GetKVStatusCmd, "getkvstatus");

struct GetDocLayerVersionCmd {
	static const char* name;
	static Future<Reference<ExtMsgReply>> call(Reference<ExtConnection> ec,
	                                           Reference<ExtMsgQuery> query,
	                                           Reference<ExtMsgReply> reply) {
		// clang-format off
		reply->addDocument(BSON(
			"ok" << 1 <<
			"packageVersion" << FDB_DOC_VT_VERSION <<
			"sourceVersion" << getGitVersion() <<
			"flowSourceVersion" << getFlowGitVersion()
			));
		// clang-format on
		return reply;
	}
};
REGISTER_CMD(GetDocLayerVersionCmd, "getdoclayerversion");

struct BuggifyKnobsCmd {
	static const char* name;
	static Future<Reference<ExtMsgReply>> call(Reference<ExtConnection> ec,
	                                           Reference<ExtMsgQuery> query,
	                                           Reference<ExtMsgReply> reply) {
		enableBuggify(true);
		const bool bEnable = query->query.getBoolField(name);
		auto* docLayerKnobs = new DocLayerKnobs(bEnable);
		delete DOCLAYER_KNOBS;
		DOCLAYER_KNOBS = docLayerKnobs;
		reply->addDocument(DOCLAYER_KNOBS->dumpKnobs());
		return reply;
	}
};
REGISTER_CMD(BuggifyKnobsCmd, "buggifyknobs");

struct AvailableQueryOptionsCmd {
	static const char* name;
	static Future<Reference<ExtMsgReply>> call(Reference<ExtConnection> ec,
	                                           Reference<ExtMsgQuery> query,
	                                           Reference<ExtMsgReply> reply) {
		unsigned int opts = EXHAUST;
		reply->addDocument(BSON("ok" << 1.0 << "options" << opts));
		return reply;
	}
};
REGISTER_CMD(AvailableQueryOptionsCmd, "availablequeryoptions");

struct GetMemoryUsageCmd {
	static const char* name;
	static Future<Reference<ExtMsgReply>> call(Reference<ExtConnection> nmc,
	                                           Reference<ExtMsgQuery> query,
	                                           Reference<ExtMsgReply> reply) {
		reply->addDocument(BSON("process memory usage"
		                        << ((double)getMemoryUsage() / (1024 * 1024)) << "resident memory usage"
		                        << ((double)getResidentMemoryUsage() / (1024 * 1024)) << "ok" << 1));
		return reply;
	}
};
REGISTER_CMD(GetMemoryUsageCmd, "getmemoryusage");

/**
 * Write commands coming part of Query message are implemented here. Doc Layer simulates a Standalone server
 * setup. So, Write concern is not really applicable. But, other flags related to whether to wait until journal is
 * persisted or not still can be implemented in a sensible way. For now, the implementation just waits for the
 * writes fully persisted before responding back to client. This is stronger guarantee than what the client
 * has asked for with the flags.
 */
ACTOR static Future<Reference<ExtMsgReply>> insertAndReply(Reference<ExtConnection> nmc,
                                                           Reference<ExtMsgQuery> msg,
                                                           Reference<ExtMsgReply> reply);
struct InsertCmd {
	static const char* name;
	static Future<Reference<ExtMsgReply>> call(Reference<ExtConnection> nmc,
	                                           Reference<ExtMsgQuery> query,
	                                           Reference<ExtMsgReply> reply) {
		return insertAndReply(nmc, query, reply);
	}
};
REGISTER_CMD(InsertCmd, "insert");

ACTOR static Future<Reference<ExtMsgReply>> insertAndReply(Reference<ExtConnection> nmc,
                                                           Reference<ExtMsgQuery> msg,
                                                           Reference<ExtMsgReply> reply) {
	if (!msg->query.hasField(InsertCmd::name) || !msg->query.hasField("documents")) {
		TraceEvent(SevWarn, "WireBadInsert").detail("query", msg->query.toString()).suppressFor(1.0);
		throw wire_protocol_mismatch();
	}

	state std::list<bson::BSONObj> docs;
	for (auto& bsonElement : msg->query.getField("documents").Array()) {
		docs.push_back(bsonElement.Obj());
	}

	try {
		WriteCmdResult ret = wait(doInsertCmd(msg->ns, &docs, nmc));
		reply->addDocument(BSON("ok" << 1 << "n" << (long long)ret.n));
	} catch (Error& e) {
		// all or nothing. If we see any error, we assume all inserts have failed. All inserts are going under
		// one FDB transaction.
		bson::BSONArrayBuilder arrayBuilder;
		for (int i = 0; i < docs.size(); i++) {
			// clang-format off
			arrayBuilder << BSON("index" << i <<
			                     "code" << e.code() <<
			                     "$err" << e.what() <<
			                     "errmsg" << e.what());
			// clang-format on
		}
		reply->addDocument(BSON("ok" << 1 << "n" << 0 << "writeErrors" << arrayBuilder.arr()));
	}

	return reply;
}

ACTOR static Future<Reference<ExtMsgReply>> deleteAndReply(Reference<ExtConnection> nmc,
                                                           Reference<ExtMsgQuery> msg,
                                                           Reference<ExtMsgReply> reply);
struct DeleteCmd {
	static const char* name;
	static Future<Reference<ExtMsgReply>> call(Reference<ExtConnection> nmc,
	                                           Reference<ExtMsgQuery> query,
	                                           Reference<ExtMsgReply> reply) {
		return deleteAndReply(nmc, query, reply);
	}
};
REGISTER_CMD(DeleteCmd, "delete");

ACTOR static Future<Reference<ExtMsgReply>> deleteAndReply(Reference<ExtConnection> nmc,
                                                           Reference<ExtMsgQuery> msg,
                                                           Reference<ExtMsgReply> reply) {
	if (!msg->query.hasField(DeleteCmd::name) || !msg->query.hasField("deletes")) {
		TraceEvent(SevWarn, "WireBadDelete").detail("query", msg->query.toString()).suppressFor(1.0);
		throw wire_protocol_mismatch();
	}

	const bool ordered = !msg->query.hasField("ordered") || msg->query.getField("ordered").Bool();
	state std::vector<bson::BSONObj> deleteQueries;
	for (auto& bsonElement : msg->query.getField("deletes").Array()) {
		auto cmd = bsonElement.Obj();
		if (!cmd.hasField("q") || !cmd.hasField("limit")) {
			TraceEvent(SevWarn, "WireBadDelete").detail("query", msg->query.toString()).suppressFor(1.0);
			throw wire_protocol_mismatch();
		}
		deleteQueries.push_back(cmd);
	}

	WriteCmdResult ret = wait(doDeleteCmd(msg->ns, ordered, &deleteQueries, nmc));

	if (ret.writeErrors.empty()) {
		reply->addDocument(BSON("ok" << 1 << "n" << (long long)ret.n));
	} else {
		// all or nothing. If we see any error, we assume all deletes have failed.
		// FIXME: BM: This may not be accurate. It's not safe to do multiple deletes under same transaction.
		// In fact, its not even entirely possible to do entire one delete in one transaction, if the query matches
		// too many documents. But, from guarantees point of view, its not worse than Mongo. So, leaving for it later.
		bson::BSONArrayBuilder arrayBuilder;
		for (auto& writeError : ret.writeErrors) {
			arrayBuilder << writeError;
		}
		// clang-format off
		reply->addDocument(BSON("ok" << 1 <<
		                        "n"  << 0 <<
		                        "writeErrors" << arrayBuilder.arr()));
		// clang-format on
	}
	return reply;
}

ACTOR static Future<Reference<ExtMsgReply>> updateAndReply(Reference<ExtConnection> nmc,
                                                           Reference<ExtMsgQuery> msg,
                                                           Reference<ExtMsgReply> reply);
struct UpdateCmd {
	static const char* name;
	static Future<Reference<ExtMsgReply>> call(Reference<ExtConnection> nmc,
	                                           Reference<ExtMsgQuery> query,
	                                           Reference<ExtMsgReply> reply) {
		return updateAndReply(nmc, query, reply);
	}
};
REGISTER_CMD(UpdateCmd, "update");

ACTOR static Future<Reference<ExtMsgReply>> updateAndReply(Reference<ExtConnection> nmc,
                                                           Reference<ExtMsgQuery> msg,
                                                           Reference<ExtMsgReply> reply) {
	if (!msg->query.hasField(UpdateCmd::name) || !msg->query.hasField("updates")) {
		TraceEvent(SevWarn, "WireBadUpdate").detail("query", msg->query.toString()).suppressFor(1.0);
		throw wire_protocol_mismatch();
	}

	// Bulk update command is not all or nothing. It is possible, some updates succeed and some don't. So, that
	// makes it important to consider "ordered" flag.
	const bool ordered = !msg->query.hasField("ordered") || msg->query.getField("ordered").Bool();
	std::vector<bson::BSONElement> bsonCmds = msg->query.getField("updates").Array();
	state std::vector<ExtUpdateCmd> cmds;
	for (const auto& bsonCmd : bsonCmds) {
		auto bsonCmdO = bsonCmd.Obj();
		if (!bsonCmdO.hasField("q") || !bsonCmdO.hasField("u")) {
			TraceEvent(SevWarn, "Wire_BadUpdate").detail("query", msg->query.toString()).suppressFor(1.0);
			throw wire_protocol_mismatch();
		}
		cmds.emplace_back(bsonCmdO.getField("q").Obj(), bsonCmdO.getField("u").Obj(),
		                  bsonCmdO.hasField("upsert") && bsonCmdO.getField("upsert").Bool(),
		                  bsonCmdO.hasField("multi") && bsonCmdO.getField("multi").Bool());
	}

	WriteCmdResult ret = wait(doUpdateCmd(msg->ns, ordered, &cmds, nmc));

	bson::BSONObjBuilder replyBuilder;
	replyBuilder << "ok" << 1 << "n" << (long long)ret.n << "nModified" << (long long)ret.nModified;

	if (!ret.upsertedOIDList.empty()) {
		bson::BSONArrayBuilder upsertBuilder;
		for (int i = 0; i < ret.upsertedOIDList.size(); i++) {
			auto& upsertedOID = ret.upsertedOIDList[i];
			bson::BSONObjBuilder builder;
			builder << "index" << i;
			builder.appendElements(DataValue::decode_key_part(upsertedOID).wrap(DocLayerConstants::ID_FIELD));
			upsertBuilder << builder.done();
		}
		replyBuilder << "upserted" << upsertBuilder.arr();
	}

	if (!ret.writeErrors.empty()) {
		bson::BSONArrayBuilder arrayBuilder;
		for (auto& writeError : ret.writeErrors) {
			arrayBuilder << writeError;
		}
		replyBuilder << "writeErrors" << arrayBuilder.arr();
	}
	reply->addDocument(replyBuilder.obj());

	return reply;
}

struct ListCollectionsCmd {
	static const char* name;
	ACTOR static Future<Reference<ExtMsgReply>> call(Reference<ExtConnection> ec,
	                                                 Reference<ExtMsgQuery> msg,
	                                                 Reference<ExtMsgReply> reply) {
		if (msg->query.hasField("cursor")) {
			const auto cursorObj = msg->query.getObjectField("cursor");
			if (cursorObj.hasField("batchSize") && cursorObj.getIntField("batchSize") != 0) {
				TraceEvent(SevWarn, "unsupportedCmdOption")
				    .detail("cmd", "listCollections")
				    .detail("option", "cursor.batchSize");
				throw unsupported_cmd_option();
			}
		}

		state std::string databaseName = msg->getDBName();
		state Reference<DocTransaction> dtr = ec->getOperationTransaction();
		state Standalone<VectorRef<StringRef>> names;
		loop {
			try {
				Standalone<VectorRef<StringRef>> _names =
				    wait(ec->docLayer->rootDirectory->list(dtr->tr, {StringRef(databaseName)}));
				names = _names;
				break;
			} catch (Error& e) {
				// If directory doesn't exist treat like empty database.
				if (e.code() == error_code_directory_does_not_exist) {
					break;
				}
				if (e.code() != error_code_actor_cancelled) {
					wait(dtr->onError(e));
				}
			}
		}

		bson::BSONArrayBuilder collList;
		for (Standalone<StringRef> name : names) {
			collList << BSON(DocLayerConstants::NAME_FIELD << name.toString() << "options"
			                                               << bson::BSONObjBuilder().obj());
		}

		// FIXME: Not using cursors to return collection list.
		reply->addDocument(BSON(
		    // clang-format off
							   "cursor" << BSON(
								   "id" << (long long) 0 <<
								   DocLayerConstants::NS_FIELD << databaseName + ".$cmd.listCollections" <<
								   "firstBatch" << collList.arr()) <<
								   "ok" << 1.0
		    // clang-format on
		    ));

		return reply;
	}
};
REGISTER_CMD(ListCollectionsCmd, "listcollections");

struct ListIndexesCmd {
	static const char* name;
	ACTOR static Future<Reference<ExtMsgReply>> call(Reference<ExtConnection> ec,
	                                                 Reference<ExtMsgQuery> msg,
	                                                 Reference<ExtMsgReply> reply) {
		if (msg->query.hasField("cursor")) {
			const auto cursorObj = msg->query.getObjectField("cursor");
			if (cursorObj.hasField("batchSize") && cursorObj.getIntField("batchSize") != 0) {
				TraceEvent(SevWarn, "unsupportedCmdOption")
				    .detail("cmd", "listCollections")
				    .detail("option", "cursor.batchSize");
				throw unsupported_cmd_option();
			}
		}

		state Reference<DocTransaction> dtr = ec->getOperationTransaction();
		loop {
			try {
				Reference<UnboundCollectionContext> unbound = wait(ec->mm->indexesCollection(dtr, msg->ns.first));

				auto getIndexesPlan = getIndexesForCollectionPlan(unbound, msg->ns);
				std::vector<bson::BSONObj> indexObjs = wait(getIndexesTransactionally(getIndexesPlan, dtr));

				bson::BSONArrayBuilder indexList;
				for (auto& indexObj : indexObjs) {
					indexList << indexObj;
				}

				// Add the _id index here
				// clang-format off
				indexList << BSON(
						DocLayerConstants::NAME_FIELD             << "_id_" <<
						DocLayerConstants::NS_FIELD               << (msg->ns.first + "." + msg->ns.second) <<
						DocLayerConstants::KEY_FIELD              << BSON(DocLayerConstants::ID_FIELD << 1) <<
						DocLayerConstants::METADATA_VERSION_FIELD << 1 <<
						DocLayerConstants::STATUS_FIELD           << DocLayerConstants::INDEX_STATUS_READY <<
						DocLayerConstants::UNIQUE_FIELD           << true
						);
				// clang-format on

				// FIXME: Not using cursors to return collection list.
				// clang-format off
				reply->addDocument(BSON(
						"cursor" << BSON(
								"id"           << (long long) 0 <<
								"ns"           << msg->ns.first + ".$cmd.listIndexes." + msg->ns.second <<
								"firstBatch"   << indexList.arr()) <<
								"ok"           << 1.0
								)
								);
				// clang-format on
				return reply;
			} catch (Error& e) {
				if (e.code() != error_code_actor_cancelled) {
					wait(dtr->onError(e));
				}
			}
		}
	}
};
REGISTER_CMD(ListIndexesCmd, "listindexes");

ACTOR static Future<Reference<ExtMsgReply>> getStreamDistinct(Reference<ExtConnection> ec,
                                                              Reference<ExtMsgQuery> query,
                                                              Reference<ExtMsgReply> reply) {
	state double startTime = timer_monotonic();
	state int scanned = 0;
	state int filtered = 0;

	try {
		state Reference<DocTransaction> dtr = ec->getOperationTransaction();
		Reference<UnboundCollectionContext> cx = wait(ec->mm->getUnboundCollectionContext(dtr, query->ns, true));

		if (!query->query.hasField(DocLayerConstants::KEY_FIELD)) {
			throw wire_protocol_mismatch();
		}
		state std::string keyValue = query->query.getStringField(DocLayerConstants::KEY_FIELD);

		Reference<Plan> qrPlan = planQuery(cx, query->query.getObjectField(DocLayerConstants::QUERY_FIELD));
		qrPlan = ec->wrapOperationPlan(qrPlan, true, cx);
		state Reference<DistinctPredicate> distinctPredicate = ref(new DistinctPredicate(keyValue));
		state Reference<IPredicate> predicate = any_predicate(keyValue, distinctPredicate);

		state Reference<PlanCheckpoint> checkpoint(new PlanCheckpoint);
		state FlowLock* flowControlLock = checkpoint->getDocumentFinishedLock();
		state FutureStream<Reference<ScanReturnedContext>> queryResults = qrPlan->execute(checkpoint.getPtr(), dtr);
		state PromiseStream<Reference<ScanReturnedContext>> filteredResults;

		wait(asyncFilter(queryResults,
		                 [=](Reference<ScanReturnedContext> queryResult) mutable {
			                 scanned++;
			                 return map(predicate->evaluate(queryResult), [=](bool keep) mutable {
				                 if (keep)
					                 filtered++;
				                 // For `distinct`, accumulated distinct values are already held in the
				                 // distinctPredicate, and the returned kv is no longer needed by any
				                 // upstream caller after this point. Thus release it immediately.
				                 flowControlLock->release();
				                 return keep;
			                 });
		                 },
		                 filteredResults));

		bson::BSONArrayBuilder arrayBuilder;
		distinctPredicate->collectDataValues(arrayBuilder);

		// FIXME: we need to audit if the `stats` sent back is the correct data and format
		// `n`: Number of documents matched
		// `nscanned`: Number of documents or indexes scanned
		// `nscannedObjects`: Number of documents scanned
		// `nscanned` and `nscannedObjects` should always be the same here.
		// clang-format off
		reply->addDocument(
		    BSON("values" << arrayBuilder.arr() << "stats"
		                  << BSON("n" << filtered <<
		                          "nscanned" << scanned <<
		                          "nscannedObjects" << scanned <<
		                          "timems" << int((timer_monotonic() - startTime) * 1e3) <<
		                          "cursor" << "BasicCursor"
		                          )
		                  << "ok" << 1.0));
		// clang-format on
		return reply;
	} catch (Error& e) {
		reply->addDocument(BSON("$err" << e.what() << "code" << e.code() << "ok" << 1.0));
		reply->setResponseFlags(2 /*0b0010*/);
		return reply;
	}
}

struct GetDistinctCmd {
	static const char* name;
	static Future<Reference<ExtMsgReply>> call(Reference<ExtConnection> ec,
	                                           Reference<ExtMsgQuery> query,
	                                           Reference<ExtMsgReply> reply) {
		return getStreamDistinct(ec, query, reply);
	}
};
REGISTER_CMD(GetDistinctCmd, "distinct");

struct ConnectionStatusCmd {
	static const char* name;
	static Future<Reference<ExtMsgReply>> call(Reference<ExtConnection> ec,
	                                           Reference<ExtMsgQuery> query,
	                                           Reference<ExtMsgReply> reply) {
		const bool showPrivileges = query->query.getBoolField("showPrivileges");

		bson::BSONObjBuilder authInfo;
		authInfo.append("authenticatedUsers", std::vector<std::string>());
		authInfo.append("authenticatedUserRoles", std::vector<std::string>());
		if (showPrivileges) {
			authInfo.append("authenticatedUserPrivileges", std::vector<std::string>());
		}
		reply->addDocument(BSON("authInfo" << authInfo.obj() << "ok" << 1));

		return reply;
	}
};
REGISTER_CMD(ConnectionStatusCmd, "connectionstatus");
