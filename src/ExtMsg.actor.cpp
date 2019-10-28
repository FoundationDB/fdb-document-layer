/*
 * ExtMsg.actor.cpp
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

#include "ExtMsg.actor.h"
#include "Ext.h"

#include "Cursor.h"
#include "ExtCmd.h"
#include "ExtOperator.h"
#include "ExtUtil.actor.h"
#include "MetadataManager.h"

#include "QLOperations.h"
#include "QLPlan.actor.h"
#include "QLPredicate.h"
#include "QLProjection.actor.h"
#include "QLTypes.h"

#include "bson.h"
#include "ordering.h"

#include "flow/Platform.h"
#include "flow/UnitTest.h"

#include "flow/actorcompiler.h" // This must be the last #include.
#include <string>

using namespace FDB;

REGISTER_MSG(ExtMsgQuery);
REGISTER_MSG(ExtMsgReply);
REGISTER_MSG(ExtMsgUpdate);
REGISTER_MSG(ExtMsgInsert);
REGISTER_MSG(ExtMsgGetMore);
REGISTER_MSG(ExtMsgDelete);
REGISTER_MSG(ExtMsgKillCursors);

ACTOR Future<Void> wrapError(Future<Void> actorThatCouldThrow) {
	try {
		wait(actorThatCouldThrow);
	} catch (Error& e) {
		TraceEvent(SevError, "BackgroundTask").error(e);
	}
	return Void();
}

// ns -> database.collection
Namespace getDBCollectionPair(const char* ns, std::pair<std::string, std::string> errMsg) {
	const auto dotPtr = strchr(ns, '.');
	if (dotPtr == nullptr) {
		TraceEvent(SevWarn, "WireBadCollectionName").detail(errMsg.first, errMsg.second).suppressFor(1.0);
		throw wire_protocol_mismatch();
	}
	return std::make_pair(std::string(ns, dotPtr - ns), std::string(dotPtr + 1));
}

/**
 * query := { $bool_op : [ query* ],   *          (a predicate)
 * 			  path : literal_match,    *
 *            path : value_query,      * }
 * value_query := { $value_op : param, * }        (with the context of a path, a predicate)
 */

/**
 * Converts a `value_query` in the above grammar, with the given path, to a predicate
 * The predicate is returned by appending to `out_terms` zero or more predicates which,
 * anded together, have the semantics of the given value_query.
 */
void valueQueryToPredicates(bson::BSONObj const& value_query,
                            std::string const& path,
                            std::vector<Reference<IPredicate>>& out_terms) {
	std::string regex_options; // hacky special handling of { $option, $regex }
	for (auto it = value_query.begin(); it.more();) {
		auto sub = it.next();
		if (std::string(sub.fieldName()) == "$options") {
			regex_options = sub.String();
		} else {
			out_terms.push_back(ExtValueOperator::toPredicate(sub.fieldName(), path, sub));
		}
	}

	// $options is applied to "$regex" so we don't have to create a Value Operator for it
	// but we still need to find the predicate that is RegEx and apply the options
	for (auto itCur = out_terms.rbegin(); !(itCur == out_terms.rend()); ++itCur) {
		if (auto pAnyPredicate = dynamic_cast<AnyPredicate*>(itCur->getPtr())) {
			if (auto pRegExPredicate = dynamic_cast<RegExPredicate*>(pAnyPredicate->pred.getPtr())) {
				pRegExPredicate->setOptions(regex_options);
				break;
			}
		}
	}
}

/**
 * Converts a `value_query` in the above grammar, with the given path, to a predicate.
 */
Reference<IPredicate> valueQueryToPredicate(bson::BSONObj const& query, std::string const& path) {
	std::vector<Reference<IPredicate>> terms;
	valueQueryToPredicates(query, path, terms);
	return ref(new AndPredicate(terms));
}

/**
 *  Converts a mongo-like query document (query in the above grammar) into a corresponding
 *  QL predicate.
 */
Reference<IPredicate> queryToPredicate(bson::BSONObj const& query, bool toplevel) {
	std::vector<Reference<IPredicate>> terms;

	for (auto i = query.begin(); i.more();) {
		auto el = i.next();

		if (el.type() == bson::BSONType::RegEx) {
			terms.push_back(re_predicate(el, el.fieldName()));
		} else if (el.fieldName()[0] == '$') {
			if (el.isABSONObj()) {
				try {
					terms.push_back(ExtBoolOperator::toPredicate(el.fieldName(), el.Obj()));
				} catch (Error& e) {
					if (e.code() == error_code_bad_dispatch)
						throw fieldname_with_dollar();
					else
						throw;
				}
			} else {
				throw fieldname_with_dollar();
			}
		} else if (el.isABSONObj() && !is_literal_match(el.Obj())) {
			// If this is a top-level _id path then force use of elemMatch because _id can NOT be an array.
			// TODO: Fix. It isn't working for {_id: {"$in":[...]}}
			//if (toplevel && strcmp(el.fieldName(), DocLayerConstants::ID_FIELD) == 0)
			//	terms.push_back(ExtValueOperator::toPredicate("$elemMatch", el.fieldName(), el));
			//else
			valueQueryToPredicates(el.Obj(), el.fieldName(), terms);
		} else {
			terms.push_back(eq_predicate(el, el.fieldName()));
		}
	}

	return Reference<IPredicate>(new AndPredicate(terms));
}

Reference<Plan> planQuery(Reference<UnboundCollectionContext> cx, bson::BSONObj const& query) {
	auto predicate = queryToPredicate(query, true);
	auto simplifiedPredicate = predicate->simplify();

	Reference<Plan> plan = Reference<Plan>(
	    FilterPlan::construct_filter_plan(cx, Reference<Plan>(new TableScanPlan(cx)), simplifiedPredicate));
	if (verboseConsoleOutput) {
		fprintf(stderr, "parsed predicate: %s\n", predicate->toString().c_str());
		fprintf(stderr, "   simplified to: %s\n\n", simplifiedPredicate->toString().c_str());
	}
	if (verboseLogging) {
		TraceEvent("BD_GetMatchingDocs")
		    .detail("QueryPredicate", predicate->toString())
		    .detail("SimplifiedPredicate", predicate->toString())
		    .detail("Plan", plan->describe().toString());
	}

	if (slowQueryLogging && plan->hasScanOfType(PlanType::TableScan)) {
		std::string collectionName = cx->collectionName();
		if (!startsWith(collectionName.c_str(), "system."))
			TraceEvent("SlowQuery")
			    .detail("Database", cx->databaseName())
			    .detail("Collection", collectionName)
			    .detail("Query", query.toString())
			    .detail("Plan", plan->describe().toString());
	}

	return plan;
}

Reference<Plan> planProjection(Reference<Plan> plan,
                               bson::BSONObj const& selector,
                               Optional<bson::BSONObj> const& ordering) {
	return Reference<Plan>(new ProjectionPlan(parseProjection(selector), plan, ordering));
}

ExtMsgQuery::ExtMsgQuery(ExtMsgHeader* header, const uint8_t* body) : header(header) {
	const uint8_t* ptr = body;
	const uint8_t* eom = (uint8_t*)header + header->messageLength;

	flags = *(int32_t*)ptr;
	ptr += sizeof(int32_t);

	const char* collName = (const char*)ptr;
	ptr += strlen(collName) + 1;

	numberToSkip = *(int32_t*)ptr;
	ptr += sizeof(int32_t);
	numberToReturn = *(int32_t*)ptr;
	ptr += sizeof(int32_t);

	query = bson::BSONObj((const char*)ptr);
	ptr += query.objsize();

	if (ptr != eom) {
		returnFieldSelector = bson::BSONObj((const char*)ptr);
		ptr += returnFieldSelector.objsize();
	}

	ns = getDBCollectionPair(collName, std::make_pair(DocLayerConstants::QUERY_FIELD, query.toString()));
	if (ns.second == "$cmd") {
		isCmd = true;
		// Mark it empty for now, command would be responsible for setting collection name later.
		ns.second = "";
	} else {
		isCmd = false;
	}

	/* We should have consumed all the bytes specified by header */
	ASSERT(ptr == eom);
}

std::string ExtMsgQuery::toString() {
	return format("QUERY: %s, collection=%s, flags=%d, numberToSkip=%d, numberToReturn=%d (%s)",
	              query.toString(false, true).c_str(), fullCollNameToString(ns).c_str(), flags, numberToSkip,
	              numberToReturn, header->toString().c_str());
}

ACTOR Future<Void> runCommand(Reference<ExtConnection> nmc,
                              Reference<ExtMsgQuery> query,
                              PromiseStream<Reference<ExtMsgReply>> replyStream) {
	state Reference<ExtMsgReply> errReply(new ExtMsgReply(query->header));

	// For OP_QUERY commands, first elements field name is the command name. And the value contains
	// the collection name if the command is at collection level.
	auto firstElement = query->query.begin().next();
	state std::string cmd = firstElement.fieldName();
	std::transform(cmd.begin(), cmd.end(), cmd.begin(), ::tolower);
	if (firstElement.isString()) {
		query->ns.second = firstElement.String();
	}

	try {
		Reference<ExtMsgReply> reply = wait(ExtCmd::call(cmd, nmc, query, errReply));
		replyStream.send(reply);
	} catch (Error& e) {
		bson::BSONObjBuilder bob;
		TraceEvent(SevWarn, "CmdFailed").error(e);

		if (e.code() == error_code_bad_dispatch) {
			bob.append("errmsg", format("no such cmd: %s", cmd.c_str()));
			bob.append("$err", format("no such cmd: %s", cmd.c_str()));
		} else {
			bob.append("errmsg", format("command [%s] failed with err: %s", cmd.c_str(), e.what()));
			bob.append("$err", format("command [%s] failed with err: %s", cmd.c_str(), e.what()));
		}
		bob.append("bad cmd", query->query.toString());
		bob.append("ok", 0);

		errReply->addDocument(bob.obj());
		errReply->setResponseFlags(2);
		replyStream.send(errReply);
	}

	replyStream.sendError(end_of_stream());
	return Void();
}

// Set the shouldBeRead flag to false on Projections whose associated fields don't need to have their entire ranges
// read.
void Projection::filterUnneededReads(Reference<Projection> const& startingProjection, int maxReads) {
	if (startingProjection->expandable() && startingProjection->fields.size() <= maxReads) {
		startingProjection->shouldBeRead = false;
	}

	// SOMEDAY: If we enable the code path below, then we will need to account for the case that one of the filtered
	// elements is an array (see SOMEDAY in projectDocument). That accounting will probably require an extra read
	// latency, so it might be useful to enable the code path conditionally based on whether we believe the fields in
	// question are not a part of an array.

	/*std::priority_queue<Reference<Projection>, std::vector<Reference<Projection>>, Projection::SizeComparer>
	projectionQueue; projectionQueue.push(startingProjection);

	// Expand the projection with the smallest number of children into one projection per child until we've either
	// hit our limit or there are none left to expand.
	loop {
	    auto projection = projectionQueue.top();

	    if(projection->expandable() && projection->fields.size() + projectionQueue.size() - 1 <= maxReads) {
	        projection->shouldBeRead = false;

	        projectionQueue.pop();
	        for(auto child : projection->fields) {
	            projectionQueue.push(child.second);
	        }
	    }
	    else {
	        break;
	    }
	}*/
}

ACTOR static Future<Reference<ExtMsgReply>> listCollections(Reference<ExtMsgQuery> query,
                                                            Reference<DocTransaction> tr,
                                                            Reference<DirectorySubspace> rootDirectory) {

	state Reference<ExtMsgReply> reply = Reference<ExtMsgReply>(new ExtMsgReply(query->header, query->query));
	state std::string databaseNameFromQuery = query->getDBName();
	state Standalone<VectorRef<StringRef>> names;
	try {
		Standalone<VectorRef<StringRef>> _names = wait(rootDirectory->list(tr->tr, {StringRef(databaseNameFromQuery)}));
		names = _names;
	} catch (Error& e) {
		return reply;
	}

	for (Standalone<StringRef> s : names) {
		std::string str = databaseNameFromQuery + "." + s.toString();
		reply->addDocument(BSON(DocLayerConstants::NAME_FIELD << str));
	}

	return reply;
}

ACTOR static Future<int32_t> addDocumentsFromCursor(Reference<Cursor> cursor,
                                                    Reference<ExtMsgReply> reply,
                                                    int32_t numberToReturn) {
	state int32_t returned = 0;
	state int32_t returnedSize = 0;
	state bool stop = false;

	state int32_t remaining = std::abs(numberToReturn);

	while (!numberToReturn || remaining) {
		try {
			if ((returned <= DOCLAYER_KNOBS->MAX_RETURNABLE_DOCUMENTS ||
			     returnedSize <= DOCLAYER_KNOBS->DEFAULT_RETURNABLE_DATA_SIZE) &&
			    returnedSize <= DOCLAYER_KNOBS->MAX_RETURNABLE_DATA_SIZE) {

				Reference<ScanReturnedContext> doc = waitNext(cursor->docs);

				// Note that this call to get() is safe here but not in general, because we know
				// that doc is wrapping a BsonContext, which means toDataValue() is synchronous.
				bson::BSONObj obj = doc->toDataValue().get().getPackedObject().getOwned();
				cursor->checkpoint->getDocumentFinishedLock()->release();
				reply->addDocument(obj);				

				remaining--;
				returned++;
				cursor->returned++;
				returnedSize += obj.objsize();
			} else {
				throw success();
			}
		} catch (Error& e) {
			if (e.code() == error_code_end_of_stream) {
				stop = true;
				break;
			}
			if (e.code() == error_code_success) {
				stop = false;
				break;
			}
			TraceEvent(SevError, "BD_addDocumentsFromCursor").error(e);
			throw;
		}
	}

	// Reply with cursorID if requested or remove the cursor
	if (numberToReturn >= 0 && !stop)
		reply->replyHeader.cursorID = cursor->id;
	else
		Cursor::pluck(cursor);

	return returned;
}

ACTOR static Future<Void> runQuery(Reference<ExtConnection> ec,
                                   Reference<ExtMsgQuery> msg,
                                   PromiseStream<Reference<ExtMsgReply>> replyStream) {

	state Reference<ExtMsgReply> reply;
	state Reference<Cursor> cursor;
	state Reference<DocTransaction> dtr = ec->getOperationTransaction();
	bool sorted = (msg->query.hasField("orderby") || msg->query.hasField("$orderby"));
	state Optional<bson::BSONObj> ordering = sorted ? msg->query.hasField("orderby")
	                                                      ? msg->query.getObjectField("orderby")
	                                                      : msg->query.getObjectField("$orderby")
	                                                : Optional<bson::BSONObj>();

	try {
		// Return `listCollections()` if `ns` is like "name.system.namespaces"
		if (msg->ns.second == DocLayerConstants::SYSTEM_NAMESPACES) {
			Reference<ExtMsgReply> collections = wait(listCollections(msg, dtr, ec->docLayer->rootDirectory));
			replyStream.send(collections);
			throw end_of_stream();
		}

		state Reference<UnboundCollectionContext> cx = wait(ec->mm->getUnboundCollectionContext(dtr, msg->ns, true));

		// The following is required by ambiguity in the wire protocol we are speaking
		bson::BSONObj queryObject = msg->query.hasField(DocLayerConstants::QUERY_FIELD)
		                                ? msg->query.getObjectField(DocLayerConstants::QUERY_FIELD)
		                                : msg->query.hasField(DocLayerConstants::QUERY_OPERATOR.c_str())
		                                      ? msg->query.getObjectField(DocLayerConstants::QUERY_OPERATOR.c_str())
		                                      : msg->query;

		// Plan needs to be state in case we have a sort plan, which in turn holds a reference to the actor that does
		// the sorting
		state Reference<Plan> plan = planQuery(cx, queryObject);
		if (!ordering.present() && msg->numberToSkip)
			plan = ref(new SkipPlan(msg->numberToSkip, plan));
		plan = planProjection(plan, msg->returnFieldSelector, ordering);
		plan = ec->wrapOperationPlan(plan, true, cx);
		if (ordering.present()) {
			plan = ref(new SortPlan(plan, ordering.get()));
			if (msg->numberToSkip)
				plan = ref(new SkipPlan(msg->numberToSkip, plan));
		}

		// return query plan explanation if `$explain` detected
		if (msg->query.hasField("$explain")) {
			reply = Reference<ExtMsgReply>(new ExtMsgReply(msg->header, msg->query));
			reply->addDocument(BSON("explanation" << plan->describe()));
			replyStream.send(reply);
			throw end_of_stream();
		}

		Reference<PlanCheckpoint> outerCheckpoint(new PlanCheckpoint);

		// Add a new cursor to the server's cursor collection
		cursor = Cursor::add(
		    ec->cursors, Reference<Cursor>(new Cursor(plan->execute(outerCheckpoint.getPtr(), dtr), outerCheckpoint)));

		state int replies = 0;
		state bool exhaust = ((msg->flags & EXHAUST) != 0);
		state int32_t lastRequestID = msg->header->requestID;

		loop {
			reply = Reference<ExtMsgReply>(new ExtMsgReply(msg->header, msg->query));
			// Add requested documents to the reply from the cursor
			int32_t toReturn = msg->numberToReturn;

			// In Exhaust mode with numberToReturn == 0, the first reply will target 100 documents because
			// this is the behavior observed in MongoDB.
			if (exhaust && toReturn == 0 && replies == 0)
				toReturn = 100;

			// Mongo protocol states that 1 is to be treated as -1.
			// See (http://docs.mongodb.org/meta-driver/latest/legacy/mongodb-wire-protocol/#op-query)
			if (toReturn == 1)
				toReturn = -1;

			int32_t returned = wait(addDocumentsFromCursor(cursor, reply, toReturn));
			reply->addResponseFlag(8 /*0b1000*/);

			// If no replies sent yet OR results were placed in reply, send them.
			if (replies == 0 || returned > 0) {

				// In Exhaust mode the server will generate RequestIDs for "get more" requests that never
				// were sent by the client because this is the behavior observed in MongoDB.
				if (exhaust) {
					reply->replyHeader.responseTo = lastRequestID;
					lastRequestID = ec->getNewRequestID();
					reply->replyHeader.requestID = lastRequestID;
				}

				replyStream.send(reply);
			}

			// If EXHAUST not set OR it is but we got <=0 results, stop.
			if (!exhaust || returned <= 0)
				break;

			++replies;
		}
	} catch (Error& e) {
		if (e.code() != error_code_end_of_stream) {
			reply = Reference<ExtMsgReply>(new ExtMsgReply(msg->header, msg->query));
			reply->addDocument(BSON("$err" << e.what() << "code" << e.code() << "ok" << 1.0));
			reply->setResponseFlags(2 /*0b0010*/);
			replyStream.send(reply);
		}
	}

	replyStream.sendError(end_of_stream());
	return Void();
}

ACTOR static Future<Void> doRun(Reference<ExtMsgQuery> query, Reference<ExtConnection> ec) {
	state PromiseStream<Reference<ExtMsgReply>> replyStream;
	state Future<Void> x;
	state uint64_t startTime = timer_int();

	if (query->isCmd) {
		// It's a command
		x = runCommand(ec, query, replyStream);
	} else {
		// Run a query
		x = runQuery(ec, query, replyStream);
	}

	state FutureStream<Reference<ExtMsgReply>> replies = replyStream.getFuture();
	loop {
		try {
			Reference<ExtMsgReply> reply = waitNext(replies);
			if (verboseLogging)
				TraceEvent("BD_doRun").detail("Reply", reply->toString());
			reply->write(ec);
		} catch (Error& e) {
			if (e.code() == error_code_end_of_stream)
				break;
			throw e;
		}
	}

	uint64_t queryLatencyMicroSeconds = (timer_int() - startTime) / 1000;
	DocumentLayer::metricReporter->captureTime(DocLayerConstants::MT_TIME_QUERY_LATENCY_US, queryLatencyMicroSeconds);

	if (slowQueryLogging && queryLatencyMicroSeconds >= DOCLAYER_KNOBS->SLOW_QUERY_THRESHOLD_MICRO_SECONDS) {
		TraceEvent(SevWarn, "SlowQuery")
		    .detail("ThresholdMicroSeconds", DOCLAYER_KNOBS->SLOW_QUERY_THRESHOLD_MICRO_SECONDS)
		    .detail("DurationMicroSeconds", queryLatencyMicroSeconds)
		    .detail("Query", query->toString());
	}

	return Void();
}

std::string ExtMsgQuery::getDBName() {
	return ns.first;
}

Future<Void> ExtMsgQuery::run(Reference<ExtConnection> nmc) {
	return doRun(Reference<ExtMsgQuery>::addRef(this), nmc);
}

ExtMsgReply::ExtMsgReply(ExtMsgHeader* header, const uint8_t* body) : header(nullptr) {
	memcpy(&replyHeader, header, sizeof(ExtReplyHeader));

	const uint8_t* ptr = (const uint8_t*)header + sizeof(ExtReplyHeader);
	const uint8_t* eom = (const uint8_t*)header + header->messageLength;

	while (ptr < eom) {
		bson::BSONObj doc((const char*)ptr);
		ptr += doc.objsize();
		documents.push_back(doc);
	}

	ASSERT(ptr == eom);
}

ExtMsgReply::ExtMsgReply(ExtMsgHeader* header, bson::BSONObj const& query) : header(header), query(query) {
	replyHeader.responseTo = header->requestID;
	replyHeader.opCode = 1; // OP_REPLY
}

ExtMsgReply::ExtMsgReply(ExtMsgHeader* header) : header(header) {
	replyHeader.responseTo = header->requestID;
	replyHeader.opCode = 1; // OP_REPLY
}

std::string ExtMsgReply::toString() {
	std::string buf = "REPLY: documents=[ ";

	for (const auto& d : documents) {
		buf += d.toString();
		buf += " ";
	}

	buf += format("], responseFlags=%d, cursorID=%d, startingFrom=%d (%s)", replyHeader.responseFlags,
	              replyHeader.cursorID, replyHeader.startingFrom, replyHeader.toString().c_str());

	return buf;
}

void ExtMsgReply::write(Reference<ExtConnection> nmc) {
	replyHeader.messageLength = sizeof(replyHeader);
	for (const auto& document : documents) {
		replyHeader.messageLength += document.objsize();
	}

	if (verboseLogging)
		TraceEvent("BD_msgReply").detail("Message", toString()).detail("connId", nmc->connectionId);
	if (verboseConsoleOutput)
		fprintf(stderr, "S -> C: %s\n\n", toString().c_str());

	nmc->bc->write(StringRef((uint8_t*)&replyHeader, sizeof(replyHeader)));

	for (const auto& doc : documents) {
		nmc->bc->write(StringRef((const uint8_t*)doc.objdata(), doc.objsize()));
	}
}

ExtMsgInsert::ExtMsgInsert(ExtMsgHeader* header, const uint8_t* body) : header(header) {
	const uint8_t* ptr = body;
	const uint8_t* eom = (const uint8_t*)header + header->messageLength;

	flags = *(int32_t*)ptr;
	ptr += sizeof(int32_t);

	const char* collName = (const char*)ptr;
	ptr += strlen(collName) + 1;
	ns = getDBCollectionPair(collName, std::make_pair("msgType", "OP_INSERT"));

	while (ptr < eom) {
		bson::BSONObj doc = bson::BSONObj((const char*)ptr);
		ptr += doc.objsize();
		documents.push_back(doc);
	}

	ASSERT(ptr == eom);
}

std::string ExtMsgInsert::toString() {
	std::string buf = "INSERT: documents=[ ";

	for (const auto& d : documents) {
		buf += d.toString();
		buf += " ";
	}

	buf +=
	    format("], collection=%s, flags=%d (%s)", fullCollNameToString(ns).c_str(), flags, header->toString().c_str());

	return buf;
}

ACTOR Future<Reference<IReadWriteContext>> insertDocument(Reference<CollectionContext> cx,
                                                          bson::BSONObj d,
                                                          Optional<IdInfo> encodedIds) {
	state Standalone<StringRef> encodedId;
	state Standalone<StringRef> valueEncodedId;
	state Optional<bson::BSONObj> idObj;
	state Reference<QueryContext> dcx;

	if (!encodedIds.present()) {
		encodedId = DataValue(bson::OID::gen()).encode_key_part();
		valueEncodedId = encodedId;
		idObj = Optional<bson::BSONObj>();
		dcx = cx->cx->getSubContext(encodedId);
	} else {
		encodedId = encodedIds.get().keyEncoded;
		valueEncodedId = encodedIds.get().valueEncoded;
		idObj = encodedIds.get().objValue;
		dcx = cx->cx->getSubContext(encodedId);
		Optional<DataValue> existing =
		    wait(dcx->get(DataValue(DocLayerConstants::ID_FIELD, DVTypeCode::STRING).encode_key_part()));
		if (existing.present())
			throw duplicated_key_field();
	}

	// FIXME: abstraction violation out of laziness
	Key prefix = StringRef(dcx->getPrefix().toString());
	dcx->getTransaction()->tr->addWriteConflictRange(KeyRangeRef(prefix, strinc(prefix)));

	dcx->set(LiteralStringRef(""), DataValue::subObject().encode_value());

	int nrFDBKeys = 0;
	for (auto i = d.begin(); i.more();) {
		auto e = i.next();
		nrFDBKeys += insertElementRecursive(e, dcx);
	}
	DocumentLayer::metricReporter->captureHistogram(DocLayerConstants::MT_HIST_KEYS_PER_DOCUMENT, nrFDBKeys);
	DocumentLayer::metricReporter->captureHistogram(DocLayerConstants::MT_HIST_DOCUMENT_SZ, d.objsize());

	if (idObj.present())
		insertElementRecursive(DocLayerConstants::ID_FIELD, idObj.get(), dcx);

	dcx->set(DataValue(DocLayerConstants::ID_FIELD, DVTypeCode::STRING).encode_key_part(), valueEncodedId);

	return dcx;
}

struct ExtInsert : ConcreteInsertOp<ExtInsert> {
	bson::BSONObj obj;
	Optional<IdInfo> encodedIds;

	ExtInsert(bson::BSONObj obj, Optional<IdInfo> encodedIds) : obj(obj), encodedIds(encodedIds) {}

	std::string describe() override { return "Insert(" + obj.toString() + ")"; }

	Future<Reference<IReadWriteContext>> insert(Reference<CollectionContext> cx) override {
		return insertDocument(cx, obj, encodedIds);
	}
};

struct ExtIndexInsert : ConcreteInsertOp<ExtIndexInsert> {
	bson::BSONObj indexObj;
	std::string ns;
	UID build_id;

	ExtIndexInsert(bson::BSONObj indexObj, UID build_id, std::string ns)
	    : indexObj(indexObj), build_id(build_id), ns(ns) {}

	std::string describe() override { return "IndexInsert(" + indexObj.toString() + ")"; }

	ACTOR static Future<Reference<IReadWriteContext>> indexInsertActor(ExtIndexInsert* self,
	                                                                   Reference<CollectionContext> cx) {
		state Standalone<StringRef> encodedId;
		state Standalone<StringRef> valueEncodedId;
		state Optional<bson::BSONObj> idObj;
		state Reference<QueryContext> dcx;

		Optional<IdInfo> encodedIds = extractEncodedIds(self->indexObj);

		if (!encodedIds.present()) {
			encodedId = DataValue(bson::OID::gen()).encode_key_part();
			valueEncodedId = encodedId;
			idObj = Optional<bson::BSONObj>();
			dcx = cx->cx->getSubContext(encodedId);
		} else {
			encodedId = encodedIds.get().keyEncoded;
			valueEncodedId = encodedIds.get().valueEncoded;
			idObj = encodedIds.get().objValue;
			dcx = cx->cx->getSubContext(encodedId);
			Optional<DataValue> existing =
			    wait(dcx->get(DataValue(DocLayerConstants::ID_FIELD, DVTypeCode::STRING).encode_key_part()));
			if (existing.present())
				throw duplicated_key_field();
		}

		// FIXME: abstraction violation out of laziness
		Key prefix = StringRef(dcx->getPrefix().toString());
		dcx->getTransaction()->tr->addWriteConflictRange(KeyRangeRef(prefix, strinc(prefix)));

		dcx->set(LiteralStringRef(""), DataValue::subObject().encode_value());

		dcx->set(DataValue(DocLayerConstants::NS_FIELD, DVTypeCode::STRING).encode_key_part(),
		         DataValue(self->ns).encode_value());
		dcx->set(
		    DataValue(DocLayerConstants::NAME_FIELD, DVTypeCode::STRING).encode_key_part(),
		    DataValue(self->indexObj.getStringField(DocLayerConstants::NAME_FIELD), DVTypeCode::STRING).encode_value());
		dcx->set(DataValue(DocLayerConstants::KEY_FIELD, DVTypeCode::STRING).encode_key_part(),
		         DataValue(self->indexObj.getObjectField(DocLayerConstants::KEY_FIELD)).encode_value());
		dcx->set(DataValue(DocLayerConstants::STATUS_FIELD, DVTypeCode::STRING).encode_key_part(),
		         DataValue(DocLayerConstants::INDEX_STATUS_BUILDING, DVTypeCode::STRING).encode_value());
		dcx->set(DataValue(DocLayerConstants::METADATA_VERSION_FIELD, DVTypeCode::STRING).encode_key_part(),
		         DataValue(1).encode_value());
		dcx->set(DataValue(DocLayerConstants::BUILD_ID_FIELD, DVTypeCode::STRING).encode_key_part(),
		         DataValue(self->build_id.toString()).encode_value());
		dcx->set(DataValue(DocLayerConstants::UNIQUE_FIELD, DVTypeCode::STRING).encode_key_part(),
		         DataValue(self->indexObj.getBoolField(DocLayerConstants::UNIQUE_FIELD)).encode_value());
		dcx->set(DataValue(DocLayerConstants::BACKGROUND_FIELD, DVTypeCode::STRING).encode_key_part(),
		         DataValue(self->indexObj.getBoolField(DocLayerConstants::BACKGROUND_FIELD)).encode_value());

		if (idObj.present())
			insertElementRecursive(DocLayerConstants::ID_FIELD, idObj.get(), dcx);

		dcx->set(DataValue(DocLayerConstants::ID_FIELD, DVTypeCode::STRING).encode_key_part(), valueEncodedId);

		return dcx;
	}

	Future<Reference<IReadWriteContext>> insert(Reference<CollectionContext> cx) { return indexInsertActor(this, cx); }
};

ACTOR Future<WriteCmdResult> attemptIndexInsertion(bson::BSONObj indexObj,
                                                   Reference<ExtConnection> ec,
                                                   Reference<DocTransaction> tr,
                                                   Namespace ns) {
	if (!indexObj.hasField(DocLayerConstants::NAME_FIELD))
		throw no_index_name();

	// It is legal to have index key to be simple string for simple value indexes.
	if (indexObj.getField(DocLayerConstants::KEY_FIELD).isString()) {
		bson::BSONObjBuilder builder;
		for (auto it = indexObj.begin(); it.more();) {
			auto el = it.next();
			if (std::string(el.fieldName()) == DocLayerConstants::KEY_FIELD) {
				builder.append(DocLayerConstants::KEY_FIELD, BSON(el.String() << 1));
			} else {
				builder.append(el);
			}
		}
		indexObj = builder.obj();
	}

	if (!indexObj.getField(DocLayerConstants::KEY_FIELD).isABSONObj()) {
		TraceEvent(SevWarn, "BadIndexSpec").detail("err_msg", "Bad key format");
		throw bad_index_specification();
	}

	if (indexObj.getObjectField(DocLayerConstants::KEY_FIELD).nFields() == 1) {
		auto keyEl = indexObj.getObjectField(DocLayerConstants::KEY_FIELD).firstElement();
		if (!keyEl.isNumber() || !(keyEl.Number() == 1.0 || keyEl.Number() == -1.0)) {
			(keyEl.isString() && keyEl.str() == "text") ? throw unsupported_index_type()
			                                            : throw bad_index_specification();
		}
		if (!strcmp(keyEl.fieldName(), DocLayerConstants::ID_FIELD)) {
			return WriteCmdResult();
		}
	} else {
		bool first = true;
		bool ascending;
		for (bson::BSONObjIterator i = indexObj.getObjectField(DocLayerConstants::KEY_FIELD).begin(); i.more();) {
			auto el = i.next();
			if (!el.isNumber()) {
				(el.isString() && el.str() == "text") ? throw unsupported_index_type()
				                                      : throw bad_index_specification();
			}
			if (!strcmp(el.fieldName(), DocLayerConstants::ID_FIELD)) {
				throw compound_id_index();
			}
			double direction = el.Number();
			if (std::abs(direction) != 1.0) {
				throw bad_index_specification();
			}
			if (first) {
				ascending = (direction == 1.0);
				first = false;
			} else if (ascending != (direction == 1.0)) {
				throw no_mixed_compound_index();
			}
		}
	}

	state bool background = indexObj.getBoolField(DocLayerConstants::BACKGROUND_FIELD);
	if (indexObj.getBoolField(DocLayerConstants::UNIQUE_FIELD) && background) {
		throw unique_index_background_construction();
	}

	// Let's add this index
	state UID build_id = g_random->randomUniqueID();
	Reference<IInsertOp> indexOp(new ExtIndexInsert(indexObj, build_id, ns.first + "." + ns.second));
	Reference<Plan> plan = ec->isolatedWrapOperationPlan(ref(new IndexInsertPlan(indexOp, indexObj, ns, ec->mm)));

	state std::pair<int64_t, Reference<ScanReturnedContext>> pair =
	    wait(executeUntilCompletionAndReturnLastTransactionally(plan, tr));

	if (pair.first) {
		Standalone<StringRef> id =
		    wait(pair.second->getKeyEncodedId()); // FIXME: Is this actually transactional? Safe? Maybe project it?

		if (background)
			ec->docLayer->backgroundTasks.add(wrapError(MetadataManager::buildIndex(indexObj, ns, id, ec, build_id)));
		else
			wait(MetadataManager::buildIndex(indexObj, ns, id, ec, build_id));

		return WriteCmdResult(1);
	} else {
		return WriteCmdResult();
	}
}

ACTOR Future<WriteCmdResult> doInsertCmd(Namespace ns,
                                         std::list<bson::BSONObj>* documents,
                                         Reference<ExtConnection> ec) {
	state Reference<DocTransaction> tr = ec->getOperationTransaction();
	state uint64_t startTime = timer_int();

	if (ns.second == DocLayerConstants::SYSTEM_INDEXES) {
		if (verboseLogging)
			TraceEvent("BD_doInsertRun").detail("AttemptIndexInsertion", "");
		if (documents->size() != 1) {
			throw multiple_index_construction();
		}
		bson::BSONObj firstDoc = documents->front();

		const char* collnsStr = firstDoc.getField(DocLayerConstants::NS_FIELD).String().c_str();
		const auto collns = getDBCollectionPair(collnsStr, std::make_pair("msg", "Bad coll name in index insert"));
		WriteCmdResult result = wait(attemptIndexInsertion(firstDoc.getOwned(), ec, tr, collns));

		DocumentLayer::metricReporter->captureTime(DocLayerConstants::MT_TIME_INSERT_LATENCY_US,
		                                           (timer_int() - startTime) / 1000);
		return result;
	}

	std::vector<Reference<IInsertOp>> inserts;
	std::set<Standalone<StringRef>> ids;
	int insertSize = 0;
	for (const auto& d : *documents) {
		const bson::BSONObj& obj = d;
		Optional<IdInfo> encodedIds = extractEncodedIds(obj);
		if (encodedIds.present()) {
			if (!ids.insert(encodedIds.get().keyEncoded).second) {
				throw duplicated_key_field();
			}
		}
		inserts.push_back(Reference<IInsertOp>(new ExtInsert(obj, encodedIds)));
		insertSize += obj.objsize();
	}
	DocumentLayer::metricReporter->captureHistogram(DocLayerConstants::MT_HIST_INSERT_SZ, insertSize);
	DocumentLayer::metricReporter->captureHistogram(DocLayerConstants::MT_HIST_DOCS_PER_INSERT, documents->size());

	state Reference<DocInserter> docInserter;	
	Reference<Plan> plan = ref(new InsertPlan(inserts, ec->mm, ns));

	if (strcmp(ns.first.c_str(), DocLayerConstants::OPLOG_DB.c_str()) != 0) {
		docInserter = ref(new DocInserter(ec->watcher));
		plan = oplogInsertPlan(plan, documents, docInserter, ec->mm, ns);
	}

	plan = ec->isolatedWrapOperationPlan(plan);
	int64_t i = wait(executeUntilCompletionTransactionally(plan, tr));

	if(docInserter.isValid()) {
		docInserter->commit();
	}

	DocumentLayer::metricReporter->captureTime(DocLayerConstants::MT_TIME_INSERT_LATENCY_US,
	                                           (timer_int() - startTime) / 1000);
	return WriteCmdResult(i);
}

ACTOR static Future<WriteResult> doInsertMsg(Future<Void> readyToWrite,
                                             Reference<ExtMsgInsert> insert,
                                             Reference<ExtConnection> ec) {
	wait(readyToWrite);
	WriteCmdResult cmdResult = wait(doInsertCmd(insert->ns, &insert->documents, ec));
	return WriteResult(cmdResult, WriteType::INSERT);
}

Future<Void> ExtMsgInsert::run(Reference<ExtConnection> ec) {
	return ec->afterWrite(doInsertMsg(ec->beforeWrite(documents.size()), Reference<ExtMsgInsert>::addRef(this), ec),
	                      documents.size());
}

ExtMsgUpdate::ExtMsgUpdate(ExtMsgHeader* header, const uint8_t* body) : header(header) {
	const uint8_t* ptr = body;
	const uint8_t* eom = (const uint8_t*)header + header->messageLength;

	ptr += sizeof(int32_t); // mongo OP_UPDATE begins with int32 ZERO, reserved for future use

	const char* collName = (const char*)ptr;
	ptr += strlen(collName) + 1;
	ns = getDBCollectionPair(collName, std::make_pair("msgType", "OP_UPDATE"));

	flags = *(int32_t*)ptr;
	ptr += sizeof(int32_t);

	upsert = ((flags & Flags::UPSERT) == Flags::UPSERT);
	multi = ((flags & Flags::MULTI) == Flags::MULTI);

	selector = bson::BSONObj((const char*)ptr);
	ptr += selector.objsize();
	update = bson::BSONObj((const char*)ptr);
	ptr += update.objsize();

	ASSERT(ptr == eom);
}

std::string ExtMsgUpdate::toString() {
	return format("UPDATE: selector=%s, update=%s, collection=%s, flags=%d (%s)", selector.toString().c_str(),
	              update.toString().c_str(), fullCollNameToString(ns).c_str(), flags, header->toString().c_str());
}

void staticValidateModifiedFields(std::string fieldName,
                                  std::set<std::string>* affectedFields,
                                  std::set<std::string>* prefixesOfAffectedFields) {
	if (affectedFields->find(fieldName) != affectedFields->end())
		throw field_name_duplication_with_mods();
	if (prefixesOfAffectedFields->find(fieldName) != prefixesOfAffectedFields->end())
		throw conflicting_mods_in_update();
	affectedFields->insert(fieldName);
	auto upOneFn = upOneLevel(fieldName);
	if (!upOneFn.empty()) {
		while (!upOneFn.empty()) {
			if (affectedFields->find(upOneFn) != affectedFields->end())
				throw conflicting_mods_in_update();
			prefixesOfAffectedFields->insert(upOneFn);
			upOneFn = upOneLevel(upOneFn);
		}
	}
}

std::vector<std::string> staticValidateUpdateObject(bson::BSONObj update, bool multi, bool upsert) {
	std::set<std::string> affectedFields;
	std::set<std::string> prefixesOfAffectedFields;
	for (auto i = update.begin(); i.more();) {

		auto el = i.next();
		std::string operatorName = el.fieldName();
		if (!el.isABSONObj() || el.Obj().nFields() == 0) {
			throw update_operator_empty_parameter();
		}

		if (upsert && operatorName == DocLayerConstants::SET_ON_INSERT)
			operatorName = DocLayerConstants::SET;

		for (auto j = el.Obj().begin(); j.more();) {
			bson::BSONElement subel = j.next();
			auto fn = std::string(subel.fieldName());
			if (fn == DocLayerConstants::ID_FIELD) {
				if (operatorName != DocLayerConstants::SET && operatorName != DocLayerConstants::SET_ON_INSERT) {
					throw cant_modify_id();
				}
			}
			staticValidateModifiedFields(fn, &affectedFields, &prefixesOfAffectedFields);
			if (operatorName == DocLayerConstants::RENAME) {
				if (!subel.isString())
					throw bad_rename_target();
				staticValidateModifiedFields(subel.String(), &affectedFields, &prefixesOfAffectedFields);
			}
		}
	}

	std::vector<std::string> bannedIndexFields;
	bannedIndexFields.insert(std::end(bannedIndexFields), std::begin(affectedFields), std::end(affectedFields));
	bannedIndexFields.insert(std::end(bannedIndexFields), std::begin(prefixesOfAffectedFields),
	                         std::end(prefixesOfAffectedFields));

	return bannedIndexFields;
}

bool shouldCreateRoot(std::string operatorName) {
	return operatorName == DocLayerConstants::SET || operatorName == DocLayerConstants::INC ||
	       operatorName == DocLayerConstants::MUL || operatorName == DocLayerConstants::CURRENT_DATE ||
	       operatorName == DocLayerConstants::MAX || operatorName == DocLayerConstants::MIN ||
	       operatorName == DocLayerConstants::PUSH || operatorName == DocLayerConstants::ADD_TO_SET;
}

ACTOR Future<Void> updateDocument(Reference<IReadWriteContext> cx,
                                  bson::BSONObj update,
                                  bool upsert,
                                  Future<Standalone<StringRef>> fEncodedId) {
	state std::vector<Future<Void>> futures;
	state Standalone<StringRef> encodedId = wait(fEncodedId);
	for (auto i = update.begin(); i.more();) {
		auto el = i.next();

		std::string operatorName = el.fieldName();
		if (upsert && operatorName == DocLayerConstants::SET_ON_INSERT)
			operatorName = DocLayerConstants::SET;
		for (auto j = el.Obj().begin(); j.more();) {
			bson::BSONElement subel = j.next();
			auto fn = std::string(subel.fieldName());
			if (fn == DocLayerConstants::ID_FIELD) {
				if (operatorName == DocLayerConstants::SET || operatorName == DocLayerConstants::SET_ON_INSERT) {
					if (extractEncodedIds(subel.wrap()).get().keyEncoded != encodedId) {
						throw cant_modify_id();
					}
				}
			}
			if (shouldCreateRoot(operatorName)) {
				auto upOneFn = upOneLevel(fn);
				if (!upOneFn.empty())
					futures.push_back(ensureValidObject(cx, upOneFn, getLastPart(fn), true));
			} else if (operatorName != DocLayerConstants::SET_ON_INSERT) {
				// Don't bother checking/setting the targeted field if we hit
				// $setOnInsert and aren't upserting
				auto upOneFn = upOneLevel(fn);
				if (!upOneFn.empty())
					futures.push_back(ensureValidObject(cx, upOneFn, getLastPart(fn), false));
			}
			if (operatorName == DocLayerConstants::RENAME) {
				auto renameTarget = subel.String();
				auto upOneRenameTarget = upOneLevel(renameTarget);
				if (!upOneRenameTarget.empty())
					futures.push_back(ensureValidObject(cx, renameTarget, upOneRenameTarget, false));
			}

			// SOMEDAY: If the query document which generated this update request specified an exact match on a field
			// name, we can potentially save time by directly transforming the stored document, and figuring out what
			// the result has to be, thus converting e.g. $inc and $bit into $set, etc.
			futures.push_back(ExtUpdateOperator::execute(operatorName, cx, encodeMaybeDotted(fn), subel));
		}
	}
	wait(waitForAll(futures));
	return Void();
}

ACTOR Future<WriteCmdResult> doUpdateCmd(Namespace ns,
                                         bool ordered,
                                         std::vector<ExtUpdateCmd>* cmds,
                                         Reference<ExtConnection> ec) {
	state WriteCmdResult cmdResult;
	state int idx;
	for (idx = 0; idx < cmds->size(); idx++) {
		try {
			state ExtUpdateCmd* cmd = &((*cmds)[idx]);
			state int isoperatorUpdate = hasOperatorFieldnames(cmd->update, 0);
			state std::vector<std::string> bannedIndexFields;

			if (!isoperatorUpdate && cmd->multi) {
				throw literal_multi_update();
			}

			if (isoperatorUpdate) {
				bannedIndexFields = staticValidateUpdateObject(cmd->update, cmd->multi, cmd->upsert);
			}

			state Optional<bson::BSONObj> upserted = Optional<bson::BSONObj>();
			state Reference<DocTransaction> dtr = ec->getOperationTransaction();
			Reference<UnboundCollectionContext> ocx = wait(ec->mm->getUnboundCollectionContext(dtr, ns));
			state Reference<UnboundCollectionContext> cx =
			    Reference<UnboundCollectionContext>(new UnboundCollectionContext(*ocx));
			cx->setBannedFieldNames(bannedIndexFields);

			Reference<IUpdateOp> updater;
			Reference<IInsertOp> upserter;
			if (isoperatorUpdate)
				updater = operatorUpdate(cmd->update);
			else
				updater = replaceUpdate(cmd->update);
			if (cmd->upsert) {
				if (isoperatorUpdate)
					upserter = operatorUpsert(cmd->selector, cmd->update);
				else
					upserter = simpleUpsert(cmd->selector, cmd->update);
			}

			state Reference<DocInserter> docInserter;
			Reference<Plan> plan = planQuery(cx, cmd->selector);
			plan =
			    ref(new UpdatePlan(plan, updater, upserter, cmd->multi ? std::numeric_limits<int64_t>::max() : 1, cx));
			
			if (strcmp(ns.first.c_str(), DocLayerConstants::OPLOG_DB.c_str()) != 0) {
				docInserter = ref(new DocInserter(ec->watcher));
				plan = ec->wrapOperationPlanOplog(plan, docInserter, cx);
			} else {
				plan = ec->wrapOperationPlan(plan, false, cx);
			}

			std::pair<int64_t, Reference<ScanReturnedContext>> pair =
			    wait(executeUntilCompletionAndReturnLastTransactionally(plan, dtr));
			cmdResult.n += pair.first;

			if (docInserter.isValid()) {
				docInserter->commit();
			}

			if (cmd->upsert && pair.first == 1 && pair.second->scanId() == -1) {
				Standalone<StringRef> upsertedId = wait(pair.second->getKeyEncodedId());
				cmdResult.upsertedOIDList.push_back(upsertedId);
			}
		} catch (Error& e) {
			TraceEvent(SevError, "ExtMsgUpdateFailure").error(e);
			// clang-format off
			cmdResult.writeErrors.push_back(BSON("index" << idx <<
			                                     "code" << e.code() <<
			                                     "$err" << e.what() <<
			                                     "errmsg" << e.what()));
			// clang-format on
			if (ordered)
				break;
		}
	}

	cmdResult.nModified = cmdResult.n - cmdResult.upsertedOIDList.size();
	return cmdResult;
}

ACTOR static Future<WriteResult> doUpdateMsg(Future<Void> readyToWrite,
                                             Reference<ExtMsgUpdate> msg,
                                             Reference<ExtConnection> ec) {
	wait(readyToWrite);
	state std::vector<ExtUpdateCmd> cmds;
	cmds.emplace_back(msg->selector, msg->update, msg->upsert, msg->multi);
	WriteCmdResult cmdResult = wait(doUpdateCmd(msg->ns, true, &cmds, ec));
	if (cmdResult.writeErrors.empty()) {
		return WriteResult(cmdResult, WriteType::UPDATE);
	} else {
		throw Error(cmdResult.writeErrors[0].getField("code").Int());
	}
}

Future<Void> ExtMsgUpdate::run(Reference<ExtConnection> ec) {
	return ec->afterWrite(doUpdateMsg(ec->beforeWrite(), Reference<ExtMsgUpdate>::addRef(this), ec));
}

ExtMsgGetMore::ExtMsgGetMore(ExtMsgHeader* header, const uint8_t* body) : header(header) {
	const uint8_t* ptr = body;
	const uint8_t* eom = (const uint8_t*)header + header->messageLength;

	// Mongo OP_GET_MORE begins with int32 ZERO, reserved for future use
	ptr += sizeof(int32_t);

	const char* collName = (const char*)ptr;
	ptr += strlen(collName) + 1;
	ns = getDBCollectionPair(collName, std::make_pair("msgType", "OP_GETMORE"));

	numberToReturn = *(int32_t*)ptr;
	ptr += sizeof(int32_t);
	cursorID = *(int64_t*)ptr;
	ptr += sizeof(int64_t);

	ASSERT(ptr == eom);
}

std::string ExtMsgGetMore::toString() {
	return format("GET_MORE: collection=%s, numberToReturn=%d, cursorID=%d (%s)", fullCollNameToString(ns).c_str(),
	              numberToReturn, cursorID, header->toString().c_str());
}

ACTOR static Future<Void> doGetMoreRun(Reference<ExtMsgGetMore> getMore, Reference<ExtConnection> ec) {
	state Reference<ExtMsgReply> reply = Reference<ExtMsgReply>(new ExtMsgReply(getMore->header));
	state Reference<Cursor> cursor = ec->cursors[getMore->cursorID];

	if (!cursor) {
		cursor = Cursor::get(getMore->cursorID);
	}

	if (cursor) {	
		try {
			int32_t returned = wait(addDocumentsFromCursor(cursor, reply, getMore->numberToReturn));
			reply->replyHeader.startingFrom = cursor->returned - returned;
			reply->addResponseFlag(8 /*0b1000*/);
			cursor->refresh();
		} catch (Error& e) {
			reply->setError(e);
		}
	} else {
		reply->addResponseFlag(1 /*0b0001*/);
	}

	reply->write(ec);
	return Void();
}

Future<Void> ExtMsgGetMore::run(Reference<ExtConnection> ec) {
	return doGetMoreRun(Reference<ExtMsgGetMore>::addRef(this), ec);
}

ExtMsgDelete::ExtMsgDelete(ExtMsgHeader* header, const uint8_t* body) : header(header) {
	const uint8_t* ptr = body;
	const uint8_t* eom = (const uint8_t*)header + header->messageLength;

	// Mongo OP_DELETE begins with int32 ZERO, reserved for future use
	ptr += sizeof(int32_t);

	const char* collName = (const char*)ptr;
	ptr += strlen(collName) + 1;
	ns = getDBCollectionPair(collName, std::make_pair("msgType", "OP_DELETE"));

	flags = *(int32_t*)ptr;
	ptr += sizeof(int32_t);

	bson::BSONObj selector = bson::BSONObj((const char*)ptr);
	ptr += selector.objsize();

	// If 0th bit is set, limit should be just one document or all documents (0).
	selectors.push_back(BSON("q" << selector << "limit" << (flags & 1)));

	ASSERT(ptr == eom);
}

std::string ExtMsgDelete::toString() {
	std::ostringstream stringStream;

	stringStream << "[";
	for (int i = 0; i < selectors.size(); i++) {
		if (i)
			stringStream << ",";
		stringStream << selectors[i].toString();
	}
	stringStream << "]";

	return format("DELETE: %s, collection=%s, flags=%d (%s)", stringStream.str().c_str(),
	              fullCollNameToString(ns).c_str(), flags, header->toString().c_str());
}

ACTOR Future<WriteCmdResult> doDeleteCmd(Namespace ns,
                                         bool ordered,
                                         std::vector<bson::BSONObj>* selectors,
                                         Reference<ExtConnection> ec) {
	try {
		state Reference<DocTransaction> dtr = ec->getOperationTransaction();
		state Reference<UnboundCollectionContext> cx;
		state int64_t nrDeletedRecords = 0;
		state std::vector<bson::BSONObj> writeErrors;

		// If collection not found then just return success from here.
		try {
			Reference<UnboundCollectionContext> _cx = wait(ec->mm->getUnboundCollectionContext(dtr, ns, false, false));
			cx = _cx;
		} catch (Error& e) {
			if (e.code() == error_code_collection_not_found)
				return WriteCmdResult(nrDeletedRecords, writeErrors);
			throw e;
		}

		state std::vector<bson::BSONObj>::iterator it;
		state int idx;
		for (it = selectors->begin(), idx = 0; it != selectors->end(); it++, idx++) {
			try {
				state Reference<DocInserter> docInserter;
				Reference<Plan> plan = planQuery(cx, it->getField("q").Obj());
				const int64_t limit = it->getField("limit").numberLong();
				plan = deletePlan(plan, cx, limit == 0 ? std::numeric_limits<int64_t>::max() : limit);

				if (strcmp(ns.first.c_str(), DocLayerConstants::OPLOG_DB.c_str()) != 0) {
					docInserter = ref(new DocInserter(ec->watcher));
					plan = ec->wrapOperationPlanOplog(plan, docInserter, cx);
				} else {
					plan = ec->wrapOperationPlan(plan, false, cx);
				}

				// TODO: BM: <rdar://problem/40661843> DocLayer: Make bulk deletes efficient
				int64_t deletedRecords = wait(executeUntilCompletionTransactionally(plan, dtr));
				nrDeletedRecords += deletedRecords;

				if (docInserter.isValid()) {
					docInserter->commit();
				}
			} catch (Error& e) {
				TraceEvent(SevError, "ExtMsgDeleteFailure").error(e);
				// clang-format off
				writeErrors.push_back(BSON("index" << idx <<
				                           "code" << e.code() <<
				                           "$err" << e.what() <<
				                           "errmsg" << e.what()));
				// clang-format on
				if (ordered)
					break;
			}
		}

		return WriteCmdResult(nrDeletedRecords, writeErrors);
	} catch (Error& e) {
		throw;
	}
}

ACTOR static Future<WriteResult> doDeleteMsg(Future<Void> readyToWrite,
                                             Reference<ExtMsgDelete> msg,
                                             Reference<ExtConnection> ec) {
	wait(readyToWrite);
	WriteCmdResult cmdResult = wait(doDeleteCmd(msg->ns, true, &msg->selectors, ec));
	if (cmdResult.writeErrors.empty())
		return WriteResult(cmdResult, WriteType::REMOVAL);
	else
		throw Error(cmdResult.writeErrors[0].getField("code").Int());
}

Future<Void> ExtMsgDelete::run(Reference<ExtConnection> ec) {
	return ec->afterWrite(doDeleteMsg(ec->beforeWrite(), Reference<ExtMsgDelete>::addRef(this), ec));
}

ExtMsgKillCursors::ExtMsgKillCursors(ExtMsgHeader* header, const uint8_t* body) : header(header) {
	const uint8_t* ptr = body;
	const uint8_t* eom = (const uint8_t*)header + header->messageLength;

	// Mongo OP_KILL_CURSORS begins with int32 ZERO, reserved for future use
	ptr += sizeof(int32_t);
	numberOfCursorIDs = *(int32_t*)ptr;
	// Totally unnecessary for this to be in the message given that we know the overall message length...
	ptr += sizeof(int32_t);
	cursorIDs = new int64_t[numberOfCursorIDs];

	memcpy(cursorIDs, ptr, numberOfCursorIDs * sizeof(int64_t));
	ptr += numberOfCursorIDs * sizeof(int64_t);

	ASSERT(ptr == eom);
}

std::string ExtMsgKillCursors::toString() {
	return format("KILL_CURSORS: ");
}

Future<Void> doKillCursorsRun(Reference<ExtMsgKillCursors> msg, Reference<ExtConnection> ec) {
	int64_t* ptr = msg->cursorIDs;

	// FIXME: I'm not quite sure what the contract around the memory owned by
	// BufferedConnection is. So do this copy for now to be conservative.
	int32_t numberOfCursorIDs = msg->numberOfCursorIDs;

	while (numberOfCursorIDs--) {
		Cursor::pluck(ec->cursors[*ptr++]);
	}

	return Future<Void>(Void());
}

Future<Void> ExtMsgKillCursors::run(Reference<ExtConnection> ec) {
	return doKillCursorsRun(Reference<ExtMsgKillCursors>::addRef(this), ec);
}

/* FIXME: These don't really belong here*/

struct ExtOperatorUpdate : ConcreteUpdateOp<ExtOperatorUpdate> {
	bson::BSONObj msgUpdate;

	explicit ExtOperatorUpdate(bson::BSONObj const& msgUpdate) : msgUpdate(msgUpdate) {}

	Future<Void> update(Reference<IReadWriteContext> document) override {
		return updateDocument(document, msgUpdate, false, document->getKeyEncodedId());
	}

	std::string describe() override { return "OperatorUpdate(" + msgUpdate.toString() + ")"; }
};

struct ExtReplaceUpdate : ConcreteUpdateOp<ExtReplaceUpdate> {
	bson::BSONObj replaceWith;

	explicit ExtReplaceUpdate(bson::BSONObj const& replaceWith) : replaceWith(replaceWith) {}

	Future<Void> update(Reference<IReadWriteContext> document) override { return replacementActor(this, document); }

	std::string describe() override { return "ReplaceWith(" + replaceWith.toString() + ")"; }

	ACTOR static Future<Void> replacementActor(ExtReplaceUpdate* self, Reference<IReadWriteContext> document) {
		state Standalone<StringRef> keyEncodedId = wait(document->getKeyEncodedId());
		Optional<IdInfo> encodedIds = extractEncodedIds(self->replaceWith);
		if (encodedIds.present() && encodedIds.get().keyEncoded != keyEncodedId) {
			throw replace_with_id();
		}

		Optional<DataValue> iddv2 =
		    wait(document->get(DataValue(DocLayerConstants::ID_FIELD, DVTypeCode::STRING).encode_key_part()));
		document->clearDescendants();

		if (iddv2.get().getBSONType() == bson::BSONType::Object) {
			DataValue iddv = DataValue::decode_key_part(keyEncodedId);
			if (iddv.getBSONType() == bson::BSONType::Object) {
				insertElementRecursive(iddv.wrap(DocLayerConstants::ID_FIELD).getField(DocLayerConstants::ID_FIELD),
				                       document);
			}
		} else {
			document->set(DataValue(DocLayerConstants::ID_FIELD, DVTypeCode::STRING).encode_key_part(),
			              iddv2.get().encode_value());
		}

		for (auto i = self->replaceWith.begin(); i.more();) {
			auto e = i.next();
			insertElementRecursive(e, document);
		}
		document->set(StringRef(), DataValue::subObject().encode_value());
		return Void();
	}
};

struct ExtOperatorUpsert : ConcreteInsertOp<ExtOperatorUpsert> {
	bson::BSONObj selector;
	bson::BSONObj update;

	ExtOperatorUpsert(bson::BSONObj selector, bson::BSONObj update) : selector(selector), update(update) {}

	ACTOR static Future<Reference<IReadWriteContext>> upsertActor(ExtOperatorUpsert* self,
	                                                              Reference<CollectionContext> cx) {
		Optional<IdInfo> encodedIds = extractEncodedIds(self->selector);
		if (!encodedIds.present())
			encodedIds = extractEncodedIdsFromUpdate(self->update);
		int selectorOperators = hasOperatorFieldnames(self->selector, -1, 0);
		bson::BSONObj thingToInsert;
		if (!selectorOperators) {
			thingToInsert = self->selector;
		} else {
			thingToInsert = transformOperatorQueryToUpdatableDocument(self->selector);
		}
		state Reference<IReadWriteContext> dcx = wait(insertDocument(cx, thingToInsert, encodedIds));
		wait(updateDocument(dcx, self->update, true, dcx->getKeyEncodedId()));
		return dcx;
	}

	Future<Reference<IReadWriteContext>> insert(Reference<CollectionContext> cx) override {
		return upsertActor(this, cx);
	}

	std::string describe() override { return "OperatorUpsert(" + selector.toString() + "," + update.toString() + ")"; }
};

struct ExtSimpleUpsert : ConcreteInsertOp<ExtSimpleUpsert> {
	bson::BSONObj selector;
	bson::BSONObj update;

	ExtSimpleUpsert(bson::BSONObj selector, bson::BSONObj update) : selector(selector), update(update) {}

	std::string describe() override { return "SimpleUpsert(" + selector.toString() + "," + update.toString() + ")"; }

	Future<Reference<IReadWriteContext>> insert(Reference<CollectionContext> cx) override {
		Optional<IdInfo> encodedIds = extractEncodedIds(selector);
		Optional<IdInfo> updateIds = extractEncodedIds(update);
		if (!encodedIds.present()) {
			encodedIds = updateIds;
		} else if (updateIds.present() && updateIds.get().keyEncoded != encodedIds.get().keyEncoded) {
			throw cant_modify_id();
		}
		return insertDocument(cx, update, encodedIds);
	}
};

Reference<IUpdateOp> operatorUpdate(bson::BSONObj const& msgUpdate) {
	return ref(new ExtOperatorUpdate(msgUpdate));
}
Reference<IUpdateOp> replaceUpdate(bson::BSONObj const& replaceWith) {
	return ref(new ExtReplaceUpdate(replaceWith));
}
Reference<IInsertOp> simpleUpsert(bson::BSONObj const& selector, bson::BSONObj const& update) {
	return ref(new ExtSimpleUpsert(selector, update));
}
Reference<IInsertOp> operatorUpsert(bson::BSONObj const& selector, bson::BSONObj const& update) {
	return ref(new ExtOperatorUpsert(selector, update));
}

Future<Reference<IReadWriteContext>> DocInserter::insert(Reference<CollectionContext> cx, bson::BSONObj obj) {
	bson::BSONElement el;
	if (obj.getObjectID(el)) {	
		objs.insert(std::pair<std::string, bson::BSONObj>(
			el.OID().toString(),
			obj.getOwned()
		));
	}	

	return insertDocument(cx, obj, extractEncodedIds(obj));
}

void DocInserter::commit() {
	watcher->log(objs);
}
