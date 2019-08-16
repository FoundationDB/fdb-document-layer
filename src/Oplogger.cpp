/*
 * Oplogger.cpp
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

#include "Oplogger.h"
#include "ExtUtil.actor.h"

Future<Reference<IReadWriteContext>> OplogActor::insertOp(Reference<CollectionContext> cx, 
												 std::string ns, 
												 bson::BSONObj obj) {
    bson::BSONObjBuilder builder;
	prepareBuilder(&builder, DocLayerConstants::OP_INSERT, ns);

	builder.append(DocLayerConstants::OP_FIELD_O, obj);

	return inserter->insert(cx, builder.obj());
}

Future<Reference<IReadWriteContext>> OplogActor::updateOp(Reference<CollectionContext> cx, 
                                                std::string ns, 
                                                bson::OID id, 
                                                bson::BSONObj obj) {
    bson::BSONObjBuilder builder;
	prepareBuilder(&builder, DocLayerConstants::OP_UPDATE, ns);

	builder.append(DocLayerConstants::OP_FIELD_O2, BSON(DocLayerConstants::ID_FIELD << id.toString()))
		   .append(DocLayerConstants::OP_FIELD_O, BSON("v" << 1 << "set" << obj));

	return inserter->insert(cx, builder.obj());
}

Future<Reference<IReadWriteContext>> OplogActor::deleteOp(Reference<CollectionContext> cx, 
                                                std::string ns, 
                                                bson::OID id) {
    bson::BSONObjBuilder builder;
	prepareBuilder(&builder, DocLayerConstants::OP_DELETE, ns);

	builder.append(DocLayerConstants::OP_FIELD_O, BSON(DocLayerConstants::ID_FIELD << id.toString()));

	return inserter->insert(cx, builder.obj());
}

void OplogActor::prepareBuilder(bson::BSONObjBuilder* builder, std::string op, std::string ns) {
	(*builder).append(DocLayerConstants::OP_FIELD_TS, (long long)(timer() * 1000))
		   	  .append(DocLayerConstants::OP_FIELD_V, int32_t(2))
		   	  .append(DocLayerConstants::OP_FIELD_H, (long long)(g_random->randomInt64(INT64_MIN, INT64_MAX)))
		      .append(DocLayerConstants::OP_FIELD_NS, ns)
		      .append(DocLayerConstants::OP_FIELD_OP, op);
}

Oplogger::Oplogger(Namespace operationNs, Reference<IOplogInserter> oplogInserter) {
    ns = fullCollNameToString(operationNs);
    actor = ref(new OplogActor(oplogInserter));
    oplogNs = Namespace(DocLayerConstants::OPLOG_DB, DocLayerConstants::OPLOG_COL);
    enabled = isValidNamespace(ns) && oplogInserter.isValid();
}

void Oplogger::reset() { 
    operations.clear(); 
}

bool Oplogger::isEnabled() {
    return enabled; 
}

Deque<Future<Reference<IReadWriteContext>>> Oplogger::buildOplogs(Reference<CollectionContext> ctx) {
    Deque<Future<Reference<IReadWriteContext>>> oplogs;

    for(auto o : operations) {
        if (o.second.second.isEmpty()) {
            oplogs.push_back(actor->deleteOp(ctx, ns, bson::OID(o.first)));
            continue;
        }

        if (o.second.first.isEmpty()) {
            oplogs.push_back(actor->insertOp(ctx, ns, o.second.second));
            continue;
        }

        oplogs.push_back(actor->updateOp(ctx, ns, bson::OID(o.first), o.second.second));
    }

    return oplogs;
}

void Oplogger::addOriginalDoc(const DataValue *dv) {
	bson::BSONObj oObj = dv->getPackedObject().getOwned();
    gatherObjectsInfo(oObj, true);
}

void Oplogger::addUpdatedDoc(const DataValue *dv) {
	bson::BSONObj oObj = dv->getPackedObject().getOwned();
    gatherObjectsInfo(oObj, false);
}

void Oplogger::addOriginalDoc(bson::BSONObj doc) {
    gatherObjectsInfo(doc, true);
}

void Oplogger::addUpdatedDoc(bson::BSONObj doc) {	
    gatherObjectsInfo(doc, false);
}

Future<Reference<UnboundCollectionContext>> Oplogger::getUnboundContext(Reference<MetadataManager> mm, 
                                                                        Reference<DocTransaction> tr) {
    return mm->getUnboundCollectionContext(tr, oplogNs);
}

void Oplogger::gatherObjectsInfo(bson::BSONObj oObj, bool isSource) {
	if (oObj.isEmpty()) {
		return;
	}

	bson::BSONElement oId;    
	if (!oObj.getObjectID(oId)) {
        return;
    }

	if (oId.isNull()) {
		return;
	}

	if (isSource) {
		operations[oId.OID().toString()] = std::make_pair(oObj, bson::BSONObj());
		return;
	}

	auto oIdStr = oId.OID().toString();	
	if (operations.count(oIdStr) > 0) {
		auto bobDiff = getUpdatedObjectsDifference(operations[oIdStr].first, oObj);
		
		if (bobDiff.isEmpty()) {
			bobDiff = getUpdatedObjectsDifference(oObj, operations[oIdStr].first, false);
		}

		if (bobDiff.isEmpty()) {
			operations.erase(oIdStr);
			return;
		}

		operations[oIdStr].second = bobDiff;
		return;
	}
	
	operations[oIdStr] = std::make_pair(bson::BSONObj(), oObj);
}

bool Oplogger::isValidNamespace(std::string ns) {
    return strcasecmp(ns.c_str(), fullCollNameToString(oplogNs).c_str()) != 0;
}
