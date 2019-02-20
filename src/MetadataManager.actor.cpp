/*
 * MetadataManager.actor.cpp
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

#include "DocumentError.h"
#include "ExtStructs.h"
#include "ExtUtil.actor.h"
#include "MetadataManager.h"

using namespace FDB;

Future<uint64_t> getMetadataVersion(Reference<DocTransaction> tr, Reference<DirectorySubspace> metadataDirectory) {
	std::string versionKey =
	    metadataDirectory->key().toString() + DataValue("version", DVTypeCode::STRING).encode_key_part();
	Future<Optional<FDBStandalone<StringRef>>> fov = tr->tr->get(StringRef(versionKey));
	Future<uint64_t> ret = map(fov, [](Optional<FDBStandalone<StringRef>> ov) -> uint64_t {
		if (!ov.present())
			return 0;
		else
			return *((uint64_t*)(ov.get().begin()));
	});
	return ret;
}

std::string describeIndex(std::vector<std::pair<std::string, int>> indexKeys) {
	std::string ret = "index: ";
	for (const auto& indexKey : indexKeys) {
		ret += format("{%s:%d}, ", indexKey.first.c_str(), indexKey.second);
	}
	ret.resize(ret.length() - 2);
	return ret;
}

IndexInfo::IndexStatus indexStatus(const bson::BSONObj& indexObj) {
	const char* statusField = indexObj.getStringField("status");
	if (strcmp(statusField, "ready") == 0)
		return IndexInfo::IndexStatus::READY;
	else if (strcmp(statusField, "building") == 0)
		return IndexInfo::IndexStatus::BUILDING;
	else
		return IndexInfo::IndexStatus::INVALID;
}

IndexInfo MetadataManager::indexInfoFromObj(const bson::BSONObj& indexObj, Reference<UnboundCollectionContext> cx) {
	IndexInfo::IndexStatus status = indexStatus(indexObj);
	bson::BSONObj keyObj = indexObj.getObjectField("key");
	std::vector<std::pair<std::string, int>> indexKeys;
	indexKeys.reserve(keyObj.nFields());
	bool isUniqueIndex = indexObj.hasField("unique") ? indexObj.getBoolField("unique") : false;
	for (auto i = keyObj.begin(); i.more();) {
		auto e = i.next();
		indexKeys.emplace_back(e.fieldName(), (int)e.Number());
	}
	if (verboseLogging) {
		TraceEvent("BD_getAndAddIndexes").detail("AddingIndex", describeIndex(indexKeys));
	}
	if (verboseConsoleOutput) {
		fprintf(stderr, "%s\n\n", describeIndex(indexKeys).c_str());
	}
	if (status == IndexInfo::IndexStatus::BUILDING) {
		return IndexInfo(indexObj.getStringField("name"), indexKeys, cx, status,
		                 UID::fromString(indexObj.getStringField("build id")), isUniqueIndex);
	} else {
		return IndexInfo(indexObj.getStringField("name"), indexKeys, cx, status, Optional<UID>(), isUniqueIndex);
	}
}

ACTOR static Future<std::pair<Reference<UnboundCollectionContext>, uint64_t>>
constructContext(Namespace ns, Reference<DocTransaction> tr, DocumentLayer* docLayer, bool includeIndex, bool createIfAbsent) {
	try {
		// The initial set of directory reads take place in a separate transaction with the same read version as `tr'.
		// This hopefully prevents us from accidentally RYWing a directory that `tr' itself created, and then adding it
		// to the cache, when there's a chance that `tr' won't commit.
		state Reference<FDB::Transaction> snapshotTr(new Transaction(docLayer->database));
		FDB::Version v = wait(tr->tr->getReadVersion());
		snapshotTr->setVersion(v);
		state Future<Reference<DirectorySubspace>> fcollectionDirectory =
		    docLayer->rootDirectory->open(snapshotTr, {StringRef(ns.first), StringRef(ns.second)});
		state Future<Reference<DirectorySubspace>> findexDirectory =
		    docLayer->rootDirectory->open(snapshotTr, {StringRef(ns.first), LiteralStringRef("system.indexes")});
		state Reference<DirectorySubspace> metadataDirectory = wait(docLayer->rootDirectory->open(
		    snapshotTr, {StringRef(ns.first), StringRef(ns.second), LiteralStringRef("metadata")}));

		state Future<uint64_t> fv = getMetadataVersion(tr, metadataDirectory);
		state Reference<DirectorySubspace> collectionDirectory = wait(fcollectionDirectory);
		state Reference<DirectorySubspace> indexDirectory = wait(findexDirectory);
		state Reference<UnboundCollectionContext> cx =
		    Reference<UnboundCollectionContext>(new UnboundCollectionContext(collectionDirectory, metadataDirectory));

		// Only include existing indexes into the context when it's NOT building a new index.
		// When it's building a new index, it's unnecessary and inefficient to pass each recorded returned by a
		// TableScan through the existing indexes.
		if (includeIndex) {
			state Reference<UnboundCollectionContext> indexCx = Reference<UnboundCollectionContext>(
			    new UnboundCollectionContext(indexDirectory, Reference<DirectorySubspace>()));
			state Reference<Plan> indexesPlan = getIndexesForCollectionPlan(indexCx, ns);
			std::vector<bson::BSONObj> allIndexes = wait(getIndexesTransactionally(indexesPlan, tr));

			for (const auto& indexObj : allIndexes) {
				IndexInfo index = MetadataManager::indexInfoFromObj(indexObj, cx);
				if (index.status != IndexInfo::IndexStatus::INVALID) {
					cx->addIndex(index);
				}
			}
		}

		// fprintf(stderr, "%s.%s Reading: Collection dir: %s Metadata dir:%s Caller:%s\n", dbName.c_str(),
		// collectionName.c_str(), printable(collectionDirectory->key()).c_str(),
		// printable(metadataDirectory->key()).c_str(), "");
		uint64_t version = wait(fv);
		return std::make_pair(cx, version);
	} catch (Error& e) {
		if (e.code() != error_code_directory_does_not_exist && e.code() != error_code_parent_directory_does_not_exist)
			throw;
		// In this case, one or more of the directories didn't exist, so this is "implicit collection creation", so
		// there are no indexes and no version.

		bool rootExists = wait(docLayer->rootDirectory->exists(tr->tr));
		if (!rootExists)
			throw doclayer_metadata_changed();

		if (!createIfAbsent)
			throw collection_not_found();

		// NB: These directory creations are not parallelized deliberately, because it is unsafe to create directories
		// in parallel with the same transaction in the Flow directory layer.
		state Reference<DirectorySubspace> tcollectionDirectory =
		    wait(docLayer->rootDirectory->createOrOpen(tr->tr, {StringRef(ns.first), StringRef(ns.second)}));
		state Reference<DirectorySubspace> tindexDirectory = wait(
		    docLayer->rootDirectory->createOrOpen(tr->tr, {StringRef(ns.first), LiteralStringRef("system.indexes")}));
		state Reference<DirectorySubspace> tmetadataDirectory = wait(docLayer->rootDirectory->createOrOpen(
		    tr->tr, {StringRef(ns.first), StringRef(ns.second), LiteralStringRef("metadata")}));
		state Reference<UnboundCollectionContext> tcx =
		    Reference<UnboundCollectionContext>(new UnboundCollectionContext(tcollectionDirectory, tmetadataDirectory));
		// fprintf(stderr, "%s.%s Creating: Collection dir: %s Metadata dir:%s Caller:%s\n", dbName.c_str(),
		// collectionName.c_str(), printable(tcollectionDirectory->key()).c_str(),
		// printable(tmetadataDirectory->key()).c_str(), "");
		tcx->bindCollectionContext(tr)->bumpMetadataVersion(); // We start at version 1.

		return std::make_pair(tcx, -1); // So we don't pollute the cache in case this transaction never commits
	}
}

ACTOR static Future<Reference<UnboundCollectionContext>> assembleCollectionContext(Reference<DocTransaction> tr,
                                                                                   Namespace ns,
                                                                                   Reference<MetadataManager> self,
                                                                                   bool includeIndex,
                                                                                   bool createIfAbsent) {
	if (self->contexts.size() > 100)
		self->contexts.clear();

	auto match = self->contexts.find(ns);

	if (match == self->contexts.end()) {
		std::pair<Reference<UnboundCollectionContext>, uint64_t> unboundPair =
		    wait(constructContext(ns, tr, self->docLayer, includeIndex, createIfAbsent));

		// Here and below don't pollute the cache if we just created the directory, since this transaction might
		// not commit.
		if (unboundPair.second != -1) {
			auto insert_result = self->contexts.insert(std::make_pair(ns, unboundPair));
			// Somebody else may have done the lookup and finished ahead of us. Either way, replace it with ours (can no
			// longer optimize this by only replacing if ours is newer, because the directory may have moved or
			// vanished.
			if (!insert_result.second) {
				insert_result.first->second = unboundPair;
			}
		}
		return unboundPair.first;
	} else {
		state uint64_t oldVersion = (*match).second.second;
		state Reference<UnboundCollectionContext> oldUnbound = (*match).second.first;
		uint64_t version = wait(getMetadataVersion(tr, oldUnbound->metadataDirectory));
		if (version != oldVersion) {
			std::pair<Reference<UnboundCollectionContext>, uint64_t> unboundPair =
			    wait(constructContext(ns, tr, self->docLayer, includeIndex, createIfAbsent));
			if (unboundPair.second != -1) {
				// Create the iterator again instead of making the previous value state, because the map could have
				// changed during the previous wait. Either way, replace it with ours (can no longer optimize this by
				// only replacing if ours is newer, because the directory may have moved or vanished.
				// std::map<std::pair<std::string, std::string>, std::pair<Reference<UnboundCollectionContext>,
				// uint64_t>>::iterator match = self->contexts.find(ns);
				auto match = self->contexts.find(ns);

				if (match != self->contexts.end())
					match->second = unboundPair;
				else
					self->contexts.insert(std::make_pair(ns, unboundPair));
			}
			return unboundPair.first;
		} else {
			return oldUnbound;
		}
	}
}

Future<Reference<UnboundCollectionContext>> MetadataManager::getUnboundCollectionContext(Reference<DocTransaction> tr,
                                                                                         Namespace const& ns,
                                                                                         bool allowSystemNamespace,
                                                                                         bool includeIndex,
                                                                                         bool createIfAbsent) {
	if (!allowSystemNamespace && startsWith(ns.second.c_str(), "system."))
		throw write_system_namespace();
	return assembleCollectionContext(tr, ns, Reference<MetadataManager>::addRef(this), includeIndex, createIfAbsent);
}

Future<Reference<UnboundCollectionContext>> MetadataManager::refreshUnboundCollectionContext(
    Reference<UnboundCollectionContext> cx,
    Reference<DocTransaction> tr) {
	return assembleCollectionContext(tr, std::make_pair(cx->databaseName(), cx->collectionName()),
	                                 Reference<MetadataManager>::addRef(this), false, false);
}

ACTOR static Future<Void> buildIndex_impl(bson::BSONObj indexObj,
                                          Namespace ns,
                                          Standalone<StringRef> encodedIndexId,
                                          Reference<ExtConnection> ec,
                                          UID build_id) {
	state IndexInfo info;
	try {
		state Reference<DocTransaction> tr = ec->getOperationTransaction();
		state Reference<UnboundCollectionContext> mcx = wait(ec->mm->getUnboundCollectionContext(tr, ns, false, false));
		info = MetadataManager::indexInfoFromObj(indexObj, mcx);
		info.status = IndexInfo::IndexStatus::BUILDING;
		info.buildId = build_id;
		mcx->addIndex(info);

		state Reference<Plan> buildingPlan = ec->wrapOperationPlan(
		    ref(new BuildIndexPlan(ref(new TableScanPlan(mcx)), info, ns.first, encodedIndexId, ec->mm)), false, mcx);
		int64_t _ = wait(executeUntilCompletionTransactionally(buildingPlan, tr));

		state Reference<Plan> finalizePlan = ec->isolatedWrapOperationPlan(
		    ref(new UpdateIndexStatusPlan(ns, encodedIndexId, ec->mm, std::string("ready"), info.buildId)), 0, -1);
		int64_t _ = wait(executeUntilCompletionTransactionally(finalizePlan, ec->getOperationTransaction()));

		return Void();
	} catch (Error& e) {
		state Error err = e;
		// try forever to set the index into an error status (unless somebody comes along before us and starts a
		// different build)
		loop {
			state bool okay;
			// Providing the build id here is sufficient to avoid clobbering a "ready" index as well, since
			// UpdateIndexStatusPlan, if it has that optional parameter, will return an error in the event that the
			// buildId field does not exist (as is the case for 'ready' indexes).
			state Reference<Plan> errorPlan = ec->isolatedWrapOperationPlan(
			    ref(new UpdateIndexStatusPlan(ns, encodedIndexId, ec->mm, std::string("error"), info.buildId)), 0, -1);
			try {
				int64_t _ = wait(executeUntilCompletionTransactionally(errorPlan, ec->getOperationTransaction()));
				okay = true;
			} catch (Error& e) {
				if (e.code() == error_code_index_wrong_build_id)
					throw e;
				okay = false;
				// Otherwise, we hit some other non-retryable problem trying to set the index metadata to an error
				// status (perhaps commit_unknown_result). Go around the loop again.
			}
			if (okay)
				throw err;
		}
	}
}

Future<Void> MetadataManager::buildIndex(bson::BSONObj indexObj,
                                         Namespace const& ns,
                                         Standalone<StringRef> encodedIndexId,
                                         Reference<ExtConnection> ec,
                                         UID build_id) {
	return buildIndex_impl(indexObj, ns, encodedIndexId, ec, build_id);
}
