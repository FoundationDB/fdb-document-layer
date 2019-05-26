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
#include "flow/actorcompiler.h" // This must be the last #include.

using namespace FDB;

std::string fullCollNameToString(Namespace const& ns) {
	return ns.first + "." + (ns.second.empty() ? "$cmd" : ns.second);
}

Future<uint64_t> getMetadataVersion(Reference<DocTransaction> tr, Reference<DirectorySubspace> metadataDirectory) {
	Standalone<StringRef> versionKey = metadataDirectory->key().withSuffix(
	    DataValue(DocLayerConstants::VERSION_KEY, DVTypeCode::STRING).encode_key_part());
	Future<Optional<FDBStandalone<StringRef>>> fov = tr->tr->get(versionKey);
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
	const char* statusField = indexObj.getStringField(DocLayerConstants::STATUS_FIELD);
	if (strcmp(statusField, DocLayerConstants::INDEX_STATUS_READY) == 0)
		return IndexInfo::IndexStatus::READY;
	else if (strcmp(statusField, DocLayerConstants::INDEX_STATUS_BUILDING) == 0)
		return IndexInfo::IndexStatus::BUILDING;
	else
		return IndexInfo::IndexStatus::INVALID;
}

Reference<IndexInfo> MetadataManager::indexInfoFromObj(const bson::BSONObj& indexObj,
                                                       Reference<UnboundCollectionContext> cx) {
	IndexInfo::IndexStatus status = indexStatus(indexObj);
	bson::BSONObj keyObj = indexObj.getObjectField(DocLayerConstants::KEY_FIELD);
	std::vector<std::pair<std::string, int>> indexKeys;
	indexKeys.reserve(keyObj.nFields());
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
		return Reference<IndexInfo>(
		    new IndexInfo(indexObj.getStringField(DocLayerConstants::NAME_FIELD), indexKeys, cx, status,
		                  UID::fromString(indexObj.getStringField(DocLayerConstants::BUILD_ID_FIELD)),
		                  indexObj.getBoolField(DocLayerConstants::UNIQUE_FIELD)));
	} else {
		return Reference<IndexInfo>(new IndexInfo(indexObj.getStringField(DocLayerConstants::NAME_FIELD), indexKeys, cx,
		                                          status, Optional<UID>(),
		                                          indexObj.getBoolField(DocLayerConstants::UNIQUE_FIELD)));
	}
}

ACTOR static Future<Reference<UnboundCollectionContext>> constructContext(Namespace ns,
                                                                          Reference<DocTransaction> tr,
                                                                          DocumentLayer* docLayer,
                                                                          bool createCollectionIfAbsent) {
	try {
		// The initial set of directory reads take place in a separate transaction with the same read version as `tr'.
		// This hopefully prevents us from accidentally RYWing a directory that `tr' itself created, and then adding it
		// to the cache, when there's a chance that `tr' won't commit.
		state Reference<FDB::Transaction> snapshotTr = docLayer->database->createTransaction();
		FDB::Version v = wait(tr->tr->getReadVersion());
		snapshotTr->setReadVersion(v);
		state Future<Reference<DirectorySubspace>> fcollectionDirectory =
		    docLayer->rootDirectory->open(snapshotTr, {StringRef(ns.first), StringRef(ns.second)});
		state Future<Reference<DirectorySubspace>> findexDirectory = docLayer->rootDirectory->open(
		    snapshotTr, {StringRef(ns.first), StringRef(DocLayerConstants::SYSTEM_INDEXES)});
		state Reference<DirectorySubspace> metadataDirectory = wait(docLayer->rootDirectory->open(
		    snapshotTr, {StringRef(ns.first), StringRef(ns.second), StringRef(DocLayerConstants::METADATA)}));

		state Future<uint64_t> fv = getMetadataVersion(tr, metadataDirectory);
		state Reference<DirectorySubspace> collectionDirectory = wait(fcollectionDirectory);
		state Reference<DirectorySubspace> indexDirectory = wait(findexDirectory);
		state Reference<UnboundCollectionContext> indexCx = Reference<UnboundCollectionContext>(
		    new UnboundCollectionContext(indexDirectory, Reference<DirectorySubspace>()));
		state Reference<Plan> indexesPlan = getIndexesForCollectionPlan(indexCx, ns);
		state std::vector<bson::BSONObj> allIndexes = wait(getIndexesTransactionally(indexesPlan, tr));

		uint64_t version = wait(fv);
		state Reference<UnboundCollectionContext> cx = Reference<UnboundCollectionContext>(
		    new UnboundCollectionContext(version, collectionDirectory, metadataDirectory));

		for (const auto& indexObj : allIndexes) {
			Reference<IndexInfo> index = MetadataManager::indexInfoFromObj(indexObj, cx);
			if (index->status != IndexInfo::IndexStatus::INVALID) {
				cx->addIndex(index);
			}
		}
		return cx;
	} catch (Error& e) {
		if (e.code() != error_code_directory_does_not_exist && e.code() != error_code_parent_directory_does_not_exist)
			throw;
		// In this case, one or more of the directories didn't exist, so this is "implicit collection creation", so
		// there are no indexes and no version.

		bool rootExists = wait(docLayer->rootDirectory->exists(tr->tr));
		if (!rootExists)
			throw doclayer_metadata_changed();

		if (!createCollectionIfAbsent)
			throw collection_not_found();

		// NB: These directory creations are not parallelized deliberately, because it is unsafe to create directories
		// in parallel with the same transaction in the Flow directory layer.
		state Reference<DirectorySubspace> tcollectionDirectory =
		    wait(docLayer->rootDirectory->createOrOpen(tr->tr, {StringRef(ns.first), StringRef(ns.second)}));
		state Reference<DirectorySubspace> tindexDirectory = wait(docLayer->rootDirectory->createOrOpen(
		    tr->tr, {StringRef(ns.first), StringRef(DocLayerConstants::SYSTEM_INDEXES)}));
		state Reference<DirectorySubspace> tmetadataDirectory = wait(docLayer->rootDirectory->createOrOpen(
		    tr->tr, {StringRef(ns.first), StringRef(ns.second), StringRef(DocLayerConstants::METADATA)}));
		state Reference<UnboundCollectionContext> tcx =
		    Reference<UnboundCollectionContext>(new UnboundCollectionContext(tcollectionDirectory, tmetadataDirectory));

		tcx->bindCollectionContext(tr)->bumpMetadataVersion(); // We start at version 1.
		TraceEvent(SevInfo, "BumpMetadataVersion")
		    .detail("reason", "createCollection")
		    .detail("ns", fullCollNameToString(ns));

		return tcx;
	}
}

ACTOR static Future<Reference<UnboundCollectionContext>> assembleCollectionContext(Reference<DocTransaction> tr,
                                                                                   Namespace ns,
                                                                                   Reference<MetadataManager> self,
                                                                                   bool createCollectionIfAbsent) {
	if (self->metadataCache.size() > DocLayerConstants::METADATA_CACHE_SIZE)
		self->metadataCache.clear();

	auto match = self->metadataCache.find(ns);

	if (match == self->metadataCache.end()) {
		Reference<UnboundCollectionContext> cx =
		    wait(constructContext(ns, tr, self->docLayer, createCollectionIfAbsent));

		// Here and below don't pollute the cache if we just created the directory, since this transaction might
		// not commit.
		if (cx->isVersioned()) {
			TraceEvent(SevInfo, "MetadataCacheAdd")
			    .detail("ns", fullCollNameToString(ns))
			    .detail("version", cx->metadataVersion);
			auto insert_result = self->metadataCache.insert(std::make_pair(ns, cx));
			// Somebody else may have done the lookup and finished ahead of us. Either way, replace it with ours (can no
			// longer optimize this by only replacing if ours is newer, because the directory may have moved or
			// vanished.
			if (!insert_result.second) {
				insert_result.first->second = cx;
			}
		}
		return cx;
	} else {
		state Reference<UnboundCollectionContext> oldUnbound = (*match).second;
		state uint64_t oldVersion = oldUnbound->metadataVersion;

		// We would never cache a collection without valid version
		ASSERT(oldUnbound->isVersioned());

		uint64_t version = wait(getMetadataVersion(tr, oldUnbound->metadataDirectory));
		if (version != oldVersion) {
			Reference<UnboundCollectionContext> cx =
			    wait(constructContext(ns, tr, self->docLayer, createCollectionIfAbsent));
			if (cx->isVersioned()) {
				// Create the iterator again instead of making the previous value state, because the map could have
				// changed during the previous wait. Either way, replace it with ours (can no longer optimize this by
				// only replacing if ours is newer, because the directory may have moved or vanished.
				auto it = self->metadataCache.find(ns);
				if (it != self->metadataCache.end())
					it->second = cx;
				else
					self->metadataCache.insert(std::make_pair(ns, cx));

				TraceEvent(SevInfo, "MetadataCacheUpdate")
				    .detail("ns", fullCollNameToString(ns))
				    .detail("oldVersion", oldVersion)
				    .detail("newVersion", cx->metadataVersion);
			}
			return cx;
		} else {
			return oldUnbound;
		}
	}
}

Future<Reference<UnboundCollectionContext>> MetadataManager::getUnboundCollectionContext(
    Reference<DocTransaction> tr,
    Namespace const& ns,
    bool allowSystemNamespace,
    bool createCollectionIfAbsent) {
	if (!allowSystemNamespace && startsWith(ns.second.c_str(), "system."))
		throw write_system_namespace();
	return assembleCollectionContext(tr, ns, Reference<MetadataManager>::addRef(this), createCollectionIfAbsent);
}

Future<Reference<UnboundCollectionContext>> MetadataManager::refreshUnboundCollectionContext(
    Reference<UnboundCollectionContext> cx,
    Reference<DocTransaction> tr) {
	return assembleCollectionContext(tr, std::make_pair(cx->databaseName(), cx->collectionName()),
	                                 Reference<MetadataManager>::addRef(this), false);
}

ACTOR static Future<Void> buildIndex_impl(bson::BSONObj indexObj,
                                          Namespace ns,
                                          Standalone<StringRef> encodedIndexId,
                                          Reference<ExtConnection> ec,
                                          UID build_id) {
	state Reference<IndexInfo> info;
	try {
		state Reference<DocTransaction> tr = ec->getOperationTransaction();
		state Reference<UnboundCollectionContext> mcx = wait(ec->mm->getUnboundCollectionContext(tr, ns, false));
		info = MetadataManager::indexInfoFromObj(indexObj, mcx);
		info->status = IndexInfo::IndexStatus::BUILDING;
		info->buildId = build_id;
		mcx->addIndex(info);

		state Reference<Plan> buildingPlan = ec->wrapOperationPlan(
		    ref(new BuildIndexPlan(ref(new TableScanPlan(mcx)), info, ns.first, encodedIndexId, ec->mm)), false, mcx);
		wait(success(executeUntilCompletionTransactionally(buildingPlan, tr)));

		state Reference<Plan> finalizePlan = ec->isolatedWrapOperationPlan(
		    ref(new UpdateIndexStatusPlan(ns, encodedIndexId, ec->mm,
		                                  std::string(DocLayerConstants::INDEX_STATUS_READY), build_id)),
		    0, -1);
		wait(success(executeUntilCompletionTransactionally(finalizePlan, ec->getOperationTransaction())));

		return Void();
	} catch (Error& e) {
		TraceEvent(SevError, "indexRebuildFailed").error(e);
		state Error err = e;
		// try forever to set the index into an error status (unless somebody comes along before us and starts a
		// different build)
		loop {
			state bool okay;
			// Providing the build id here is sufficient to avoid clobbering a "ready" index as well, since
			// UpdateIndexStatusPlan, if it has that optional parameter, will return an error in the event that the
			// buildId field does not exist (as is the case for 'ready' indexes).
			state Reference<Plan> errorPlan = ec->isolatedWrapOperationPlan(
			    ref(new UpdateIndexStatusPlan(ns, encodedIndexId, ec->mm,
			                                  std::string(DocLayerConstants::INDEX_STATUS_ERROR), build_id)),
			    0, -1);
			try {
				wait(success(executeUntilCompletionTransactionally(errorPlan, ec->getOperationTransaction())));
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
