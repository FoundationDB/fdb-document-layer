/*
 * QLContext.h
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

#ifndef _QL_CONTEXT_H
#define _QL_CONTEXT_H
#pragma once

#include "Constants.h"
#include "Knobs.h"
#include "QLTypes.h"

#include "bindings/flow/DirectorySubspace.h"
#include "bindings/flow/fdb_flow.h"
#include "flow/flow.h"

using namespace FDB;

struct SnapshotLock {
	int use_count;
	AsyncVar<bool> in_use;

	SnapshotLock() : use_count(0), in_use(false) {}
	void use() {
		use_count++;
		in_use.set(true);
	}
	void unuse() {
		if (!--use_count)
			in_use.set(false);
	}
	bool is_in_use() const { return use_count > 0; }
	Future<Void> onUnused() {
		if (!use_count)
			return Void();
		else
			return in_use.onChange();
	}
};

struct DocumentDeferred : ReferenceCounted<DocumentDeferred> {
	SnapshotLock snapshotLock;
	Promise<Void> writes_finished;
	std::vector<Future<Void>> index_update_actors;
	std::set<struct ITDoc*> dirty;
	std::vector<std::function<void(Reference<struct DocTransaction>)>> deferred;

	Future<Void> commitChanges(Reference<DocTransaction> tr);
};

struct DocTransaction : ReferenceCounted<DocTransaction> {
	Reference<FDB::Transaction> tr;

	explicit DocTransaction(Reference<FDB::Transaction> tr) : tr(tr) {}

	static Reference<DocTransaction> create(Reference<FDB::Transaction> tr) {
		return Reference<DocTransaction>(new DocTransaction(tr));
	}

	Future<Void> commitChanges(std::string const& docPrefix);

	Future<Void> onError(Error const& e);

	// If you are about to call this function, think very carefully about why you are doing that. It is not safe to call
	// in general. Look at the comments in doNonIsolatedRW() for more on what it's for.
	void cancel_ongoing_index_reads();

	std::map<std::string, Reference<DocumentDeferred>> deferredDocuments;
};

template <class T>
struct GenFutureStream : public FutureStream<T> {
	GenFutureStream() = default;
	GenFutureStream(FutureStream<T> const& f) : FutureStream<T>(f) {}
	Future<Void> actor;
};

struct IReadContext {
	virtual Future<Optional<DataValue>> get(StringRef key) = 0;
	virtual GenFutureStream<Standalone<FDB::KeyValueRef>> getDescendants(
	    StringRef begin = LiteralStringRef("\x00"),
	    StringRef end = LiteralStringRef("\xff"),
	    Reference<FlowLock> flowControlLock = Reference<FlowLock>()) = 0;
	Reference<IReadContext> getSubContext(StringRef sub) { return Reference<IReadContext>(v_getSubContext(sub)); }
	virtual Future<DataValue> toDataValue();
	virtual std::string toDbgString() = 0;
	virtual void addref() = 0;
	virtual void delref() = 0;

protected:
	virtual IReadContext* v_getSubContext(StringRef sub) = 0;
};

struct IReadWriteContext : IReadContext {
	virtual void set(StringRef key, FDB::ValueRef value) = 0;
	virtual void clearDescendants() = 0;
	virtual void clearRoot() = 0;
	virtual void clear(StringRef key) = 0;
	virtual Future<Void> commitChanges() = 0;

	virtual Future<Standalone<StringRef>> getValueEncodedId();
	virtual Future<Standalone<StringRef>> getKeyEncodedId();

	Reference<IReadWriteContext> getSubContext(StringRef sub) {
		return Reference<IReadWriteContext>(v_getSubContext(sub));
	}

protected:
	virtual IReadWriteContext* v_getSubContext(StringRef sub) = 0;
};

struct NullContext : IReadContext, ReferenceCounted<NullContext>, FastAllocated<NullContext> {
	Future<Optional<DataValue>> get(StringRef key) override { return Optional<DataValue>(DataValue::nullValue()); }
	virtual GenFutureStream<Standalone<FDB::KeyValueRef>> getDescendants(
	    StringRef begin = LiteralStringRef("\x00"),
	    StringRef end = LiteralStringRef("\xff"),
	    Reference<FlowLock> flowControlLock = Reference<FlowLock>()) {
		PromiseStream<Standalone<FDB::KeyValueRef>> p;
		GenFutureStream<Standalone<FDB::KeyValueRef>> ret(p.getFuture());
		p.sendError(end_of_stream());
		return ret;
	}
	Reference<NullContext> getSubContext(StringRef sub) { return Reference<NullContext>(v_getSubContext(sub)); }
	Future<DataValue> toDataValue() override { return DataValue::nullValue(); }
	std::string toDbgString() override { return "NullContext"; }

	void addref() override { ReferenceCounted<NullContext>::addref(); }
	void delref() override { ReferenceCounted<NullContext>::delref(); }

protected:
	virtual NullContext* v_getSubContext(StringRef sub) { return new NullContext(); }
};

struct BsonContext : IReadWriteContext, ReferenceCounted<BsonContext>, FastAllocated<BsonContext> {
	BsonContext(bson::BSONObj const& obj, bool isArray) : obj(obj), isArray(isArray) {}

	Future<Optional<DataValue>> get(StringRef key) override {
		std::string k = keyPartToFieldName(key);
		if (!obj.hasField(k.c_str()))
			return Optional<DataValue>();
		return Optional<DataValue>(DataValue(obj.getField(k.c_str())));
	}
	GenFutureStream<Standalone<FDB::KeyValueRef>> getDescendants(
	    StringRef begin = LiteralStringRef("\x00"),
	    StringRef end = LiteralStringRef("\xff"),
	    Reference<FlowLock> flowControlLock = Reference<FlowLock>()) override {
		PromiseStream<Standalone<FDB::KeyValueRef>> out;
		bson_getDescendants(StringRef(), begin, end, out);
		out.sendError(end_of_stream());
		GenFutureStream<Standalone<FDB::KeyValueRef>> s = out.getFuture();
		s.actor = end_of_stream();
		return s;
	}
	Future<DataValue> toDataValue() override { return isArray ? DataValue(bson::BSONArray(obj)) : DataValue(obj); }
	std::string toDbgString() override {
		std::ostringstream strStream;
		strStream << "BsonContext: " << obj.toString(isArray, true);
		return strStream.str();
	}

	Reference<BsonContext> getSubContext(StringRef sub) { return Reference<BsonContext>(v_getSubContext(sub)); }

	void addref() override { ReferenceCounted<BsonContext>::addref(); }
	void delref() override { ReferenceCounted<BsonContext>::delref(); }

	void set(StringRef key, FDB::ValueRef value) override { throw internal_error(); }
	void clearDescendants() override { throw internal_error(); }
	void clearRoot() override { throw internal_error(); }
	void clear(StringRef key) override { throw internal_error(); }
	Future<Void> commitChanges() override { return Future<Void>(Void()); } // no-op

	Future<Standalone<StringRef>> getValueEncodedId() override;
	Future<Standalone<StringRef>> getKeyEncodedId() override;

protected:
	bson::BSONObj obj;
	bool isArray;

	std::string fieldNameToKeyPart(StringRef fn) {
		if (isArray || std::all_of(fn.begin(), fn.end(), ::isdigit)) {
			return DataValue(atoi(fn.toString().c_str())).encode_key_part();
		} else
			return DataValue(fn, DVTypeCode::STRING).encode_key_part();
	}

	std::string keyPartToFieldName(StringRef kp) {
		DataValue dv = DataValue::decode_key_part(kp);
		if (dv.getSortType() == DVTypeCode::NUMBER)
			return std::to_string(dv.getInt());
		else
			return dv.getString();
	}

	void bson_getDescendants(StringRef prefix,
	                         StringRef begin,
	                         StringRef end,
	                         PromiseStream<Standalone<FDB::KeyValueRef>> out) {
		for (auto i = obj.begin(); i.more();) {
			bson::BSONElement el = i.next();
			StringRef name((uint8_t*)el.fieldName(), el.fieldNameSize());

			auto key = prefix.toString() + fieldNameToKeyPart(name);
			if (key >= begin && key < end) {
				out.send(FDB::KeyValueRef(key, DataValue(el).encode_value()));
			}

			if (el.isABSONObj())
				BsonContext(el.Obj(), el.type() == bson::BSONType::Array).bson_getDescendants(key, begin, end, out);
		}
	}

	BsonContext* v_getSubContext(StringRef sub) override {
		bson::BSONElement el = obj.getField(keyPartToFieldName(sub));
		return new BsonContext(el.Obj(), el.type() == bson::BSONType::Array);
	}
};

struct QueryContext : IReadWriteContext, ReferenceCounted<QueryContext>, FastAllocated<QueryContext> {
	explicit QueryContext(Reference<DocTransaction> tr);
	QueryContext(Reference<struct ITDoc>, Reference<DocTransaction> tr, DataKey path);
	~QueryContext();

	Reference<QueryContext> getSubContext(StringRef sub) { return Reference<QueryContext>(v_getSubContext(sub)); }
	Future<Optional<DataValue>> get(StringRef key) override;
	GenFutureStream<Standalone<FDB::KeyValueRef>> getDescendants(
	    StringRef begin = LiteralStringRef("\x00"),
	    StringRef end = LiteralStringRef("\xff"),
	    Reference<FlowLock> flowControlLock = Reference<FlowLock>()) override;
	void set(StringRef key, FDB::ValueRef value) override;
	void clearDescendants() override;
	void clearRoot() override;
	void clear(StringRef key) override;
	void addIndex(struct IndexInfo index);
	const DataKey getPrefix();

	Future<Void> commitChanges() override;

	Reference<DocTransaction> getTransaction();

	void printPlugins();

	std::string toDbgString() override {
		std::ostringstream strStream;
		strStream << "QueryContext: " << getPrefix().bytes().printable();
		return strStream.str();
	}
	void addref() override { ReferenceCounted<QueryContext>::addref(); }
	void delref() override { ReferenceCounted<QueryContext>::delref(); }

protected:
	virtual QueryContext* v_getSubContext(StringRef sub);

private:
	struct QueryContextData* self;
	// QueryContext( Reference<FDB::Transaction> tr, const std::string& prefix, const std::string& sub );
	QueryContext(QueryContext const& other, StringRef sub);
};

struct ScanReturnedContext : IReadWriteContext,
                             ReferenceCounted<ScanReturnedContext>,
                             FastAllocated<ScanReturnedContext> {
	ScanReturnedContext(Reference<IReadWriteContext> cx, int scanId, FDB::Key scanKey)
	    : internal_context(cx), m_scanId(scanId), m_scanKey(scanKey) {}
	Reference<ScanReturnedContext> getSubContext(StringRef sub) {
		return Reference<ScanReturnedContext>(v_getSubContext(sub));
	}

	Future<Optional<DataValue>> get(StringRef key) override { return internal_context->get(key); }
	GenFutureStream<Standalone<FDB::KeyValueRef>> getDescendants(
	    StringRef begin = LiteralStringRef("\x00"),
	    StringRef end = LiteralStringRef("\xff"),
	    Reference<FlowLock> flowControlLock = Reference<FlowLock>()) override {
		return internal_context->getDescendants(begin, end, flowControlLock);
	}
	Future<DataValue> toDataValue() override { return internal_context->toDataValue(); }
	std::string toDbgString() override {
		std::ostringstream strStream;
		strStream << "ScanReturnedContext: (scanId: " << scanId() << ", scanKey: " << scanKey().printable()
		          << ", internalCtx: " << internal_context->toDbgString() << "(";
		return strStream.str();
	}

	void set(StringRef key, FDB::ValueRef value) override { internal_context->set(key, value); }
	void clearDescendants() override { internal_context->clearDescendants(); }
	void clearRoot() override { internal_context->clearRoot(); }
	void clear(StringRef key) override { internal_context->clear(key); }
	Future<Void> commitChanges() override { return internal_context->commitChanges(); }

	Future<Standalone<StringRef>> getValueEncodedId() override { return internal_context->getValueEncodedId(); }
	Future<Standalone<StringRef>> getKeyEncodedId() override { return internal_context->getKeyEncodedId(); }

	void addref() override { ReferenceCounted<ScanReturnedContext>::addref(); }
	void delref() override { ReferenceCounted<ScanReturnedContext>::delref(); }

	int scanId() const { return m_scanId; }
	FDB::Key scanKey() const { return m_scanKey; }

	int compare(const Reference<ScanReturnedContext>& other) const { return m_scanKey.compare(other->scanKey()); }
	bool operator<(const Reference<ScanReturnedContext>& other) const { return m_scanKey < other->scanKey(); }

protected:
	ScanReturnedContext* v_getSubContext(StringRef sub) override {
		return new ScanReturnedContext(internal_context->getSubContext(sub), m_scanId, m_scanKey);
	}

private:
	Reference<IReadWriteContext> internal_context;
	int m_scanId;
	FDB::Key m_scanKey;
};

struct UnboundQueryContext : ReferenceCounted<UnboundQueryContext>, FastAllocated<UnboundQueryContext> {
	UnboundQueryContext() = default;
	explicit UnboundQueryContext(DataKey prefix) : prefix(prefix) {}
	UnboundQueryContext(UnboundQueryContext const& other, StringRef sub) : prefix(other.prefix) { prefix.append(sub); }

	Reference<UnboundQueryContext> getSubContext(StringRef sub);
	Reference<QueryContext> bindQueryContext(Reference<DocTransaction> tr);
	const DataKey getPrefix();

private:
	DataKey prefix;
};

struct IndexInfo {
	enum IndexStatus { READY = 0, BUILDING = 1000, INVALID = 9999 };

	std::string indexName;
	std::string encodedIndexName;
	Reference<UnboundQueryContext> indexCx;
	std::vector<std::pair<std::string, int>> indexKeys;
	IndexStatus status;
	Optional<UID> buildId;
	bool multikey;
	bool isUniqueIndex;

	IndexInfo(std::string indexName,
	          std::vector<std::pair<std::string, int>> indexKeys,
	          Reference<struct UnboundCollectionContext> collectionCx,
	          IndexStatus status,
	          Optional<UID> buildId = Optional<UID>(),
	          bool isUniqueIndex = false);
	IndexInfo() : status(IndexStatus::INVALID) {}
	bool hasPrefix(std::vector<std::string> const& prefix);
	int size() const { return static_cast<int>(indexKeys.size()); }
};

struct IndexComparator {
	bool operator()(const IndexInfo& lhs, const IndexInfo& rhs) {
		return lhs.indexKeys.size() == rhs.indexKeys.size() ? lhs.indexName < rhs.indexName
		                                                    : lhs.indexKeys.size() < rhs.indexKeys.size();
	}
};

struct UnboundCollectionContext : ReferenceCounted<UnboundCollectionContext>, FastAllocated<UnboundCollectionContext> {
	UnboundCollectionContext(uint64_t metadataVersion,
	                         Reference<DirectorySubspace> collectionDirectory,
	                         Reference<DirectorySubspace> metadataDirectory)
	    : metadataVersion(metadataVersion),
	      collectionDirectory(collectionDirectory),
	      metadataDirectory(metadataDirectory) {
		cx = Reference<UnboundQueryContext>(new UnboundQueryContext())->getSubContext(collectionDirectory->key());
	}

	UnboundCollectionContext(Reference<DirectorySubspace> collectionDirectory,
	                         Reference<DirectorySubspace> metadataDirectory)
	    : UnboundCollectionContext(DocLayerConstants::METADATA_INVALID_VERSION,
	                               collectionDirectory,
	                               metadataDirectory) {}

	UnboundCollectionContext(const UnboundCollectionContext& other)
	    : metadataVersion(other.metadataVersion),
	      collectionDirectory(other.collectionDirectory),
	      metadataDirectory(other.metadataDirectory),
	      simpleIndexMap(other.simpleIndexMap),
	      knownIndexes(other.knownIndexes),
	      cx(Reference<UnboundQueryContext>::addRef(other.cx.getPtr())),
	      bannedFieldNames(other.bannedFieldNames) {}

	Optional<IndexInfo> getSimpleIndex(std::string simple_index_map_key);
	Optional<IndexInfo> getCompoundIndex(std::vector<std::string> const& prefix, std::string nextIndexKey);
	void setBannedFieldNames(std::vector<std::string> bannedFns) {
		bannedFieldNames = std::set<std::string>(bannedFns.begin(), bannedFns.end());
	}
	FDB::Key getVersionKey();
	Reference<struct CollectionContext> bindCollectionContext(Reference<DocTransaction> tr);
	void addIndex(IndexInfo index);
	Key getIndexesSubspace();
	Reference<UnboundQueryContext> getIndexesContext(); // FIXME: Remove this method
	std::string databaseName();
	std::string collectionName();

	bool isVersioned() { return metadataVersion != DocLayerConstants::METADATA_INVALID_VERSION; }

	Reference<DirectorySubspace> collectionDirectory;
	Reference<DirectorySubspace> metadataDirectory;

	Reference<UnboundQueryContext> cx;

	// This is used by the planner, and should only hold indexes that are ready to speed up queries
	std::map<std::string, std::set<IndexInfo, IndexComparator>> simpleIndexMap;

	// This holds all indexes that will be loaded as plugins (i.e. for sets and clears), and should
	// include indexes that are still building
	std::vector<IndexInfo> knownIndexes;

	const uint64_t metadataVersion;

private:
	std::set<std::string> bannedFieldNames;
};

struct CollectionContext : ReferenceCounted<CollectionContext>, FastAllocated<CollectionContext> {
	Reference<QueryContext> cx;

	CollectionContext(Reference<DocTransaction> tr, Reference<UnboundCollectionContext> unbound) : unbound(unbound) {
		cx = unbound->cx->bindQueryContext(tr);
		for (const auto& entry : unbound->knownIndexes) {
			cx->addIndex(entry);
		}
	}

	void bumpMetadataVersion();
	Future<uint64_t> getMetadataVersion();

private:
	Reference<UnboundCollectionContext> unbound;
};

template <class T, class Iterator = T*>
struct cartesian_product_iterator {
	struct Item {
		Iterator begin, end, current;
		Item(Iterator begin, Iterator end) : begin(begin), end(end), current(begin) {}
	};
	std::vector<Item> items;

	template <class Collection>
	explicit cartesian_product_iterator(Collection& c) {
		items.reserve(c.size());
		for (auto& seq : c)
			items.push_back(Item(seq.begin(), seq.end()));
	}
	cartesian_product_iterator() = default;

	explicit operator bool() { // safe_bool?
		return items.back().current != items.back().end;
	}

	T const& operator[](int d) { return *items[d].current; }

	int size() const { return static_cast<int>(items.size()); }

	void operator++() {
		for (int dim = 0; dim < items.size(); dim++) {
			++items[dim].current;
			if (items[dim].current != items[dim].end)
				break;
			if (dim != items.size() - 1)
				items[dim].current = items[dim].begin;
		}
	}

	void reset() {
		for (auto& item : items) {
			item.current = item.begin;
		}
	}
};

#endif /* _QL_CONTEXT_H_ */
