/*
 * QLPlan.h
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

#if defined(NO_INTELLISENSE) && !defined(_QL_PLAN_ACTOR_G_H_)
#define _QL_PLAN_ACTOR_G_H_
#include "QLPlan.actor.g.h"
#elif !defined(_QL_PLAN_ACTOR_H_)
#define _QL_PLAN_ACTOR_H_

#pragma once

#include "MetadataManager.h"
#include "QLContext.h"
#include "QLOperations.h"
#include "QLPredicate.h"
#include "QLProjection.actor.h"
#include "Oplogger.h"

/**
 * Represents a plan running in a particular transaction(al try) and its bounds in the keyspace
 * of each scan.
 */
struct PlanCheckpoint : ReferenceCounted<PlanCheckpoint>, FastAllocated<PlanCheckpoint> {

	/**
	 * Interface for retry loop
	 */
	PlanCheckpoint(); // Unbounded

	/**
	 * Cancels all outstanding operations, and returns a new PlanCheckpoint bounded to the remainder of
	 * this plan's original bounds.  Must not be called with any actor in the plan on the C++ call stack
	 * (see implementation comment for details).
	 */
	Reference<PlanCheckpoint> stopAndCheckpoint();

	/**
	 * Cancels any outstanding operations but doesn't change the bounds of this.
	 */
	void stop();

	/**
	 * Interface for operations.
	 * addOperation and addScan must be called in a topological sort order (inputs before outputs).
	 */
	void addOperation(Future<Void> actors, PromiseStream<Reference<ScanReturnedContext>> output);
	int addScan();
	FDB::KeyRange getBounds(int whichScan);
	bool splitBoundWanted();

	/**
	 * Modified by operations at cancellation time if splitBooundWanted().
	 */
	FDB::Key& splitBound(int whichScan);

	/**
	 * Returns a state variable which will be preserved at the beginning of the bounds of each
	 * checkpoint.
	 */
	int64_t& getIntState(int64_t defaultValue);

	FlowLock* getDocumentFinishedLock() {
		return &flowControlLock;
	} // To be called in topsort order along with addOperation(), addScan(), etc.
	// void newDocumentFinishedLock();

	std::string toString();

private:
	struct OpInfo {
		Future<Void> actors;
		PromiseStream<Reference<ScanReturnedContext>> output;
		OpInfo(Future<Void> const& actors, PromiseStream<Reference<ScanReturnedContext>> const& output)
		    : actors(actors), output(output) {}
	};
	struct ScanInfo {
		FDB::KeyRange bounds;
		FDB::Key split;
		ScanInfo() : bounds(FDB::KeyRangeRef(StringRef(), LiteralStringRef("\xff"))), split(LiteralStringRef("\xff")) {}
	};
	struct StateInfo {
		int64_t begin;
		int64_t split;
		explicit StateInfo(int64_t x) : begin(x) {}
	};
	FlowLock flowControlLock;
	std::vector<OpInfo> ops;

	// Scans either added to this via addScan() or bounds inherited from previous stopAndCheckpoint().
	std::vector<ScanInfo> scans;
	std::vector<StateInfo> states;

	// Number of scans added to this via addScan().
	int scansAdded;
	bool boundsWanted;
	int stateAdded;
};

enum class PlanType : uint8_t {
	TableScan,
	Filter,
	IndexScan,
	PrimaryKeyLookup,
	Union,
	Empty,
	NonIsolated,
	Projection,
	Skip,
	ProjectAndUpdate,
	Update,
	Insert,
	IndexInsert,
	Sort,
	Retry,
	BuildIndex,
	UpdateIndexStatus,
	FlushChanges,
	FindAndModify
};

/**
 * Plan represents a (sub)plan which outputs a stream of documents.
 *
 * Plan::execute() generates actual actors to evaluate the plan, while other Plan methods like Plan::push_down()
 * participate in planning and optimization.  So this interface and its implementations straddle the planner and
 * execution engine.
 */
struct Plan {
	virtual void addref() = 0;
	virtual void delref() = 0;

	virtual ~Plan() = default;
	virtual bson::BSONObj describe() = 0;
	virtual PlanType getType() = 0;
	virtual bool hasScanOfType(PlanType type) { return getType() == type; }

	/**
	 * Executes the plan within the given checkpoint's bounds and with the given transaction, and return a stream of
	 * resulting documents. The caller must call checkpoint->getDocumentFinishedLock() before calling execute()
	 * and must release the lock for each document which it receives to allow the plan to continue executing.
	 *
	 * Implementations of execute() have a complex contract with respect to their interaction with the checkpoint to
	 * implement flow control and non-isolated scans. See the comment at the top of QLPlan.cpp for a detailed contract
	 * and the implementation comment of PlanCheckpoint::stopAndCheckpoint for an understanding of the checkpoint
	 * bounds mechanism.
	 */
	virtual FutureStream<Reference<ScanReturnedContext>> execute(PlanCheckpoint* checkpoint,
	                                                             Reference<DocTransaction> tr) = 0;

	/**
	 * Return a plan equivalent to, but more efficient than, FilterPlan(this, query), or Optional() if no such plan can
	 * be found.
	 */
	virtual Optional<Reference<Plan>> push_down(Reference<UnboundCollectionContext> cx, Reference<IPredicate> query) {
		return Optional<Reference<Plan>>();
	}
	virtual bool empty() { return false; }

	virtual bool wasMetadataChangeOkay(Reference<UnboundCollectionContext> cx) { return false; }
};

template <class PlanType>
struct ConcretePlan : Plan, ReferenceCounted<PlanType>, FastAllocated<PlanType> {
	void addref() override { ReferenceCounted<PlanType>::addref(); }
	void delref() override { ReferenceCounted<PlanType>::delref(); }
};

struct TableScanPlan : ConcretePlan<TableScanPlan> {
	explicit TableScanPlan(Reference<UnboundCollectionContext> cx) : cx(cx) {}
	bson::BSONObj describe() override {
		return BSON("type"
		            << "table scan");
	}
	FutureStream<Reference<ScanReturnedContext>> execute(PlanCheckpoint* checkpoint,
	                                                     Reference<DocTransaction> tr) override;
	PlanType getType() override { return PlanType::TableScan; }

	Optional<Reference<Plan>> push_down(Reference<UnboundCollectionContext> cx, Reference<IPredicate> query) override;
	bool wasMetadataChangeOkay(Reference<UnboundCollectionContext> cx) override { return true; }

private:
	Reference<UnboundCollectionContext> cx;
};

struct FilterPlan : ConcretePlan<FilterPlan> {
	bson::BSONObj describe() override {
		return BSON(
		    // clang-format off
			"type" << "filter" <<
			"source_plan" << source->describe() <<
			"filter" << filter->toString()
		    // clang-format on
		);
	}
	FutureStream<Reference<ScanReturnedContext>> execute(PlanCheckpoint* checkpoint,
	                                                     Reference<DocTransaction> tr) override;
	PlanType getType() override { return PlanType::Filter; }
	bool hasScanOfType(PlanType type) override { return source->hasScanOfType(type); }
	static Reference<Plan> construct_filter_plan(Reference<UnboundCollectionContext> cx,
	                                             Reference<Plan> source,
	                                             Reference<IPredicate> filter);
	static Reference<Plan> construct_filter_plan_no_pushdown(Reference<UnboundCollectionContext> cx,
	                                                         Reference<Plan> source,
	                                                         Reference<IPredicate> filter) {
		return Reference<Plan>(new FilterPlan(cx, source, filter));
	}
	bool wasMetadataChangeOkay(Reference<UnboundCollectionContext> cx) override {
		return source->wasMetadataChangeOkay(cx);
	}
	Optional<Reference<Plan>> push_down(Reference<UnboundCollectionContext> cx, Reference<IPredicate> query) override;

private:
	Reference<Plan> source;
	Reference<IPredicate> filter;
	Reference<UnboundCollectionContext> cx;
	FilterPlan(Reference<UnboundCollectionContext> cx, Reference<Plan> source, Reference<IPredicate> filter)
	    : cx(cx), source(source), filter(filter) {}
};

struct IndexScanPlan : ConcretePlan<IndexScanPlan> {
	IndexScanPlan(Reference<UnboundCollectionContext> cx,
	              Reference<IndexInfo> index,
	              Optional<Key> begin,
	              Optional<Key> end,
	              std::vector<std::string> matchedPrefix)
	    : cx(cx), index(index), begin(begin), end(end), matchedPrefix(matchedPrefix) {}
	bson::BSONObj describe() override {
		std::string bound_begin = begin.present() ? FDB::printable(begin.get()) : "-inf";
		std::string bound_end = end.present() ? FDB::printable(end.get()) : "+inf";
		return BSON(
		    // clang-format off
			"type" << "index scan" <<
			"index name" << index->indexName <<
			"bounds" << BSON(
				"begin" << bound_begin <<
				"end" << bound_end)
		    // clang-format on
		);
	}
	FutureStream<Reference<ScanReturnedContext>> execute(PlanCheckpoint* checkpoint,
	                                                     Reference<DocTransaction> tr) override;
	PlanType getType() override { return PlanType::IndexScan; }
	Optional<Reference<Plan>> push_down(Reference<UnboundCollectionContext> cx, Reference<IPredicate> query) override;

	bool single_key() { return begin.present() && end.present() && begin.get() == end.get(); }

private:
	Reference<UnboundCollectionContext> cx;
	Reference<IndexInfo> index;
	Optional<Key> begin;
	Optional<Key> end;

	// Matched index not necessarily the exact match. This plan could simply use prefix of index.
	// For now, index direction is not honored by Doc Layer, probably SOMEDAY
	std::vector<std::string> matchedPrefix;
};

struct PrimaryKeyLookupPlan : ConcretePlan<PrimaryKeyLookupPlan> {
	PrimaryKeyLookupPlan(Reference<UnboundCollectionContext> cx, Optional<DataValue> begin, Optional<DataValue> end)
	    : cx(cx), begin(begin), end(end) {}

	bson::BSONObj describe() override {
		std::string bound_begin = begin.present() ? begin.get().toString() : "-inf";
		std::string bound_end = end.present() ? end.get().toString() : "+inf";
		return BSON(
		    // clang-format off
			"type" << "PK lookup" <<
			"bounds" << BSON("begin" << bound_begin << "end" << bound_end)
		    // clang-format on
		);
	}
	FutureStream<Reference<ScanReturnedContext>> execute(PlanCheckpoint* checkpoint,
	                                                     Reference<DocTransaction> tr) override;
	PlanType getType() override { return PlanType::PrimaryKeyLookup; }
	bool wasMetadataChangeOkay(Reference<UnboundCollectionContext> cx) override { return true; }

private:
	Reference<UnboundCollectionContext> cx;
	Optional<DataValue> begin;
	Optional<DataValue> end;
};

struct UnionPlan : ConcretePlan<UnionPlan> {
	UnionPlan(Reference<Plan> plan1, Reference<Plan> plan2) : plan1(plan1), plan2(plan2) {}
	bson::BSONObj describe() override {
		return BSON(
		    // clang-format off
			"type" << "union" <<
			"plans" << BSON_ARRAY(plan1->describe() << plan2->describe())
		    // clang-format on
		);
	}
	FutureStream<Reference<ScanReturnedContext>> execute(PlanCheckpoint* checkpoint,
	                                                     Reference<DocTransaction> tr) override;
	PlanType getType() override { return PlanType::Union; }
	bool hasScanOfType(PlanType type) override { return plan1->hasScanOfType(type) || plan2->hasScanOfType(type); }

private:
	Reference<Plan> plan1, plan2;
};

struct EmptyPlan : ConcretePlan<EmptyPlan> {
	bson::BSONObj describe() override {
		return BSON("type"
		            << "EMPTY");
	}
	FutureStream<Reference<ScanReturnedContext>> execute(PlanCheckpoint* checkpoint,
	                                                     Reference<DocTransaction> tr) override {
		PromiseStream<Reference<ScanReturnedContext>> p;
		p.sendError(end_of_stream());
		checkpoint->addOperation(Void(), p);
		return p.getFuture();
	}
	PlanType getType() override { return PlanType::Empty; }
};

struct NonIsolatedPlan : ConcretePlan<NonIsolatedPlan> {
	Reference<Plan> subPlan;
	bool isReadOnly;
	Reference<MetadataManager> mm;
	Reference<IOplogInserter> oplogInserter;

	NonIsolatedPlan(Reference<Plan> subPlan,
	                bool isReadOnly,
	                Reference<UnboundCollectionContext> cx,
	                Reference<FDB::Database> database,
					Reference<IOplogInserter> oplogInserter,
	                Reference<MetadataManager> mm)
	    : subPlan(subPlan), isReadOnly(isReadOnly), cx(cx), database(database), oplogInserter(oplogInserter), mm(mm) {}

	NonIsolatedPlan(Reference<Plan> subPlan,
	                bool isReadOnly,
	                Reference<UnboundCollectionContext> cx,
	                Reference<FDB::Database> database,
	                Reference<MetadataManager> mm)
	    : subPlan(subPlan), isReadOnly(isReadOnly), cx(cx), database(database), mm(mm) {}

	bson::BSONObj describe() override {
		return BSON(
		    // clang-format off
			"type" << "non-isolated" <<
			"source_plan" << subPlan->describe()
		    // clang-format on
		);
	}
	PlanType getType() override { return PlanType::NonIsolated; }
	FutureStream<Reference<ScanReturnedContext>> execute(PlanCheckpoint* checkpoint,
	                                                     Reference<DocTransaction> tr) override;
	bool hasScanOfType(PlanType type) override { return subPlan->hasScanOfType(type); }

	Reference<DocTransaction> newTransaction();
	static Reference<DocTransaction> newTransaction(Reference<FDB::Database> database);

private:
	Reference<UnboundCollectionContext> cx;
	Reference<FDB::Database> database;
};

/**
 * Since a RetryPlan may reset the transaction it uses an arbitrary number of times, responsibility
 * for creating/opening collection directories must belong to the subPlan nested within.
 */
struct RetryPlan : ConcretePlan<RetryPlan> {
	Reference<Plan> subPlan;
	int64_t timeout;
	int64_t retryLimit;

	RetryPlan(Reference<Plan> subPlan, int64_t timeout, int64_t retryLimit, Reference<FDB::Database> database)
	    : subPlan(subPlan), timeout(timeout), retryLimit(retryLimit), database(database) {}
	bson::BSONObj describe() override {
		return BSON(
		    // clang-format off
			"type" << "retry" <<
			"source_plan" << subPlan->describe()
		    // clang-format on
		);
	}
	PlanType getType() override { return PlanType::Retry; }
	FutureStream<Reference<ScanReturnedContext>> execute(PlanCheckpoint* checkpoint,
	                                                     Reference<DocTransaction> tr) override;
	bool hasScanOfType(PlanType type) override { return subPlan->hasScanOfType(type); }

	Reference<DocTransaction> newTransaction();

private:
	Reference<FDB::Database> database;
};

struct ProjectionPlan : ConcretePlan<ProjectionPlan> {
	Reference<Projection> projection;
	Reference<Plan> subPlan;
	Optional<bson::BSONObj> ordering;

	ProjectionPlan(Reference<Projection> projection,
	               Reference<Plan> subPlan,
	               Optional<bson::BSONObj> ordering = Optional<bson::BSONObj>())
	    : projection(projection), subPlan(subPlan), ordering(ordering) {}

	bson::BSONObj describe() override {
		return BSON(
		    // clang-format off
			"type" << "projection" <<
			"projection" << projection->debugString() <<
			"source_plan" << subPlan->describe()
		    // clang-format on
		);
	}
	PlanType getType() override { return PlanType::Projection; }
	FutureStream<Reference<ScanReturnedContext>> execute(PlanCheckpoint* checkpoint,
	                                                     Reference<DocTransaction> tr) override;
	bool hasScanOfType(PlanType type) override { return subPlan->hasScanOfType(type); }
	bool wasMetadataChangeOkay(Reference<UnboundCollectionContext> cx) override {
		return subPlan->wasMetadataChangeOkay(cx);
	}
};

struct UpdatePlan : ConcretePlan<UpdatePlan> {
	Reference<Plan> subPlan;
	Reference<IUpdateOp> updateOp;
	Reference<IInsertOp> upsertOp;
	int64_t limit;
	Reference<UnboundCollectionContext> cx;

	UpdatePlan(Reference<Plan> subPlan,
	           Reference<IUpdateOp> updateOp,
	           Reference<IInsertOp> upsertOp,
	           int64_t limit,
	           Reference<UnboundCollectionContext> cx)
	    : subPlan(subPlan), updateOp(updateOp), upsertOp(upsertOp), limit(limit), cx(cx) {}

	bson::BSONObj describe() override {
		return BSON(
		    // clang-format off
			"type" << "update" <<
			"updateOp" << updateOp->describe() <<
			"upsertOp" << (upsertOp ? upsertOp->describe() : "none") <<
			"source_plan" << subPlan->describe()
		    // clang-format on
		);
	}
	PlanType getType() override { return PlanType::Update; }
	FutureStream<Reference<ScanReturnedContext>> execute(PlanCheckpoint* checkpoint,
	                                                     Reference<DocTransaction> tr) override;
	bool hasScanOfType(PlanType type) override { return subPlan->hasScanOfType(type); }

	// RW plan needs to worry that directory might have changed
	bool wasMetadataChangeOkay(Reference<UnboundCollectionContext> cx) override { return false; }
};

struct ProjectAndUpdatePlan : ConcretePlan<ProjectAndUpdatePlan> {
	Reference<Plan> subPlan;
	Reference<IUpdateOp> updateOp;
	Reference<IInsertOp> upsertOp;
	Reference<Projection> projection;
	Optional<bson::BSONObj> ordering;
	bool projectNew;
	Reference<UnboundCollectionContext> cx;

	ProjectAndUpdatePlan(Reference<Plan> subPlan,
	                     Reference<IUpdateOp> updateOp,
	                     Reference<IInsertOp> upsertOp,
	                     Reference<Projection> projection,
	                     Optional<bson::BSONObj> ordering,
	                     bool projectNew,
	                     Reference<UnboundCollectionContext> cx)
	    : subPlan(subPlan),
	      updateOp(updateOp),
	      upsertOp(upsertOp),
	      projection(projection),
	      projectNew(projectNew),
	      ordering(ordering),
	      cx(cx) {}

	bson::BSONObj describe() override {
		return BSON(
		    // clang-format off
			"type" << "projectAndUpdate" <<
			"projection" << projection->debugString() <<
			"updateOp" << updateOp->describe() <<
			"upsertOp" << (upsertOp ? upsertOp->describe() : "none") <<
			"source_plan" << subPlan->describe()
		    // clang-format on
		);
	}
	PlanType getType() override { return PlanType::ProjectAndUpdate; }
	FutureStream<Reference<ScanReturnedContext>> execute(PlanCheckpoint* checkpoint,
	                                                     Reference<DocTransaction> tr) override;
	bool hasScanOfType(PlanType type) override { return subPlan->hasScanOfType(type); }

	// RW plan needs to worry that directory might have changed
	bool wasMetadataChangeOkay(Reference<UnboundCollectionContext> cx) override { return false; }
};

struct FindAndModifyPlan : ConcretePlan<FindAndModifyPlan> {
	Reference<Plan> subPlan;
	Reference<IUpdateOp> updateOp;
	Reference<IInsertOp> upsertOp;
	Reference<Projection> projection;
	Optional<bson::BSONObj> ordering;
	bool projectNew;
	Reference<UnboundCollectionContext> cx;
	Reference<FDB::Database> database;
	Reference<MetadataManager> mm;
	Reference<IOplogInserter> oplogInserter;

	FindAndModifyPlan(Reference<Plan> subPlan,
	                  Reference<IUpdateOp> updateOp,
	                  Reference<IInsertOp> upsertOp,
	                  Reference<Projection> projection,
	                  Optional<bson::BSONObj> ordering,
	                  bool projectNew,
	                  Reference<UnboundCollectionContext> cx,
					  Reference<IOplogInserter> oplogInserter,
	                  Reference<FDB::Database> database,
	                  Reference<MetadataManager> mm)
	    : subPlan(subPlan),
	      updateOp(updateOp),
	      upsertOp(upsertOp),
	      projection(projection),
	      projectNew(projectNew),
	      ordering(ordering),
	      cx(cx),
		  oplogInserter(oplogInserter),
	      database(database),
	      mm(mm) {}

	bson::BSONObj describe() override {
		return BSON(
		    // clang-format off
			"type" << "findAndModify" <<
			"projection" << projection->debugString() <<
			"updateOp" << updateOp->describe() <<
			"upsertOp" << (upsertOp ? upsertOp->describe() : "none") <<
			"source_plan" << subPlan->describe()
		    // clang-format on
		);
	}
	PlanType getType() override { return PlanType::FindAndModify; }
	FutureStream<Reference<ScanReturnedContext>> execute(PlanCheckpoint* checkpoint,
	                                                     Reference<DocTransaction> tr) override;
	bool hasScanOfType(PlanType type) override { return subPlan->hasScanOfType(type); }
	bool wasMetadataChangeOkay(Reference<UnboundCollectionContext> cx) override { return false; }
};

struct SkipPlan : ConcretePlan<SkipPlan> {
	int64_t skip;
	Reference<Plan> subPlan;

	SkipPlan(int64_t skip, Reference<Plan> subPlan) : skip(skip), subPlan(subPlan) {}

	bson::BSONObj describe() override {
		return BSON(
		    // clang-format off
			"type" << "skip" <<
			"number" << (long long)skip <<
			"source_plan" << subPlan->describe()
		    // clang-format on
		);
	}
	PlanType getType() override { return PlanType::Skip; }
	FutureStream<Reference<ScanReturnedContext>> execute(PlanCheckpoint* checkpoint,
	                                                     Reference<DocTransaction> tr) override;
	bool hasScanOfType(PlanType type) override { return subPlan->hasScanOfType(type); }
	bool wasMetadataChangeOkay(Reference<UnboundCollectionContext> cx) override {
		return subPlan->wasMetadataChangeOkay(cx);
	}
};

struct InsertPlan : ConcretePlan<InsertPlan> {
	InsertPlan(std::vector<Reference<IInsertOp>> docs, Reference<MetadataManager> mm, Namespace ns)
	    : docs(docs), mm(mm), ns(ns) {}

	bson::BSONObj describe() override {
		return BSON(
		    // clang-format off
			"type" << "insert" <<
			"numDocs" << (int)docs.size()
		    // clang-format on
		);
	}
	PlanType getType() override { return PlanType::Insert; }
	FutureStream<Reference<ScanReturnedContext>> execute(PlanCheckpoint* checkpoint,
	                                                     Reference<DocTransaction> tr) override;

private:
	std::vector<Reference<IInsertOp>> docs;
	Reference<MetadataManager> mm;
	Namespace ns;
};

struct OplogInsertPlan : ConcretePlan<OplogInsertPlan> {
	OplogInsertPlan(Reference<Plan> subPlan,
					std::list<bson::BSONObj>* docs,
					Reference<IOplogInserter> oplogInserter,
					Reference<MetadataManager> mm,
					Namespace ns)
	    : docs(docs), subPlan(subPlan), oplogInserter(oplogInserter), mm(mm), ns(ns) {}

	bson::BSONObj describe() override {
		return subPlan->describe();
	}

	PlanType getType() override { return PlanType::Insert; }
	FutureStream<Reference<ScanReturnedContext>> execute(PlanCheckpoint* checkpoint,
	                                                     Reference<DocTransaction> tr) override;

	private:
		Reference<Plan> subPlan;
		Reference<IOplogInserter> oplogInserter;
		std::list<bson::BSONObj>* docs;
		Reference<MetadataManager> mm;
		Namespace ns;
};

struct SortPlan : ConcretePlan<SortPlan> {
	Reference<Plan> subPlan;
	bson::BSONObj orderObj; // FIXME: Belongs in sort key
	SortPlan(Reference<Plan> subPlan, bson::BSONObj orderObj) : subPlan(subPlan), orderObj(orderObj) {}

	bson::BSONObj describe() override {
		return BSON(
		    // clang-format off
			"type" << "sort" <<
			"source_plan" << subPlan->describe()
		    // clang-format on
		);
	}
	PlanType getType() override { return PlanType::Sort; }
	bool hasScanOfType(PlanType type) override { return subPlan->hasScanOfType(type); }
	FutureStream<Reference<ScanReturnedContext>> execute(PlanCheckpoint* checkpoint,
	                                                     Reference<DocTransaction> tr) override;
};

struct IndexInsertPlan : ConcretePlan<IndexInsertPlan> {
	Reference<IInsertOp> indexInsert;
	bson::BSONObj indexObj;
	Namespace ns;
	Reference<MetadataManager> mm;

	IndexInsertPlan(Reference<IInsertOp> indexInsert,
	                bson::BSONObj indexObj,
	                Namespace ns,
	                Reference<MetadataManager> mm)
	    : indexInsert(indexInsert), indexObj(indexObj), ns(ns), mm(mm) {}

	bson::BSONObj describe() override {
		return BSON("type"
		            << "index insert");
	}
	PlanType getType() override { return PlanType::IndexInsert; }
	FutureStream<Reference<ScanReturnedContext>> execute(PlanCheckpoint* checkpoint,
	                                                     Reference<DocTransaction> tr) override;
};

struct BuildIndexPlan : ConcretePlan<BuildIndexPlan> {
	Reference<Plan> scan;
	Reference<IndexInfo> index;
	std::string dbName;
	Standalone<StringRef> encodedIndexId;
	Reference<MetadataManager> mm;

	BuildIndexPlan(Reference<Plan> scan,
	               Reference<IndexInfo> index,
	               std::string const& dbName,
	               Standalone<StringRef> encodedIndexId,
	               Reference<MetadataManager> mm)
	    : scan(scan), index(index), dbName(dbName), encodedIndexId(encodedIndexId), mm(mm) {}

	bson::BSONObj describe() override {
		return BSON("type"
		            << "build index");
	}
	PlanType getType() override { return PlanType::BuildIndex; }
	FutureStream<Reference<ScanReturnedContext>> execute(PlanCheckpoint* checkpoint,
	                                                     Reference<DocTransaction> tr) override;
	bool wasMetadataChangeOkay(Reference<UnboundCollectionContext> cx) override;
};

struct UpdateIndexStatusPlan : ConcretePlan<UpdateIndexStatusPlan> {
	Namespace ns;
	Standalone<StringRef> encodedIndexId;
	Reference<MetadataManager> mm;
	std::string newStatus;
	UID buildId;

	UpdateIndexStatusPlan(Namespace const& ns,
	                      Standalone<StringRef> encodedIndexId,
	                      Reference<MetadataManager> mm,
	                      std::string newStatus,
	                      UID buildId)
	    : ns(ns), encodedIndexId(encodedIndexId), mm(mm), newStatus(newStatus), buildId(buildId) {}

	bson::BSONObj describe() override {
		return BSON("type"
		            << "update index status");
	}
	PlanType getType() override { return PlanType::UpdateIndexStatus; }
	FutureStream<Reference<ScanReturnedContext>> execute(PlanCheckpoint* checkpoint,
	                                                     Reference<DocTransaction> tr) override;
};

struct FlushChangesPlan : ConcretePlan<FlushChangesPlan> {
	Reference<Plan> subPlan;

	explicit FlushChangesPlan(Reference<Plan> subPlan) : subPlan(subPlan) {}

	bson::BSONObj describe() override {
		return BSON(
		    // clang-format off
			"type" << "flush changes" <<
			"source_plan" << subPlan->describe()
		    // clang-format on
		);
	}
	PlanType getType() override { return PlanType::FlushChanges; }
	bool hasScanOfType(PlanType type) override { return subPlan->hasScanOfType(type); }
	FutureStream<Reference<ScanReturnedContext>> execute(PlanCheckpoint* checkpoint,
	                                                     Reference<DocTransaction> tr) override;
};

// Like executeUntilCompletion(), but uses the transaction you gave it.
ACTOR Future<int64_t> executeUntilCompletionTransactionally(Reference<Plan> plan, Reference<DocTransaction> tr);
// Like executeUntilCompletionTransactionally(), but also returns the last thing returned by the plan (if any).
ACTOR Future<std::pair<int64_t, Reference<ScanReturnedContext>>> executeUntilCompletionAndReturnLastTransactionally(
    Reference<Plan> plan,
    Reference<DocTransaction> tr);

Reference<Plan> deletePlan(Reference<Plan> subPlan, Reference<UnboundCollectionContext> cx, int64_t limit);
Reference<Plan> flushChanges(Reference<Plan> subPlan);
Reference<Plan> oplogInsertPlan(Reference<Plan> subPlan,
								std::list<bson::BSONObj>* docs,
								Reference<IOplogInserter> oplogInserter,
								Reference<MetadataManager> mm,
								Namespace ns);

#endif /* _QL_PLAN_ACTOR_H_ */