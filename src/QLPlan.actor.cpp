/*
 * QLPlan.actor.cpp
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

#include "QLPlan.actor.h"
#include "DocumentError.h"
#include "ExtStructs.h"
#include "ExtUtil.actor.h"
#include "QLProjection.actor.h"

#include "flow/actorcompiler.h" // This must be the last #include.
#include "ordering.h"

using namespace FDB;

/**
 * Plan::execute() contract
 *
 * 	- The actors required to implement the plan must be created synchronously (i.e. during the call to execute(),
 * 	without waiting), and passed to PlanCheckpoint::addOperation in topological sort order.  (Plan actors may create
 * 	additional actors to process individual documents, but the actors that operate on document streams must be passed to
 * 	addOperation())
 *
 * 	- execute() must synchronously and in a consistent order call execute() on any subplans whose evaluation is needed
 * 	to evaluate the plan (The consistent order is required to ensure that addScan() is called in a consistent order by
 * 	subplans)
 *
 * 	- All plan actors that have input document streams must be waiting on those streams at all times.  (It is fine to
 * 	choose on the input stream and some other stream, but not OK to wait on another future without waiting on the input
 * 	stream). In other words, it is illegal to let documents accumulate in the PromiseStream.
 *
 * 	- Scan plans (those that output documents that they do not receive from a subplan) must
 * 		- call addScan() to obtain a scan ID
 * 		- output the scan ID with all documents they output
 * 		- output a monotonically increasing scan key less than "\xff" with each document they output.
 * 		- check PlanCheckpoint::getBounds() and ensure that they efficiently limit their scan to documents having scan
 * 		  keys in the given [begin,end) range when cancelled without completing, if PlanCheckpoint::splitBoundWanted()
 * 		  is true, set PlanCheckpoint::splitBound(scanID) to a scan key greater than that of the last document output
 * 		  and less than or equal to that of the next document the scan could possibly output.
 * 		- PlanCheckpoint::getDocumentFinishedLock()->take() before outputting each document
 *  - Asynchronous plans (those that may output documents later than they are received from an input stream) must
 *    when cancelled without completing, iterate over the documents they have recieved but not yet output in the reverse
 *    of the order they would be output and set the split key for the given scan ID to the scan key of the document
 *  - Filtering plans (those that may not output every document they receive on an input) must
 *  	- PlanCheckpoint::getDocumentFinishedLock()->release() each document that they discard
 */
Reference<Plan> FilterPlan::construct_filter_plan(Reference<UnboundCollectionContext> cx,
                                                  Reference<Plan> source,
                                                  Reference<IPredicate> filter) {
	if (filter->getTypeCode() == IPredicate::ALL) {
		return source;
	}
	Optional<Reference<Plan>> pdPlan = source->push_down(cx, filter);
	if (pdPlan.present()) {
		if (verboseLogging) {
			TraceEvent("BD_construct_filter_plan")
			    .detail("source_plan", source->describe().toString())
			    .detail("pushed_down_into", pdPlan.get()->describe().toString());
		}
		return pdPlan.get();
	}
	return ref(new FilterPlan(cx, source, filter));
}

Optional<Reference<Plan>> FilterPlan::push_down(Reference<UnboundCollectionContext> cx, Reference<IPredicate> query) {
	return ref(new FilterPlan(cx, source, ref(new AndPredicate(filter, query))->simplify()));
}

Optional<Reference<Plan>> TableScanPlan::push_down(Reference<UnboundCollectionContext> cx,
                                                   Reference<IPredicate> query) {
	switch (query->getTypeCode()) {
	case IPredicate::ANY: {
		auto anyPred = dynamic_cast<AnyPredicate*>(query.getPtr());
		if (anyPred->expr->get_index_key() == DocLayerConstants::ID_FIELD) {
			Optional<DataValue> begin, end;
			anyPred->pred->get_range(begin, end);
			if (begin.present() || end.present()) {
				if (anyPred->pred->range_is_tight()) {
					return ref(new PrimaryKeyLookupPlan(cx, begin, end));
				} else {
					return FilterPlan::construct_filter_plan(cx, ref(new PrimaryKeyLookupPlan(cx, begin, end)), query);
				}
			}
		} else {
			std::string indexKey = anyPred->expr->get_index_key();
			Optional<Reference<IndexInfo>> oIndex = cx->getSimpleIndex(indexKey);
			if (oIndex.present()) {
				Optional<DataValue> begin, end;
				anyPred->pred->get_range(begin, end);
				if (begin.present() || end.present()) {
					Optional<Key> beginKey = begin.present() ? begin.get().encode_key_part() : Optional<Key>();
					Optional<Key> endKey = end.present() ? end.get().encode_key_part() : Optional<Key>();
					if (anyPred->pred->range_is_tight()) {
						return ref(new IndexScanPlan(cx, oIndex.get(), beginKey, endKey, {indexKey}));
					} else {
						return FilterPlan::construct_filter_plan(
						    cx, ref(new IndexScanPlan(cx, oIndex.get(), beginKey, endKey, {indexKey})), query);
					}
				}
			}
		}

		return Optional<Reference<Plan>>();
	}
	case IPredicate::OR: {
		std::vector<Reference<IPredicate>> terms = dynamic_cast<OrPredicate*>(query.getPtr())->terms;
		Reference<IPredicate> last = terms.back();
		Optional<Reference<Plan>> lastPlan = push_down(cx, last);
		if (lastPlan.present()) {
			std::vector<Reference<IPredicate>> pdTerms = std::vector<Reference<IPredicate>>(terms);
			pdTerms.pop_back();
			std::vector<Reference<IPredicate>> and_terms;
			and_terms.emplace_back(new OrPredicate(pdTerms));
			and_terms.emplace_back(new NotPredicate(last));
			Optional<Reference<Plan>> pd = push_down(cx, ref(new AndPredicate(and_terms))->simplify());
			if (pd.present()) {
				return Optional<Reference<Plan>>(ref(new UnionPlan(pd.get(), lastPlan.get())));
			} else {
				return Optional<Reference<Plan>>();
			}
		} else {
			return Optional<Reference<Plan>>();
		}
	}
	case IPredicate::AND: {
		std::vector<Reference<IPredicate>> terms = dynamic_cast<AndPredicate*>(query.getPtr())->terms;
		std::vector<Reference<Plan>> plans;
		for (int i = 0; i < terms.size(); ++i) {
			Reference<IPredicate> this_term = terms[i];
			Optional<Reference<Plan>> pd = push_down(cx, this_term);
			if (pd.present()) {
				std::vector<Reference<IPredicate>> other_terms =
				    std::vector<Reference<IPredicate>>(terms.begin(), terms.begin() + i);
				other_terms.insert(other_terms.end(), terms.begin() + (i + 1), terms.end());
				plans.push_back(
				    FilterPlan::construct_filter_plan(cx, pd.get(), ref(new AndPredicate(other_terms))->simplify()));
				// SOMEDAY: Don't break here
				break;
			}
		}
		if (!plans.empty()) {
			// SOMEDAY: return race(plans);
			return Optional<Reference<Plan>>(plans.front());
		} else {
			return Optional<Reference<Plan>>();
		}
	}
	case IPredicate::NONE: {
		return ref(new EmptyPlan());
	}
	default:
		return Optional<Reference<Plan>>();
	}
	return Optional<Reference<Plan>>();
}

// FIXME: yuck

static inline void operator+=(std::string& lhs, StringRef const& rhs) {
	lhs.append((const char*)rhs.begin(), rhs.size());
}

static inline std::string strAppend(std::string& lhs, StringRef const& rhs) {
	std::string r;
	r.reserve(lhs.size() + rhs.size());
	r = lhs;
	r += rhs;
	return r;
}

Optional<Reference<Plan>> IndexScanPlan::push_down(Reference<UnboundCollectionContext> cx,
                                                   Reference<IPredicate> query) {
	if (single_key()) {
		switch (query->getTypeCode()) {
		case IPredicate::ANY: {
			auto anyPred = dynamic_cast<AnyPredicate*>(query.getPtr());
			Optional<Reference<IndexInfo>> oIndex = cx->getCompoundIndex(matchedPrefix, anyPred->expr->get_index_key());
			if (oIndex.present()) {
				Optional<DataValue> beginSuffix, endSuffix;
				anyPred->pred->get_range(beginSuffix, endSuffix);
				if (beginSuffix.present() || endSuffix.present()) {
					Key beginKeySuffix =
					    beginSuffix.present() ? beginSuffix.get().encode_key_part() : LiteralStringRef("\x00");
					Key endKeySuffix =
					    endSuffix.present() ? endSuffix.get().encode_key_part() : LiteralStringRef("\xff");
					std::vector<std::string> newPrefix(matchedPrefix);
					newPrefix.push_back(anyPred->expr->get_index_key());
					if (anyPred->pred->range_is_tight()) {
						return ref(new IndexScanPlan(
						    cx, oIndex.get(), begin.present() ? begin.get().withSuffix(beginKeySuffix) : begin,
						    end.present() ? end.get().withSuffix(endKeySuffix) : end, newPrefix));
					} else {
						return FilterPlan::construct_filter_plan(
						    cx,
						    ref(new IndexScanPlan(cx, oIndex.get(),
						                          begin.present() ? begin.get().withSuffix(beginKeySuffix) : begin,
						                          end.present() ? end.get().withSuffix(endKeySuffix) : end, newPrefix)),
						    query);
					}
				}
			}
			return Optional<Reference<Plan>>();
		}
		case IPredicate::AND: {
			std::vector<Reference<IPredicate>> terms = dynamic_cast<AndPredicate*>(query.getPtr())->terms;
			std::vector<Reference<Plan>> plans;
			for (int i = 0; i < terms.size(); ++i) {
				Reference<IPredicate> this_term = terms[i];
				Optional<Reference<Plan>> pd = push_down(cx, this_term);
				if (pd.present()) {
					std::vector<Reference<IPredicate>> other_terms =
					    std::vector<Reference<IPredicate>>(terms.begin(), terms.begin() + i);
					other_terms.insert(other_terms.end(), terms.begin() + i + 1, terms.end());
					plans.push_back(FilterPlan::construct_filter_plan(cx, pd.get(),
					                                                  ref(new AndPredicate(other_terms))->simplify()));
					// SOMEDAY: Don't break here
					break;
				}
			}
			if (!plans.empty()) {
				// SOMEDAY: return race(plans);
				return plans.front();
			} else {
				return Optional<Reference<Plan>>();
			}
			break;
		}
		default:
			return Optional<Reference<Plan>>();
		}
	}
	// Issue #16: Added return statement in right place to fix the warning during compilation
	return Optional<Reference<Plan>>();
}

ACTOR static Future<Void> doFilter(PlanCheckpoint* checkpoint,
                                   FutureStream<Reference<ScanReturnedContext>> input,
                                   PromiseStream<Reference<ScanReturnedContext>> output,
                                   Reference<IPredicate> predicate) {
	state Deque<std::pair<Reference<ScanReturnedContext>, Future<bool>>> futures;
	state std::pair<Reference<ScanReturnedContext>, Future<bool>> p;
	state FlowLock* flowControlLock = checkpoint->getDocumentFinishedLock();
	try {
		loop {
			try {
				choose {
					when(Reference<ScanReturnedContext> nextInput = waitNext(input)) {
						futures.push_back(std::pair<Reference<ScanReturnedContext>, Future<bool>>(
						    nextInput, predicate->evaluate(nextInput)));
					}
					when(bool pass = wait(futures.empty() ? Never() : futures.front().second)) {
						if (pass)
							output.send(futures.front().first);
						else
							flowControlLock->release();
						futures.pop_front();
					}
				}
			} catch (Error& e) {
				if (e.code() == error_code_end_of_stream)
					break;
				else
					throw;
			}
		}

		while (!futures.empty()) {
			p = futures.front();
			bool pass = wait(p.second);
			if (pass)
				output.send(p.first);
			else
				flowControlLock->release();
			futures.pop_front();
		}

		throw end_of_stream();
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled) {
			if (checkpoint->splitBoundWanted()) {
				for (int i = futures.size() - 1; i >= 0; i--)
					checkpoint->splitBound(futures[i].first->scanId()) = futures[i].first->scanKey();
			}
		} else
			output.sendError(e);
		throw;
	}
}

FutureStream<Reference<ScanReturnedContext>> FilterPlan::execute(PlanCheckpoint* checkpoint,
                                                                 Reference<DocTransaction> tr) {
	PromiseStream<Reference<ScanReturnedContext>> output;
	checkpoint->addOperation(doFilter(checkpoint, source->execute(checkpoint, tr), output, filter), output);
	return output.getFuture();
}

ACTOR static Future<Void> toDocInfo(PlanCheckpoint* checkpoint,
                                    Reference<IReadWriteContext> base,
                                    int scanID,
                                    GenFutureStream<KeyValue> index_keys,
                                    PromiseStream<Reference<ScanReturnedContext>> dis,
                                    Reference<FlowLock> inputLock) {
	// Each key has a document ID as its last entry
	state Key lastKey;
	state FlowLock* outputLock = checkpoint->getDocumentFinishedLock();
	state uint32_t nrDocs = 0;
	try {
		loop {
			state KeyValue kv = waitNext(index_keys);
			inputLock->release();
			wait(outputLock->take());
			lastKey = Key(kv.key, kv.arena());
			Standalone<StringRef> last(DataKey::decode_item_rev(kv.key, 0), kv.arena());
			Reference<ScanReturnedContext> output(new ScanReturnedContext(base->getSubContext(last), scanID, lastKey));
			dis.send(output);
			nrDocs++;
		}
	} catch (Error& e) {
		DocumentLayer::metricReporter->captureMeter(DocLayerConstants::MT_RATE_IDX_SCAN_DOCS, nrDocs);
		if (e.code() == error_code_actor_cancelled) {
			if (checkpoint->splitBoundWanted())
				checkpoint->splitBound(scanID) = keyAfter(lastKey);
			throw;
		}
		if (e.code() != error_code_end_of_stream)
			TraceEvent(SevError, "BD_toDocInfo_error").error(e);
		dis.sendError(e);
		throw;
	}
}

ACTOR static Future<bool> simpleWouldBeLast(Reference<ScanReturnedContext> doc,
                                            std::vector<Reference<IExpression>> expr,
                                            FDB::Key indexUpperBound) {
	std::vector<DataValue> old_values = wait(consumeAll(mapAsync(
	    expr[0]->evaluate(doc), [](Reference<IReadContext> valcx) { return getMaybeRecursive(valcx, StringRef()); })));
	if (old_values.size() == 1)
		return true;
	else {
		std::vector<Standalone<StringRef>> old_key_parts;
		old_key_parts.reserve(old_values.size());
		for (const auto& dv : old_values)
			old_key_parts.push_back(dv.encode_key_part());
		std::sort(old_key_parts.begin(), old_key_parts.end());
		Standalone<StringRef> last;
		for (auto s = old_key_parts.rbegin(); !(s == old_key_parts.rend()); ++s) {
			if (*s < indexUpperBound) {
				last = *s;
				break;
			}
		}
		if (doc->scanKey().startsWith(last))
			return true;
	}
	return false;
}

ACTOR static Future<bool> compoundWouldBeLast(Reference<ScanReturnedContext> doc,
                                              std::vector<Reference<IExpression>> exprs,
                                              FDB::Key indexUpperBound) {
	std::vector<Future<std::vector<DataValue>>> f_old_values;
	for (const auto& expr : exprs) {
		f_old_values.push_back(consumeAll(mapAsync(
		    expr->evaluate(doc), [](Reference<IReadContext> valcx) { return getMaybeRecursive(valcx, StringRef()); })));
	}
	state std::vector<std::vector<DataValue>> old_values = wait(getAll(f_old_values));

	int old_values_size = 1;
	for (const auto& v : old_values) {
		old_values_size *= v.size();
	}

	if (old_values_size == 1)
		return true;
	else {
		std::vector<std::string> old_key_parts;
		old_key_parts.reserve(old_values_size);
		for (cartesian_product_iterator<DataValue, std::vector<DataValue>::iterator> vv(old_values); vv; ++vv) {
			std::string buildingKey;
			for (int i = 0; i < vv.size(); i++)
				buildingKey.append(vv[i].encode_key_part().toString());
			old_key_parts.push_back(buildingKey);
		}
		std::sort(old_key_parts.begin(), old_key_parts.end());
		std::string last;
		for (auto s = old_key_parts.rbegin(); !(s == old_key_parts.rend()); ++s) {
			if (*s < indexUpperBound) {
				last = *s;
				break;
			}
		}
		if (doc->scanKey().startsWith(last))
			return true;
	}
	return false;
}

ACTOR static Future<Void> deduplicateIndexStream(PlanCheckpoint* checkpoint,
                                                 Reference<IndexInfo> self,
                                                 FDB::Key indexUpperBound,
                                                 FutureStream<Reference<ScanReturnedContext>> dis,
                                                 PromiseStream<Reference<ScanReturnedContext>> filtered) {
	state Deque<std::pair<Reference<ScanReturnedContext>, Future<bool>>> futures;
	state std::pair<Reference<ScanReturnedContext>, Future<bool>> p;
	state std::vector<Reference<IExpression>> exprs;
	for (const auto& indexKey : self->indexKeys)
		exprs.push_back(Reference<IExpression>(new ExtPathExpression(indexKey.first, true, true)));
	state FlowLock* flowControlLock = checkpoint->getDocumentFinishedLock();
	try {
		loop {
			try {
				choose {
					when(Reference<ScanReturnedContext> nextInput = waitNext(dis)) {
						futures.push_back(std::pair<Reference<ScanReturnedContext>, Future<bool>>(
						    nextInput, (self->size() == 1 ? simpleWouldBeLast : compoundWouldBeLast)(nextInput, exprs,
						                                                                             indexUpperBound)));
					}
					when(bool pass = wait(futures.empty() ? Never() : futures.front().second)) {
						if (pass)
							filtered.send(futures.front().first);
						else
							flowControlLock->release();
						futures.pop_front();
					}
				}
			} catch (Error& e) {
				if (e.code() == error_code_end_of_stream)
					break;
				else
					throw;
			}
		}

		while (!futures.empty()) {
			p = futures.front();
			bool pass = wait(p.second);
			if (pass)
				filtered.send(p.first);
			else
				flowControlLock->release();
			futures.pop_front();
		}

		throw end_of_stream();
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled) {
			if (checkpoint->splitBoundWanted()) {
				for (int i = futures.size() - 1; i >= 0; i--)
					checkpoint->splitBound(futures[i].first->scanId()) = futures[i].first->scanKey();
			}
		} else
			filtered.sendError(e);
		throw;
	}
}

FutureStream<Reference<ScanReturnedContext>> IndexScanPlan::execute(PlanCheckpoint* checkpoint,
                                                                    Reference<DocTransaction> tr) {
	Reference<QueryContext> index_cx = index->indexCx->bindQueryContext(tr);
	Reference<CollectionContext> bcx = cx->bindCollectionContext(tr);
	int scanID = checkpoint->addScan();
	PromiseStream<Reference<ScanReturnedContext>> p;
	FDB::Key lowerBound =
	    std::max(begin.present() ? begin.get() : LiteralStringRef("\x00"), checkpoint->getBounds(scanID).begin);
	FDB::Key upperBound =
	    std::max<FDB::Key>(lowerBound, std::min(end.present() ? strinc(end.get()) : LiteralStringRef("\xff"),
	                                            checkpoint->getBounds(scanID).end));
	Reference<FlowLock> flowControlLock(new FlowLock(1));
	GenFutureStream<KeyValue> kvs = index_cx->getDescendants(lowerBound, upperBound, flowControlLock);
	checkpoint->addOperation(kvs.actor && toDocInfo(checkpoint, bcx->cx, scanID, kvs, p, flowControlLock), p);

	if (begin.present() && end.present() && begin.get() == end.get() && index->size() == 1) {
		return p.getFuture();
	} else {
		PromiseStream<Reference<ScanReturnedContext>> p2;
		checkpoint->addOperation(deduplicateIndexStream(checkpoint, index, upperBound, p.getFuture(), p2), p2);
		return p2.getFuture();
	}
}

ACTOR static Future<Void> doSinglePKLookup(PlanCheckpoint* checkpoint,
                                           PromiseStream<Reference<ScanReturnedContext>> dis,
                                           Reference<CollectionContext> cx,
                                           DataValue begin,
                                           int scanID) {
	state FlowLock* flowControlLock = checkpoint->getDocumentFinishedLock();	
	try {
		state Standalone<StringRef> x = begin.encode_key_part();
		FDB::KeyRangeRef scanBounds = checkpoint->getBounds(scanID);
		if (x >= scanBounds.begin && x < scanBounds.end) {
			Optional<DataValue> odv = wait(cx->cx->get(x));
			if (odv.present()) {
				wait(flowControlLock->take());
				dis.send(Reference<ScanReturnedContext>(new ScanReturnedContext(
					cx->cx->getSubContext(begin.encode_key_part()), scanID, StringRef(begin.encode_key_part()))));
			}
		}
		throw end_of_stream();
	} catch (Error& e) {
		dis.sendError(e);
		throw;
	}
}

ACTOR static Future<Void> doPKScan(PlanCheckpoint* checkpoint,
                                   Reference<CollectionContext> cx,
                                   int scanID,
                                   GenFutureStream<KeyValue> kvs,
                                   PromiseStream<Reference<ScanReturnedContext>> output,
                                   Reference<FlowLock> inputLock) {
	state FlowLock* outputLock = checkpoint->getDocumentFinishedLock();
	state Standalone<StringRef> lastPK;
	state Key lastKey;
	state uint32_t nrDocs = 0;
	try {
		loop {
			state KeyValue kv = waitNext(kvs);
			inputLock->release();
			StringRef curPK(DataKey::decode_item(kv.key, 0));
			if (curPK.compare(lastPK)) {
				lastPK = Standalone<StringRef>(curPK, kv.arena());
				// We are adding a brand new document, so
				wait(outputLock->take());
				output.send(Reference<ScanReturnedContext>(
				    new ScanReturnedContext(cx->cx->getSubContext(lastPK), scanID, Key(kv.key, kv.arena()))));
				nrDocs++;
			}
			// This needs to happen down here, so that we don't reset the split bound one later if we're cancelled while
			// failing to get the lock.
			lastKey = Key(kv.key, kv.arena());
		}
	} catch (Error& e) {
		DocumentLayer::metricReporter->captureMeter(DocLayerConstants::MT_RATE_TABLE_SCAN_DOCS, nrDocs);
		if (e.code() == error_code_actor_cancelled) {
			if (checkpoint->splitBoundWanted()) {
				DataKey splitKey = DataKey::decode_bytes(lastKey);
				checkpoint->splitBound(scanID) = strinc(splitKey[0]);
			}
			throw;
		}
		output.sendError(e);
		throw;
	}
}

FutureStream<Reference<ScanReturnedContext>> PrimaryKeyLookupPlan::execute(PlanCheckpoint* checkpoint,
                                                                           Reference<DocTransaction> tr) {
	int scanID = checkpoint->addScan();
	Reference<CollectionContext> bcx = cx->bindCollectionContext(tr);
	if (begin.present() && end.present() && begin.get() == end.get()) {
		PromiseStream<Reference<ScanReturnedContext>> p;
		// ??? Can we skip this overhead?
		checkpoint->addOperation(doSinglePKLookup(checkpoint, p, bcx, begin.get(), scanID), p);
		return p.getFuture();
	} else {
		PromiseStream<Reference<ScanReturnedContext>> p;
		Reference<FlowLock> descendantFlowControlLock(new FlowLock(1));

		Standalone<StringRef> beginKey =
		    std::max(begin.present() ? begin.get().encode_key_part() : LiteralStringRef("\x00"),
		             checkpoint->getBounds(scanID).begin);
		Standalone<StringRef> endKey = std::max<Standalone<StringRef>>(
		    beginKey, std::min(end.present() ? strinc(end.get().encode_key_part()) : LiteralStringRef("\xff"),
		                       checkpoint->getBounds(scanID).end));

		GenFutureStream<KeyValue> kvs = bcx->cx->getDescendants(beginKey, endKey, descendantFlowControlLock);
		//< descendantFlowControlLock is actually being moved
		checkpoint->addOperation(doPKScan(checkpoint, bcx, scanID, kvs, p, descendantFlowControlLock), p);
		return p.getFuture();
	}
}

ACTOR static Future<Void> doUnion(FutureStream<Reference<ScanReturnedContext>> a,
                                  FutureStream<Reference<ScanReturnedContext>> b,
                                  PromiseStream<Reference<ScanReturnedContext>> output) {
	state Future<Reference<ScanReturnedContext>> aFuture = waitAndForward(a);
	state Future<Reference<ScanReturnedContext>> bFuture = waitAndForward(b);
	state bool aOpen = true;
	state bool bOpen = true;

	loop {
		try {
			choose {
				when(Reference<ScanReturnedContext> val = wait(aFuture)) {
					output.send(val);
					aFuture = waitAndForward(a);
				}
				when(Reference<ScanReturnedContext> val = wait(bFuture)) {
					output.send(val);
					bFuture = waitAndForward(b);
				}
			}
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled)
				throw;

			if (e.code() != error_code_end_of_stream) {
				output.sendError(e);
				throw;
			}

			ASSERT(!aFuture.isError() || !bFuture.isError() || aFuture.getError().code() == bFuture.getError().code());

			if (aFuture.isError()) {
				aFuture = Never();
				aOpen = false;				
			}
			if (bFuture.isError()) {
				bFuture = Never();
				bOpen = false;
			}

			if (!aOpen && !bOpen) {
				output.sendError(e);
				throw;
			}
		}
	}
}

FutureStream<Reference<ScanReturnedContext>> UnionPlan::execute(PlanCheckpoint* checkpoint,
                                                                Reference<DocTransaction> tr) {
	PromiseStream<Reference<ScanReturnedContext>> output;
	checkpoint->addOperation(doUnion(plan1->execute(checkpoint, tr), plan2->execute(checkpoint, tr), output), output);
	return output.getFuture();
}

FutureStream<Reference<ScanReturnedContext>> TableScanPlan::execute(PlanCheckpoint* checkpoint,
                                                                    Reference<DocTransaction> tr) {
	Reference<CollectionContext> bcx = cx->bindCollectionContext(tr);
	int scanID = checkpoint->addScan();
	PromiseStream<Reference<ScanReturnedContext>> p;
	Reference<FlowLock> descendantFlowControlLock(new FlowLock(1));
	Standalone<StringRef> beginKey = std::max(LiteralStringRef("\x00"), checkpoint->getBounds(scanID).begin);
	Standalone<StringRef> endKey = std::max<Standalone<StringRef>>(
	    beginKey, std::min(LiteralStringRef("\xff"), checkpoint->getBounds(scanID).end));
	GenFutureStream<KeyValue> kvs = bcx->cx->getDescendants(beginKey, endKey, descendantFlowControlLock);
	//< descendantFlowControlLock is actually being moved
	checkpoint->addOperation(doPKScan(checkpoint, bcx, scanID, kvs, p, descendantFlowControlLock), p);
	return p.getFuture();
}

Reference<DocTransaction> NonIsolatedPlan::newTransaction() {
	Reference<FDB::Transaction> tr = database->createTransaction();
	int64_t timeoutMS = 4000;
	tr->setOption(FDB_TR_OPTION_TIMEOUT, StringRef((uint8_t*)&timeoutMS, sizeof(timeoutMS)));
	tr->setOption(FDB_TR_OPTION_CAUSAL_READ_RISKY);
	return DocTransaction::create(tr);
}

Reference<DocTransaction> NonIsolatedPlan::newTransaction(Reference<FDB::Database> database) {
	Reference<FDB::Transaction> tr = database->createTransaction();
	int64_t timeoutMS = 4000;
	tr->setOption(FDB_TR_OPTION_TIMEOUT, StringRef((uint8_t*)&timeoutMS, sizeof(timeoutMS)));
	tr->setOption(FDB_TR_OPTION_CAUSAL_READ_RISKY);
	return DocTransaction::create(tr);
}

ACTOR static Future<Void> doNonIsolatedRO(PlanCheckpoint* outerCheckpoint,
                                          Reference<Plan> subPlan,
                                          PromiseStream<Reference<ScanReturnedContext>> output,
                                          Reference<UnboundCollectionContext> cx,
                                          Reference<NonIsolatedPlan> self,
                                          Reference<DocTransaction> dtr,
                                          Reference<MetadataManager> mm) {
	if (!dtr)
		dtr = self->newTransaction();
	state Reference<PlanCheckpoint> innerCheckpoint(new PlanCheckpoint);
	state int nTransactions = 1;
	state int64_t nResults = 0;
	state FlowLock* outerLock = outerCheckpoint->getDocumentFinishedLock();
	state Deque<std::pair<Reference<ScanReturnedContext>, Future<Void>>> bufferedDocs;

	try {
		state uint64_t metadataVersion = wait(cx->bindCollectionContext(dtr)->getMetadataVersion());
		loop {
			state FutureStream<Reference<ScanReturnedContext>> docs = subPlan->execute(innerCheckpoint.getPtr(), dtr);
			state FlowLock* innerLock = innerCheckpoint->getDocumentFinishedLock();
			state bool first = true;
			state Future<Void> timeout = delay(3.0, g_network->getCurrentTask() + 1);
			state bool finished = false;

			try {
				loop choose {
					when(Reference<ScanReturnedContext> doc = waitNext(docs)) {
						// throws end_of_stream when totally finished
						bufferedDocs.push_back(std::make_pair(doc, outerLock->take(g_network->getCurrentTask() + 1)));
					}
					when(wait(bufferedDocs.empty() ? Never() : bufferedDocs.front().second)) {
						innerLock->release();
						output.send(bufferedDocs.front().first);
						bufferedDocs.pop_front();
						++nResults;
						if (first) {
							timeout =
							    delay(DOCLAYER_KNOBS->NONISOLATED_INTERNAL_TIMEOUT, g_network->getCurrentTask() + 1);
							first = false;
						}
					}
					when(wait(timeout)) { break; }
				}
				ASSERT(!docs.isReady());
			} catch (Error& e) {
				if (e.code() != error_code_end_of_stream)
					throw;
				finished = true;
			}

			innerCheckpoint = innerCheckpoint->stopAndCheckpoint();

			while (!bufferedDocs.empty()) {
				wait(bufferedDocs.front().second);
				output.send(bufferedDocs.front().first);
				bufferedDocs.pop_front();
				++nResults;
			}

			if (finished)
				throw end_of_stream();

			dtr = self->newTransaction();
			state uint64_t newMetadataVersion = wait(cx->bindCollectionContext(dtr)->getMetadataVersion());
			if (newMetadataVersion != metadataVersion) {
				Reference<UnboundCollectionContext> newCx = wait(mm->refreshUnboundCollectionContext(cx, dtr));
				if (newCx->collectionDirectory->key() != cx->collectionDirectory->key() ||
				    newCx->metadataDirectory->key() != cx->metadataDirectory->key())
					throw collection_metadata_changed();
				if (subPlan->wasMetadataChangeOkay(newCx)) {
					metadataVersion = newMetadataVersion;
				} else {
					throw metadata_changed_nonisolated();
				}
			}

			++nTransactions;
		}
	} catch (Error& e) {
		DocumentLayer::metricReporter->captureHistogram(DocLayerConstants::MT_HIST_TR_PER_REQUEST, nTransactions);
		innerCheckpoint->stop();
		output.sendError(e);
		throw;
	}
}

ACTOR static Future<Void> doNonIsolatedRW(PlanCheckpoint* outerCheckpoint,
                                          Reference<Plan> subPlan,
                                          PromiseStream<Reference<ScanReturnedContext>> output,
                                          Reference<UnboundCollectionContext> cx,
                                          Reference<NonIsolatedPlan> self,
                                          Reference<DocTransaction> dtr,
                                          Reference<MetadataManager> mm) {
	if (!dtr)
		dtr = self->newTransaction();
	state Reference<PlanCheckpoint> innerCheckpoint(new PlanCheckpoint);
	state FlowLock* outerLock = outerCheckpoint->getDocumentFinishedLock();
	state int oCount = 0;
	state int nTransactions = 1;

	state Reference<Oplogger> oplogger = ref(new Oplogger(
		Namespace(cx->databaseName(), cx->collectionName()),
		self->oplogInserter
	));

	try {
		state uint64_t metadataVersion = wait(cx->bindCollectionContext(dtr)->getMetadataVersion());
		loop {
			state FutureStream<Reference<ScanReturnedContext>> docs = subPlan->execute(innerCheckpoint.getPtr(), dtr);
			state FlowLock* innerLock = innerCheckpoint->getDocumentFinishedLock();
			state bool first = true;
			state bool finished = false;
			state Future<Void> timeout = delay(3.0, g_network->getCurrentTask() + 1);
			state Deque<std::pair<Reference<ScanReturnedContext>, Future<Void>>> committingDocs;
			state Deque<Reference<ScanReturnedContext>> bufferedDocs;

			try {
				try {
					loop {
						if (bufferedDocs.size() + committingDocs.size() >=
						    DOCLAYER_KNOBS->NONISOLATED_RW_INTERNAL_BUFFER_MAX)
							// We do this instead of breaking so that when stopAndCheckpoint() gets
							// called below, the actor for the plan immediately inside us is never
							// on the call stack, so gets its actor_cancelled delivered immediately.
							timeout = delay(0);
						choose {
							when(state Reference<ScanReturnedContext> doc =
							         waitNext(docs)) { // throws end_of_stream when totally finished	
								if (oplogger->isEnabled()) {									
									// Oplog. Add original doc.
									DataValue oDv = wait(doc->toDataValue());
									oplogger->addOriginalDoc(&oDv);								
								}

								committingDocs.push_back(std::make_pair(doc, doc->commitChanges()));						
								if (first) {
									timeout = delay(DOCLAYER_KNOBS->NONISOLATED_INTERNAL_TIMEOUT,
									                g_network->getCurrentTask() + 1);
									first = false;
								}
							}
							when(wait(committingDocs.empty() ? Never() : committingDocs.front().second)) {
								if (oplogger->isEnabled()) {
									// Oplog. Add updated doc.
									DataValue oDv = wait(committingDocs.front().first->toDataValue());
									oplogger->addUpdatedDoc(&oDv);
								}

								bufferedDocs.push_back(committingDocs.front().first);
								committingDocs.pop_front();
								innerLock->release();
							}
							when(wait(timeout)) { break; }
						}
					}
					ASSERT(!docs.isReady());
				} catch (Error& e) {
					if (e.code() != error_code_end_of_stream) {
						throw;
					} else
						finished = true;
				}

				// Cancel all ongoing work in the lower levels of the plan. Any document that hasn't made it to
				// committingDocs will not be updated in this transaction.
				state Reference<PlanCheckpoint> next_checkpoint = innerCheckpoint->stopAndCheckpoint();

				// This section MUST come before the call to dtr->cancel_ongoing_index_reads(), since these futures
				// refer to documents that we are considering committed.
				while (!committingDocs.empty()) {
					wait(committingDocs.front().second);

					if (oplogger->isEnabled()) {
						// Oplog. Add updated doc.
						DataValue oDv = wait(committingDocs.front().first->toDataValue());
						oplogger->addUpdatedDoc(&oDv);
					}

					bufferedDocs.push_back(committingDocs.front().first);
					committingDocs.pop_front();
				}

				if (oplogger->isEnabled()) {
					// Oplog. Commit operations.
					state Reference<UnboundCollectionContext> ucx = wait(oplogger->getUnboundContext(mm, dtr));
					state Reference<CollectionContext> opCtx = ucx->bindCollectionContext(dtr);
					
					state int i = 0;
					state Deque<Future<Reference<IReadWriteContext>>> logs = oplogger->buildOplogs(opCtx);
					for (; i < logs.size(); i++) {
						Reference<IReadWriteContext> _doc = wait(logs[i]);
						wait(_doc->commitChanges());
					}
				}


				// In this case (but not in the case of NonIsolatedRO, Retry, or FindAndModify), we must keep the
				// reference to the transaction alive (because we're going to commit it), but have not necessarily
				// consumed all of the outputs of subPlan->execute. This means there could be dependent index reads
				// (triggered by mutations) still pending when we call commit(). So the following function goes through
				// and cancels all of those so we don't get used_during_commit bubbling up from those actors.

				// Note that this is safe to do, because the deferred sets and clears that triggered those reads have
				// not made it through to the underlying FDB::Transaction yet, so the indexes are in a consistent state
				// when we cancel these actors.
				dtr->cancel_ongoing_index_reads();

				wait(dtr->tr->commit());

				// Ideally we shouldn't do anything on this transaction anymore. But caller of this code would try to
				// read the upserted document with this transaction. There is no need to use same 'dtr' transaction
				// except that code is structured in a way makes it hard to use any other transaction.
				dtr->tr = self->newTransaction()->tr;

				// Since commit succeeded, we can do the next part next instead of redoing this part
				innerCheckpoint = next_checkpoint;

				while (!bufferedDocs.empty()) {
					wait(outerLock->take());
					Reference<ScanReturnedContext> finishedDoc = bufferedDocs.front();
					output.send(finishedDoc);					
					++oCount;
					bufferedDocs.pop_front();
				}
			} catch (Error& e) {
				wait(dtr->onError(e));
				finished = false;
			}

			if (finished) {
				throw end_of_stream();
			}

			dtr = self->newTransaction(); // FIXME: keep dtr->tr if this is a retry
			state uint64_t newMetadataVersion = wait(cx->bindCollectionContext(dtr)->getMetadataVersion());
			if (newMetadataVersion != metadataVersion) {
				Reference<UnboundCollectionContext> newCx = wait(mm->refreshUnboundCollectionContext(cx, dtr));
				if (newCx->collectionDirectory->key() != cx->collectionDirectory->key() ||
				    newCx->metadataDirectory->key() != cx->metadataDirectory->key())
					throw collection_metadata_changed();
				if (subPlan->wasMetadataChangeOkay(newCx)) {
					metadataVersion = newMetadataVersion;
				} else {
					throw metadata_changed_nonisolated();
				}
			}
			nTransactions++;
		}
	} catch (Error& e) {
		DocumentLayer::metricReporter->captureHistogram(DocLayerConstants::MT_HIST_TR_PER_REQUEST, nTransactions);
		innerCheckpoint->stop();
		output.sendError(e);
		throw;
	}
}

FutureStream<Reference<ScanReturnedContext>> NonIsolatedPlan::execute(PlanCheckpoint* checkpoint,
                                                                      Reference<DocTransaction> tr) {
	PromiseStream<Reference<ScanReturnedContext>> docs;
	checkpoint->addOperation((isReadOnly ? doNonIsolatedRO : doNonIsolatedRW)(
	                             checkpoint, subPlan, docs, cx, Reference<NonIsolatedPlan>::addRef(this), tr, mm),
	                         docs);
	return docs.getFuture();
}

ACTOR static Future<Void> doRetry(Reference<Plan> subPlan,
                                  PromiseStream<Reference<ScanReturnedContext>> output,
                                  Reference<RetryPlan> self,
                                  PlanCheckpoint* outerCheckpoint,
                                  Reference<DocTransaction> tr) {
	if (!tr)
		tr = self->newTransaction();
	state std::vector<Reference<ScanReturnedContext>> ret;
	state FutureStream<Reference<ScanReturnedContext>> docs;
	state FlowLock* outerLock = outerCheckpoint->getDocumentFinishedLock();

	try {
		loop {
			try {
				state Reference<PlanCheckpoint> innerCheckpoint(new PlanCheckpoint);
				docs = subPlan->execute(innerCheckpoint.getPtr(), tr);
				state FlowLock* innerLock = innerCheckpoint->getDocumentFinishedLock();
				state Deque<std::pair<Reference<ScanReturnedContext>, Future<Void>>> committing;
				ret = std::vector<Reference<ScanReturnedContext>>();
				try {
					loop {
						choose {
							when(Reference<ScanReturnedContext> next = waitNext(docs)) {
								committing.push_back(std::make_pair(next, next->commitChanges()));
							}
							when(wait(committing.empty() ? Never() : committing.front().second)) {
								ret.push_back(committing.front().first);
								committing.pop_front();
								innerLock->release();
							}
						}
					}
				} catch (Error& e) {
					innerCheckpoint->stop();
					if (e.code() != error_code_end_of_stream)
						throw;
				}

				while (!committing.empty()) {
					wait(committing.front().second);
					ret.push_back(committing.front().first);
					committing.pop_front();
					innerLock->release();
				}

				wait(tr->tr->commit());
				// Ideally we shouldn't do anything on this transaction anymore. But caller of this code, createIndexes,
				// would try to read the added index with this transaction. There is no need to use same document
				// transaction except that code is structured in a way makes it hard to use any other transaction.
				tr->tr = self->newTransaction()->tr;

				state Reference<ScanReturnedContext> r;
				for (const Reference<ScanReturnedContext>& loopThing : ret) {
					r = loopThing;
					wait(outerLock->take());
					output.send(r);
				}
				throw end_of_stream();
			} catch (Error& e) {
				if (e.code() == error_code_commit_unknown_result)
					throw;
				if (e.code() == error_code_end_of_stream)
					throw;
				wait(tr->onError(e));
				tr = self->newTransaction(); // FIXME: keep dtr->tr if this is a retry
			}
		}
	} catch (Error& e) {
		output.sendError(e);
		throw;
	}
}

FutureStream<Reference<ScanReturnedContext>> RetryPlan::execute(PlanCheckpoint* checkpoint,
                                                                Reference<DocTransaction> tr) {
	PromiseStream<Reference<ScanReturnedContext>> docs;
	checkpoint->addOperation(doRetry(subPlan, docs, Reference<RetryPlan>::addRef(this), checkpoint, tr), docs);
	return docs.getFuture();
}

Reference<DocTransaction> RetryPlan::newTransaction() {
	Reference<FDB::Transaction> tr = database->createTransaction();
	tr->setOption(FDB_TR_OPTION_CAUSAL_READ_RISKY);
	tr->setOption(FDB_TR_OPTION_RETRY_LIMIT, StringRef((uint8_t*)&(retryLimit), sizeof(int64_t)));
	tr->setOption(FDB_TR_OPTION_TIMEOUT, StringRef((uint8_t*)&(timeout), sizeof(int64_t)));
	return DocTransaction::create(tr);
}

ACTOR static Future<Void> doProject(PlanCheckpoint* checkpoint,
                                    FutureStream<Reference<ScanReturnedContext>> input,
                                    PromiseStream<Reference<ScanReturnedContext>> output,
                                    Reference<Projection> projection,
                                    Optional<bson::BSONObj> ordering) {
	state Deque<std::pair<Reference<ScanReturnedContext>, Future<bson::BSONObj>>> futures;
	try {
		loop {
			try {
				choose {
					when(Reference<ScanReturnedContext> nextInput = waitNext(input)) {
						futures.push_back(std::pair<Reference<ScanReturnedContext>, Future<bson::BSONObj>>(
						    nextInput, projectDocument(nextInput, projection, ordering)));
					}
					when(bson::BSONObj proj = wait(futures.empty() ? Never() : futures.front().second)) {
						output.send(ref(new ScanReturnedContext(ref(new BsonContext(proj, false)),
						                                        futures.front().first->scanId(),
						                                        futures.front().first->scanKey())));
						futures.pop_front();
					}
				}
			} catch (Error& e) {
				if (e.code() == error_code_end_of_stream)
					break;
				else
					throw;
			}
		}

		while (!futures.empty()) {
			bson::BSONObj proj = wait(futures.front().second);
			output.send(ref(new ScanReturnedContext(ref(new BsonContext(proj, false)), futures.front().first->scanId(),
			                                        futures.front().first->scanKey())));
			futures.pop_front();
		}
		throw end_of_stream();
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled) {
			if (checkpoint->splitBoundWanted()) {
				for (int i = futures.size() - 1; i >= 0; i--)
					checkpoint->splitBound(futures[i].first->scanId()) = futures[i].first->scanKey();
			}
		} else
			output.sendError(e);
		throw;
	}
}

FutureStream<Reference<ScanReturnedContext>> ProjectionPlan::execute(PlanCheckpoint* checkpoint,
                                                                     Reference<DocTransaction> tr) {
	PromiseStream<Reference<ScanReturnedContext>> docs;
	checkpoint->addOperation(doProject(checkpoint, subPlan->execute(checkpoint, tr), docs, projection, ordering), docs);
	return docs.getFuture();
}

ACTOR static Future<Void> doFlushChanges(FutureStream<Reference<ScanReturnedContext>> input,
                                         PromiseStream<Reference<ScanReturnedContext>> output) {
	state Deque<std::pair<Reference<ScanReturnedContext>, Future<Void>>> futures;
	try {
		loop {
			try {
				choose {
					when(Reference<ScanReturnedContext> nextInput = waitNext(input)) {
						// FIXME: this will be unsafe with unique indexes. Something has to happen here that doesn't
						// kill performance.
						futures.push_back(std::pair<Reference<ScanReturnedContext>, Future<Void>>(
						    nextInput, nextInput->commitChanges()));
					}
					when(wait(futures.empty() ? Never() : futures.front().second)) {
						output.send(futures.front().first);
						futures.pop_front();
					}
				}
			} catch (Error& e) {
				if (e.code() == error_code_end_of_stream)
					break;
				else
					throw;
			}
		}

		while (!futures.empty()) {
			wait(futures.front().second);
			output.send(futures.front().first);
			futures.pop_front();
		}
		throw end_of_stream();
	} catch (Error& e) {
		output.sendError(e);
		throw;
	}
}

FutureStream<Reference<ScanReturnedContext>> FlushChangesPlan::execute(PlanCheckpoint* checkpoint,
                                                                       Reference<DocTransaction> tr) {
	PromiseStream<Reference<ScanReturnedContext>> docs;
	checkpoint->addOperation(doFlushChanges(subPlan->execute(checkpoint, tr), docs), docs);
	return docs.getFuture();
}

ACTOR static Future<Void> doOplogInsert(PlanCheckpoint* checkpoint,
										Reference<DocTransaction> tr,
										Reference<MetadataManager> mm,
										Namespace ns,
										std::list<bson::BSONObj>* docs,
										Reference<IOplogInserter> oplogInserter,
										FutureStream<Reference<ScanReturnedContext>> input,
										PromiseStream<Reference<ScanReturnedContext>> output) {
	state Deque<Future<Reference<IReadWriteContext>>> inserts;
	state FlowLock* flowControlLock = checkpoint->getDocumentFinishedLock();
	state Reference<Oplogger> oplogger = ref(new Oplogger(ns, oplogInserter));	
	state Deque<Future<Reference<IReadWriteContext>>> logs;

	try {
		try {		
			if (oplogger->isEnabled()) {
				state Reference<UnboundCollectionContext> ucx = wait(oplogger->getUnboundContext(mm, tr));
				state Reference<CollectionContext> ctx = ucx->bindCollectionContext(tr);

				// Oplog. Add inserts.
				for (const auto& d : *docs) {
					oplogger->addUpdatedDoc(d);
				}
				
				logs = oplogger->buildOplogs(ctx);
			}

			loop choose {
				when(state Reference<ScanReturnedContext> doc = waitNext(input)) {
					output.send(doc);
				}
				when(Reference<IReadWriteContext> _doc = wait(logs.empty() ? Never() : logs.front())) {
					// Oplog. Commit operations.
					wait(_doc->commitChanges());
					logs.pop_front();
				}
			}
		} catch (Error& e) {
			if (e.code() != error_code_end_of_stream)
				throw;
		}

		if (oplogger->isEnabled()) {
			// Oplog. Commit operations.
			while(!logs.empty()) {
				Reference<IReadWriteContext> _doc = wait(logs.front());
				wait(_doc->commitChanges());
				logs.pop_front();
			}
		}

		throw end_of_stream();
	} catch (Error& e) {
		if (e.code() != error_code_actor_cancelled)
			output.sendError(e);
		throw;
	}
}

ACTOR static Future<Void> doUpdate(PlanCheckpoint* checkpoint,
                                   Reference<DocTransaction> tr,
                                   FutureStream<Reference<ScanReturnedContext>> input,
                                   PromiseStream<Reference<ScanReturnedContext>> output,
                                   Reference<IUpdateOp> updateOp,
                                   Reference<IInsertOp> upsertOp,
                                   int64_t limit,
                                   Reference<UnboundCollectionContext> cx) {
	state int64_t& count = checkpoint->getIntState(0);
	state Deque<std::pair<Reference<ScanReturnedContext>, Future<Void>>> futures;
	state FlowLock* flowControlLock = checkpoint->getDocumentFinishedLock();

	try {
		try {
			loop choose {
				when(Reference<ScanReturnedContext> doc = waitNext(input)) {
					futures.push_back(std::make_pair(doc, updateOp->update(doc)));
					count += 1;
					if (count >= limit)
						break;
				}
				when(wait(futures.empty() ? Never() : futures.front().second)) {
					output.send(futures.front().first);
					futures.pop_front();
				}
			}
		} catch (Error& e) {
			if (e.code() != error_code_end_of_stream)
				throw;
		}

		while (!futures.empty()) {
			wait(futures.front().second);
			output.send(futures.front().first);
			futures.pop_front();
		}

		if (upsertOp && count == 0) {
			wait(flowControlLock->take());
			Reference<IReadWriteContext> inserted = wait(upsertOp->insert(cx->bindCollectionContext(tr)));
			//< Is this choice of scanId etc right?
			output.send(ref(new ScanReturnedContext(inserted, -1, FDB::Key())));
		}

		throw end_of_stream();
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled) {
			if (checkpoint->splitBoundWanted()) {
				for (int i = futures.size() - 1; i >= 0; i--)
					checkpoint->splitBound(futures[i].first->scanId()) = futures[i].first->scanKey();
			}
		} else
			output.sendError(e);
		throw;
	}
}

FutureStream<Reference<ScanReturnedContext>> OplogInsertPlan::execute(PlanCheckpoint* checkpoint,
																	  Reference<DocTransaction> tr) {
    PromiseStream<Reference<ScanReturnedContext>> output;

	checkpoint->addOperation(
		doOplogInsert(checkpoint, tr, mm, ns, docs, oplogInserter, subPlan->execute(checkpoint, tr), output), output);

	return output.getFuture();
}

FutureStream<Reference<ScanReturnedContext>> UpdatePlan::execute(PlanCheckpoint* checkpoint,
                                                                 Reference<DocTransaction> tr) {
	PromiseStream<Reference<ScanReturnedContext>> docs;
	checkpoint->addOperation(
	    doUpdate(checkpoint, tr, subPlan->execute(checkpoint, tr), docs, updateOp, upsertOp, limit, cx), docs);

	return docs.getFuture();
}

ACTOR static Future<Void> findAndModify(PlanCheckpoint* outerCheckpoint,
                                        Reference<DocTransaction> dtr,
                                        Reference<Plan> subPlan,
                                        Reference<MetadataManager> mm,
                                        Reference<Database> database,
                                        Reference<UnboundCollectionContext> cx,
										Reference<IOplogInserter> oplogInserter,
                                        Reference<IUpdateOp> updateOp,
                                        Reference<IInsertOp> upsertOp,
                                        Reference<Projection> projection,
                                        Optional<bson::BSONObj> ordering,
                                        bool projectNew,
                                        PromiseStream<Reference<ScanReturnedContext>> output) {
	if (!dtr)
		dtr = NonIsolatedPlan::newTransaction(database);
	state Reference<PlanCheckpoint> innerCheckpoint(new PlanCheckpoint);
	state int nTransactions = 1;
	state FlowLock* outerLock = outerCheckpoint->getDocumentFinishedLock();
	state Reference<ScanReturnedContext> firstDoc;
	state bool any = false;
	state bson::BSONObj proj;
	state Reference<Oplogger> oplogger = ref(new Oplogger(
		Namespace(cx->databaseName(), cx->collectionName()),
		oplogInserter
	));

	try {
		state uint64_t metadataVersion = wait(cx->bindCollectionContext(dtr)->getMetadataVersion());
		loop {
			state FutureStream<Reference<ScanReturnedContext>> docs = subPlan->execute(innerCheckpoint.getPtr(), dtr);
			state FlowLock* innerLock = innerCheckpoint->getDocumentFinishedLock();
			state Future<Void> timeout = delay(1.0);
			state bool done = false;

			try {
				loop choose {
					when(state Reference<ScanReturnedContext> doc = waitNext(docs)) {
						// throws end_of_stream when totally finished
						firstDoc = doc;
						innerLock->release();
						done = true;
						any = true;
						break;
					}
					when(wait(timeout)) { break; }
				}
			} catch (Error& e) {
				if (e.code() != error_code_end_of_stream)
					throw;
				done = true;
			}

			if (done)
				break;

			ASSERT(!docs.isReady());

			innerCheckpoint = innerCheckpoint->stopAndCheckpoint();

			dtr = NonIsolatedPlan::newTransaction(database);
			state uint64_t newMetadataVersion = wait(cx->bindCollectionContext(dtr)->getMetadataVersion());
			if (newMetadataVersion != metadataVersion) {
				Reference<UnboundCollectionContext> newCx = wait(mm->refreshUnboundCollectionContext(cx, dtr));
				if (newCx->collectionDirectory->key() != cx->collectionDirectory->key() ||
				    newCx->metadataDirectory->key() != cx->metadataDirectory->key())
					throw collection_metadata_changed();
				if (subPlan->wasMetadataChangeOkay(newCx)) {
					metadataVersion = newMetadataVersion;
				} else {
					throw metadata_changed_nonisolated();
				}
			}

			++nTransactions;
		}

		// From here on, everything takes place in a single transaction, which is also the same one in which we found
		// the document.

		innerCheckpoint->stop(); // cancel all ongoing work

		if (!projectNew && any) {
			bson::BSONObj project = wait(projectDocument(firstDoc, projection, ordering));
			proj = project;
		}

		if (any)
			wait(updateOp->update(firstDoc));
		else if (upsertOp) {
			Reference<IReadWriteContext> inserted = wait(upsertOp->insert(cx->bindCollectionContext(dtr)));
			firstDoc = ref(new ScanReturnedContext(inserted, -1, FDB::Key()));
		}

		if (any || upsertOp) {
			if (oplogger->isEnabled()) {
				// Oplog. Add original doc.
				DataValue dv = wait(firstDoc->toDataValue());
				oplogger->addOriginalDoc(&dv);
			}

			wait(firstDoc->commitChanges());

			if (oplogger->isEnabled()) {
				// Oplog. Add updated doc.
				DataValue dv = wait(firstDoc->toDataValue());
				oplogger->addUpdatedDoc(&dv);

				state Reference<UnboundCollectionContext> ucx = wait(oplogger->getUnboundContext(mm, dtr));
				state Reference<CollectionContext> opCtx = ucx->bindCollectionContext(dtr);

				state int i = 0;
				state Deque<Future<Reference<IReadWriteContext>>> logs = oplogger->buildOplogs(opCtx);
				// Oplog. Commit docs.
				for (; i < logs.size(); i++) {
					Reference<IReadWriteContext> _doc = wait(logs[i]);
					wait(_doc->commitChanges());
				}
			}			
		}

		if (projectNew && (any || upsertOp)) {
			bson::BSONObj project = wait(projectDocument(firstDoc, projection, ordering));
			proj = project;
		}

		wait(dtr->tr->commit());
		// Ideally we shouldn't do anything on this transaction anymore. But caller of this code would try to
		// read the updated documents with this transaction. There is no need to use same 'dtr' transaction
		// except that code is structured in a way makes it hard to use any other transaction.
		dtr->tr = NonIsolatedPlan::newTransaction(database)->tr;

		wait(outerLock->take());

		if (any || (projectNew && upsertOp))
			output.send(ref(
			    new ScanReturnedContext(ref(new BsonContext(proj, false)), firstDoc->scanId(), firstDoc->scanKey())));
		throw end_of_stream();
	} catch (Error& e) {
		DocumentLayer::metricReporter->captureHistogram(DocLayerConstants::MT_HIST_TR_PER_REQUEST, nTransactions);
		innerCheckpoint->stop();
		output.sendError(e);
		throw;
	}
}

FutureStream<Reference<ScanReturnedContext>> FindAndModifyPlan::execute(PlanCheckpoint* checkpoint,
                                                                        Reference<DocTransaction> tr) {
	PromiseStream<Reference<ScanReturnedContext>> docs;
	checkpoint->addOperation(findAndModify(checkpoint, tr, subPlan, mm, database, cx, 
										   oplogInserter, updateOp, upsertOp, projection,
	                                       ordering, projectNew, docs),
	                         docs);
	return docs.getFuture();
}

ACTOR static Future<Void> projectAndUpdate(PlanCheckpoint* checkpoint,
                                           Reference<DocTransaction> tr,
                                           FutureStream<Reference<ScanReturnedContext>> input,
                                           PromiseStream<Reference<ScanReturnedContext>> output,
                                           Reference<IUpdateOp> updateOp,
                                           Reference<IInsertOp> upsertOp,
                                           Reference<Projection> projection,
                                           Optional<bson::BSONObj> ordering,
                                           bool projectNew,
                                           Reference<UnboundCollectionContext> cx) {
	state FlowLock* flowControlLock = checkpoint->getDocumentFinishedLock();
	state Reference<ScanReturnedContext> firstDoc;
	state bson::BSONObj proj;
	state bool any;
	try {
		try {
			Reference<ScanReturnedContext> next = waitNext(input);
			firstDoc = next;
			any = true;
		} catch (Error& e) {
			if (e.code() == error_code_end_of_stream)
				any = false;
			else
				throw;
		}

		if (!projectNew && any) {
			bson::BSONObj project = wait(projectDocument(firstDoc, projection, ordering));
			proj = project;
		}

		if (any)
			wait(updateOp->update(firstDoc));
		else if (upsertOp) {
			wait(flowControlLock->take());
			Reference<IReadWriteContext> inserted = wait(upsertOp->insert(cx->bindCollectionContext(tr)));
			firstDoc = ref(new ScanReturnedContext(inserted, -1, FDB::Key()));
		}

		if (any || upsertOp) {
			wait(firstDoc->commitChanges());
		}

		if (projectNew && (any || upsertOp)) {
			bson::BSONObj project = wait(projectDocument(firstDoc, projection, ordering));
			proj = project;
		}

		if (any || (projectNew && upsertOp))
			output.send(ref(
			    new ScanReturnedContext(ref(new BsonContext(proj, false)), firstDoc->scanId(), firstDoc->scanKey())));
		throw end_of_stream();
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled) {
			if (checkpoint->splitBoundWanted()) {
				if (firstDoc && any)
					checkpoint->splitBound(firstDoc->scanId()) = firstDoc->scanKey();
			}
		} else
			output.sendError(e);
		throw;
	}
}

FutureStream<Reference<ScanReturnedContext>> ProjectAndUpdatePlan::execute(PlanCheckpoint* checkpoint,
                                                                           Reference<DocTransaction> tr) {
	PromiseStream<Reference<ScanReturnedContext>> docs;
	checkpoint->addOperation(projectAndUpdate(checkpoint, tr, subPlan->execute(checkpoint, tr), docs, updateOp,
	                                          upsertOp, projection, ordering, projectNew, cx),
	                         docs);
	return docs.getFuture();
}

ACTOR static Future<Void> doSkip(PlanCheckpoint* checkpoint,
                                 FutureStream<Reference<ScanReturnedContext>> input,
                                 PromiseStream<Reference<ScanReturnedContext>> output,
                                 int64_t skip) {
	state int64_t& leftToSkip = checkpoint->getIntState(skip);
	state FlowLock* flowControlLock = checkpoint->getDocumentFinishedLock();

	try {
		while (leftToSkip != 0) {
			Reference<ScanReturnedContext> next = waitNext(input);
			flowControlLock->release();
			--leftToSkip;
		}
		loop {
			Reference<ScanReturnedContext> next = waitNext(input);
			output.send(next);
		}
	} catch (Error& e) {
		if (e.code() != error_code_actor_cancelled)
			output.sendError(e);
		throw;
	}
}

FutureStream<Reference<ScanReturnedContext>> SkipPlan::execute(PlanCheckpoint* checkpoint,
                                                               Reference<DocTransaction> tr) {
	PromiseStream<Reference<ScanReturnedContext>> docs;
	checkpoint->addOperation(doSkip(checkpoint, subPlan->execute(checkpoint, tr), docs, skip), docs);
	return docs.getFuture();
}

ACTOR static Future<Void> doIndexInsert(PlanCheckpoint* checkpoint,
                                        Reference<DocTransaction> tr,
                                        Reference<IInsertOp> indexInsert,
                                        bson::BSONObj indexObj,
                                        Namespace ns,
                                        PromiseStream<Reference<ScanReturnedContext>> output,
                                        Reference<MetadataManager> mm) {
	state FlowLock* flowControlLock = checkpoint->getDocumentFinishedLock();
	try {
		wait(flowControlLock->take());
		state Reference<UnboundCollectionContext> mcx = wait(mm->getUnboundCollectionContext(tr, ns));
		state Reference<UnboundCollectionContext> unbound = wait(mm->indexesCollection(tr, ns.first));
		state Reference<Plan> getIndexesPlan = getIndexesForCollectionPlan(unbound, ns);
		try {
			std::vector<bson::BSONObj> indexObjs = wait(getIndexesTransactionally(getIndexesPlan, tr));
			for (const auto& existingindexObj : indexObjs) {
				if (indexObj.getObjectField(DocLayerConstants::KEY_FIELD)
				        .woCompare(existingindexObj.getObjectField(DocLayerConstants::KEY_FIELD)) == 0) {
					throw index_already_exists();
				}
				if (strcmp(indexObj.getStringField(DocLayerConstants::NAME_FIELD),
				           existingindexObj.getStringField(DocLayerConstants::NAME_FIELD)) == 0) {
					throw index_name_taken();
				}
			}
		} catch (Error& e) {
			// For some reason, in this case mongo tells the client that everything went okay. SOMEDAY evaluate whether
			// we want to handle this differently.
			if (e.code() == error_code_index_already_exists)
				throw end_of_stream();
			throw;
		}

		Reference<IReadWriteContext> doc = wait(indexInsert->insert(unbound->bindCollectionContext(tr)));
		mcx->bindCollectionContext(tr)->bumpMetadataVersion();
		TraceEvent(SevInfo, "BumpMetadataVersion")
		    .detail("reason", "createIndex")
		    .detail("ns", fullCollNameToString(ns))
		    .detail("index", indexObj.toString());
		output.send(ref(new ScanReturnedContext(doc, -1, Key())));
		throw end_of_stream();
	} catch (Error& e) {
		if (e.code() != error_code_actor_cancelled)
			output.sendError(e);
		throw;
	}
}

FutureStream<Reference<ScanReturnedContext>> IndexInsertPlan::execute(PlanCheckpoint* checkpoint,
                                                                      Reference<DocTransaction> tr) {
	PromiseStream<Reference<ScanReturnedContext>> docs;
	checkpoint->addOperation(doIndexInsert(checkpoint, tr, indexInsert, indexObj, ns, docs, mm), docs);
	return docs.getFuture();
}

ACTOR static Future<Void> doInsert(PlanCheckpoint* checkpoint,
                                   std::vector<Reference<IInsertOp>> docs,
                                   Reference<DocTransaction> tr,
                                   Reference<MetadataManager> mm,
                                   Namespace ns,
                                   PromiseStream<Reference<ScanReturnedContext>> output) {
	state Deque<Future<Reference<IReadWriteContext>>> f;
	state FlowLock* flowControlLock = checkpoint->getDocumentFinishedLock();
	state int i = 0;

	try {
		state Reference<UnboundCollectionContext> ucx = wait(mm->getUnboundCollectionContext(tr, ns));
		loop {
			if (i >= docs.size())
				break;
			choose {
				when(wait(flowControlLock->take())) {
					f.push_back(docs[i]->insert(ucx->bindCollectionContext(tr)));
					i++;
				}
				when(Reference<IReadWriteContext> doc = wait(f.empty() ? Never() : f.front())) {
					output.send(ref(new ScanReturnedContext(doc, -1, Key()))); // Are these the right scanId etc?
					f.pop_front();
				}
			}
		}
		state int j = 0;
		for (; j < f.size(); j++) {
			Reference<IReadWriteContext> doc = wait(f[j]);
			output.send(ref(new ScanReturnedContext(doc, -1, Key()))); // Are these the right scanId etc?
		}
		throw end_of_stream();
	} catch (Error& e) {
		if (e.code() != error_code_actor_cancelled)
			output.sendError(e);
		throw;
	}
}

FutureStream<Reference<ScanReturnedContext>> InsertPlan::execute(PlanCheckpoint* checkpoint,
                                                                 Reference<DocTransaction> tr) {
	PromiseStream<Reference<ScanReturnedContext>> output;
	checkpoint->addOperation(doInsert(checkpoint, docs, tr, mm, ns, output), output);
	return output.getFuture();
}

int bsonCompare(const bson::BSONObj& first, const bson::BSONObj& second, bson::Ordering o, bson::BSONObj orderObj) {
	bson::BSONObj newfirst = first.getObjectField("sortKey").extractFields(orderObj, true);
	bson::BSONObj newSecond = second.getObjectField("sortKey").extractFields(orderObj, true);
	return newfirst.woCompare(newSecond, o) < 0;
}

ACTOR static Future<Void> doSort(PlanCheckpoint* outerCheckpoint,
                                 Reference<DocTransaction> tr,
                                 Reference<Plan> subPlan,
                                 bson::BSONObj orderObj,
                                 PromiseStream<Reference<ScanReturnedContext>> output) {
	state std::vector<bson::BSONObj> returnProjections;
	state Reference<PlanCheckpoint> innerCheckpoint(new PlanCheckpoint);
	state FutureStream<Reference<ScanReturnedContext>> docs = subPlan->execute(innerCheckpoint.getPtr(), tr);
	state FlowLock* outerLock = outerCheckpoint->getDocumentFinishedLock();
	state FlowLock* innerLock = innerCheckpoint->getDocumentFinishedLock();
	loop {
		try {
			Reference<ScanReturnedContext> doc = waitNext(docs);
			// Note that this call to get() is safe here but not in general, because we know that doc is wrapping a
			// BsonContext, which means toDataValue() is synchronous.
			returnProjections.push_back(doc->toDataValue().get().getPackedObject().getOwned());
			innerLock->release();
		} catch (Error& e) {
			if (e.code() == error_code_end_of_stream) {
				break;
			}
			TraceEvent(SevError, "BD_doSort_fetching").error(e);
			throw;
		}
	}
	bson::BSONObj torderObj = orderObj;
	bson::Ordering o = bson::Ordering::make(torderObj);
	std::sort(returnProjections.begin(), returnProjections.end(),
	          [o, torderObj](const bson::BSONObj& first, const bson::BSONObj& second) {
		          return bsonCompare(first, second, o, torderObj);
	          });

	state int i = 0;
	try {
		for (; i < returnProjections.size(); ++i) {
			wait(outerLock->take());
			output.send(ref(new ScanReturnedContext(
			    ref(new BsonContext(returnProjections[i].getObjectField("doc").getOwned(), false)), -1, Key())));
		}
	} catch (Error& e) {
		TraceEvent(SevError, "BD_doSort_replying").error(e);
		throw;
	}
	innerCheckpoint->stop();
	output.sendError(end_of_stream());
	return Void();
}

FutureStream<Reference<ScanReturnedContext>> SortPlan::execute(PlanCheckpoint* checkpoint,
                                                               Reference<DocTransaction> tr) {
	PromiseStream<Reference<ScanReturnedContext>> output;
	checkpoint->addOperation(doSort(checkpoint, tr, subPlan, orderObj, output), output);
	return output.getFuture();
}

ACTOR static Future<Void> updateIndexStatus(PlanCheckpoint* checkpoint,
                                            Reference<DocTransaction> tr,
                                            Namespace ns,
                                            Standalone<StringRef> encodedIndexId,
                                            Reference<MetadataManager> mm,
                                            std::string newStatus,
                                            UID buildId,
                                            PromiseStream<Reference<ScanReturnedContext>> output) {
	state bool okay;
	state FlowLock* flowControlLock = checkpoint->getDocumentFinishedLock();

	try {
		state Reference<UnboundCollectionContext> indexCollection = wait(mm->indexesCollection(tr, ns.first));
		state Reference<QueryContext> indexDoc =
		    indexCollection->bindCollectionContext(tr)->cx->getSubContext(encodedIndexId);
		Reference<UnboundCollectionContext> ucx = wait(mm->getUnboundCollectionContext(tr, ns));
		state Reference<CollectionContext> mcx = ucx->bindCollectionContext(tr);
		Optional<DataValue> dv =
		    wait(indexDoc->get(DataValue(DocLayerConstants::BUILD_ID_FIELD, DVTypeCode::STRING).encode_key_part()));
		if (dv.present()) {
			UID currId = UID::fromString(dv.get().getString());
			if (currId == buildId) {
				okay = true;
			} else {
				TraceEvent(SevError, "MismatchedIndexBuildID")
				    .detail("indexDoc", indexDoc->toDbgString())
				    .detail("newStatus", newStatus);
				okay = false;
			}
		} else {
			TraceEvent(SevError, "MissingIndexBuildID")
			    .detail("indexDoc", indexDoc->toDbgString())
			    .detail("newStatus", newStatus);
			okay = false;
		}

		if (okay) {
			wait(flowControlLock->take());
			indexDoc->set(DataValue(DocLayerConstants::STATUS_FIELD, DVTypeCode::STRING).encode_key_part(),
			              DataValue(newStatus, DVTypeCode::STRING).encode_value());
			indexDoc->clear(
			    DataValue(DocLayerConstants::CURRENTLY_PROCESSING_DOC_FIELD, DVTypeCode::STRING).encode_key_part());
			indexDoc->clear(DataValue(DocLayerConstants::BUILD_ID_FIELD, DVTypeCode::STRING).encode_key_part());
			mcx->bumpMetadataVersion();
			TraceEvent(SevInfo, "BumpMetadataVersion")
			    .detail("reason", "updateIndexStatus")
			    .detail("ns", fullCollNameToString(ns))
			    .detail("indexID", printable(encodedIndexId))
			    .detail("status", newStatus);
			output.send(ref(new ScanReturnedContext(indexDoc, -1, Key())));
			throw end_of_stream();
		} else {
			throw index_wrong_build_id();
		}
	} catch (Error& e) {
		output.sendError(e);
		throw;
	}
}

FutureStream<Reference<ScanReturnedContext>> UpdateIndexStatusPlan::execute(PlanCheckpoint* checkpoint,
                                                                            Reference<DocTransaction> tr) {
	PromiseStream<Reference<ScanReturnedContext>> output;
	checkpoint->addOperation(updateIndexStatus(checkpoint, tr, ns, encodedIndexId, mm, newStatus, buildId, output),
	                         output);
	return output.getFuture();
}

ACTOR static Future<Void> buildIndexEntry(Reference<ScanReturnedContext> doc, Reference<IndexInfo> index) {
	// This is sufficient even for compound indexes, because we have one index entry per document, so
	// dirtying one of the indexed fields causes the plugin to rewrite the entry.
	state Standalone<StringRef> index_key = encodeMaybeDotted(index->indexKeys[0].first);
	Optional<DataValue> odv = wait(doc->get(index_key));

	// Don't need to worry about objects or arrays, because even if
	// we just set the header, the plugin stack is going to
	// re-evaluate the expression and do everything it needs to do.
	if (odv.present())
		doc->set(index_key, odv.get().encode_value());
	else
		doc->clear(index_key);

	return Void();
}

// What follows is evidence that we do not live in the best of all possible worlds
std::string unstrincObjectId(StringRef encodedKeyPart) {
	bool reduceLength = false;
	bool shouldBeNullTerminated = true;
	switch (DVTypeCode(encodedKeyPart[0])) {
	case DVTypeCode::NUMBER:
		shouldBeNullTerminated = false;
		if (encodedKeyPart.size() == 12)
			reduceLength = true;
		break;
	case DVTypeCode::OID:
		shouldBeNullTerminated = false;
		if (encodedKeyPart.size() == 14)
			reduceLength = true;
		break;
	case DVTypeCode::DATE:
		shouldBeNullTerminated = false;
		if (encodedKeyPart.size() == 10)
			reduceLength = true;
		break;
	default:
		throw unsupported_operation();
	}

	if (reduceLength) {
		return std::string((const char*)encodedKeyPart.begin(), encodedKeyPart.size() - 1);
	} else {
		std::string ret((const char*)encodedKeyPart.begin(), encodedKeyPart.size());
		if (!shouldBeNullTerminated || (uint8_t)ret[encodedKeyPart.size() - 1] == 1)
			ret[encodedKeyPart.size() - 1]--;
		return ret;
	}
}

ACTOR static Future<Void> scanAndBuildIndex(PlanCheckpoint* checkpoint,
                                            Reference<DocTransaction> tr,
                                            Reference<IndexInfo> index,
                                            std::string dbName,
                                            Standalone<StringRef> encodedIndexId,
                                            Reference<MetadataManager> mm,
                                            FutureStream<Reference<ScanReturnedContext>> input,
                                            PromiseStream<Reference<ScanReturnedContext>> output) {
	state Deque<std::pair<Reference<ScanReturnedContext>, Future<Void>>> futures;
	// Only unique index needs a lock to do the build.
	try {
		if (checkpoint->getBounds(0).begin.size()) {
			Reference<UnboundCollectionContext> indexCollection = wait(mm->indexesCollection(tr, dbName));
			state Reference<QueryContext> indexDoc =
			    indexCollection->bindCollectionContext(tr)->cx->getSubContext(encodedIndexId);
			try {
				std::string encodedId = unstrincObjectId(checkpoint->getBounds(0).begin);
				indexDoc->set(
				    DataValue(DocLayerConstants::CURRENTLY_PROCESSING_DOC_FIELD, DVTypeCode::STRING).encode_key_part(),
				    DataValue::decode_key_part(DataKey::decode_item(StringRef(encodedId), 0)).encode_value());
			} catch (Error& e) {
				indexDoc->set(
				    DataValue(DocLayerConstants::CURRENTLY_PROCESSING_DOC_FIELD, DVTypeCode::STRING).encode_key_part(),
				    DataValue("unknown", DVTypeCode::STRING).encode_value());
			}
			wait(indexDoc->commitChanges());
		}
		state int nrDocs = 0;
		try {
			loop choose {
				when(state Reference<ScanReturnedContext> doc = waitNext(input)) {
					futures.push_back(std::make_pair(doc, buildIndexEntry(doc, index)));
					nrDocs++;
				}
				when(wait(futures.empty() ? Never() : futures.front().second)) {
					output.send(futures.front().first);
					futures.pop_front();
				}
				when(wait(delay(0.1))) {
					DocumentLayer::metricReporter->captureMeter(DocLayerConstants::MT_RATE_IDX_REBUILD, nrDocs);
					nrDocs = 0;
				}
			}
		} catch (Error& e) {
			if (e.code() != error_code_end_of_stream)
				throw;
		}

		if (nrDocs) {
			DocumentLayer::metricReporter->captureMeter(DocLayerConstants::MT_RATE_IDX_REBUILD, nrDocs);
		}

		while (!futures.empty()) {
			wait(futures.front().second);
			output.send(futures.front().first);
			futures.pop_front();
		}

		throw end_of_stream();
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled) {
			if (checkpoint->splitBoundWanted()) {
				if (input.isReady()) {
					Deque<Reference<ScanReturnedContext>> leftInStream;
					while (input.isReady() && !input.isError()) {
						leftInStream.push_back(input.pop());
					}
					for (int i = leftInStream.size() - 1; i >= 0; i--)
						checkpoint->splitBound(leftInStream[i]->scanId()) = leftInStream[i]->scanKey();
				}
				for (int i = futures.size() - 1; i >= 0; i--)
					checkpoint->splitBound(futures[i].first->scanId()) = futures[i].first->scanKey();
			}
		} else
			output.sendError(e);
		throw;
	}
}

FutureStream<Reference<ScanReturnedContext>> BuildIndexPlan::execute(PlanCheckpoint* checkpoint,
                                                                     Reference<DocTransaction> tr) {
	PromiseStream<Reference<ScanReturnedContext>> p;
	checkpoint->addOperation(
	    scanAndBuildIndex(checkpoint, tr, index, dbName, encodedIndexId, mm, scan->execute(checkpoint, tr), p), p);
	return p.getFuture();
}

bool BuildIndexPlan::wasMetadataChangeOkay(Reference<UnboundCollectionContext> newCx) {
	for (const Reference<IndexInfo>& i : newCx->knownIndexes) {
		if (i->indexName == index->indexName && i->status == IndexInfo::IndexStatus::BUILDING &&
		    i->buildId.get() == index->buildId.get())
			return scan->wasMetadataChangeOkay(newCx);
	}
	return false;
}

ACTOR Future<std::pair<int64_t, Reference<ScanReturnedContext>>> executeUntilCompletionAndReturnLastTransactionally(
    Reference<Plan> plan,
    Reference<DocTransaction> tr) {
	state int64_t count = 0;
	state Reference<PlanCheckpoint> checkpoint(new PlanCheckpoint);
	state FutureStream<Reference<ScanReturnedContext>> stream = plan->execute(checkpoint.getPtr(), tr);
	state FlowLock* flowControlLock = checkpoint->getDocumentFinishedLock();
	state Reference<ScanReturnedContext> last;

	try {
		loop {
			Reference<ScanReturnedContext> next = waitNext(stream);
			last = std::move(next);
			flowControlLock->release();
			count++;
		}
	} catch (Error& e) {
		checkpoint->stop();
		if (e.code() != error_code_end_of_stream)
			throw e;
	}

	return std::make_pair(count, last);
}

ACTOR Future<int64_t> executeUntilCompletionTransactionally(Reference<Plan> plan, Reference<DocTransaction> tr) {
	state int64_t count = 0;
	state Reference<PlanCheckpoint> checkpoint(new PlanCheckpoint);
	state FutureStream<Reference<ScanReturnedContext>> stream = plan->execute(checkpoint.getPtr(), tr);
	state FlowLock* flowControlLock = checkpoint->getDocumentFinishedLock();

	try {
		loop {
			state Reference<ScanReturnedContext> next = waitNext(stream);
			flowControlLock->release();
			count++;
		}
	} catch (Error& e) {
		checkpoint->stop();
		if (e.code() != error_code_end_of_stream)
			throw e;
	}

	return count;
}

Reference<Plan> deletePlan(Reference<Plan> subPlan, Reference<UnboundCollectionContext> cx, int64_t limit) {
	return Reference<Plan>(
	    new UpdatePlan(subPlan, Reference<IUpdateOp>(new DeleteDocument()), Reference<IInsertOp>(), limit, cx));
}

Reference<Plan> oplogInsertPlan(Reference<Plan> subPlan, 
								std::list<bson::BSONObj>* docs,
								Reference<IOplogInserter> oplogInserter,
								Reference<MetadataManager> mm,
								Namespace ns) {	
	return Reference<Plan>(new OplogInsertPlan(subPlan, docs, oplogInserter, mm, ns));
}

Reference<Plan> flushChanges(Reference<Plan> subPlan) {
	return Reference<Plan>(new FlushChangesPlan(subPlan));
}

// ******** PlanCheckpoint implementation **************

PlanCheckpoint::PlanCheckpoint()
    : boundsWanted(false), scansAdded(0), stateAdded(0), flowControlLock(DOCLAYER_KNOBS->FLOW_CONTROL_LOCK_PERMITS) {}

/*
  Overview
    stopAndCheckpoint, in order to return a new checkpoint which picks up where the execution of this
    checkpoint stops, needs to determine a split key for each "scan" in the plan.  The appropriate split
    key is greater than the scan key of any document that has been output by the plan (to avoid repeats),
    less than or equal to the next document that the plan would have output (to avoid missing documents),
    and within that range should be as large as possible (to avoid repeating work).

    A "scan" is a plan actor which has no input document stream, but outputs a document stream (presumably
    based on the database contents).

    Each scan is assigned a scanID by calling addScan() in a consistent order from within Plan::execute().
    The scanID is used both to read the scan's bounds and to update them.

    Plan actors, including scan actors, are added to the `PlanCheckpoint::ops` vector in a topological sort
    by calls to PlanCheckpoint::addOperation().  These calls are naturally in a topological sort (inputs before
    outputs), since it is necessary to construct an actor's inputs by calling Plan::execute() on subplans
    before constructing the actor, and to construct the actor before passing its return future to
    PlanCheckpoint::addOperation().  But if a single Plan generates multiple operations, it must call
    addOperation() for each of them in topological sort order.

    stopAndCheckpoint() calls stop() with the `boundsWanted` flag set to true.  stop() cancels all plan actors
    that have not already terminated in the order of the `ops` vector, and therefore in a topological sort order.

    All plan actors are required to always wait on their input streams.  If an actor waits on something else without
    simultaneously waiting on its input stream via choose, then documents could be "stored" in the PromiseStreams
    and would not currently be discovered by stopAndCheckpoint, resulting in incorrect plan bounds.  (An implementation
    of PlanCheckpoint which *did* discover such documents should be possible, since the PromiseStreams are passed
    to addOperation)

    Each plan actor which operates on document asynchronously, when cancelled, checks the `boundsWanted` flag on
    the checkpoint, and if it is true, sets the bounds for each scan for which it has a document "outstanding"
    (i.e. a document which it has received from its input stream but not sent to its output stream) to the scan
    key of the first outstanding document with that scanID.  This is most easily done by iterating over the
    outstanding documents in the reverse order that they would be output, setting the split key for the document's
    scan ID to the document's scan key.  The scan ID and scan key are available to it through the IScanReturnedContext
    interface.

    A scan actor, when cancelled, also checks the `boundsWanted` flag and sets the split key for its scan key to
    some key after the last document it has output, and less than or equal to the first document it has not output.
    Scan actors also are responsible for associating each document they output with a monotonically increasing scan
    key less than "\xff".

    A "synchronous" plan actor which always outputs a document as soon as it receives it has no responsibilities
    with respect to bounds calculation.

    The split key for each scan at the end of this process is determined by the last actor in the topological sort
    which sets it.  Everything that has been output by this actor has either been discarded by a later actor or
    output from the scan, so the next document it outputs with any given scanID is the earliest possible document
    with that scanID that could possibly be output by the scan as a whole.  The above rules ensure that the actor
    sets the split key for the given scanID to be <= the scan key of the next such document, and therefore it is also
    <= the scan key of the next document that would be output.  Similarly, the scan key is > that of the last document
    output with that scanID because the scan keys are monotonically increasing.

    If no actor sets the split key for a scanID, it can only be because the scan actor itself has terminated (completed
    the entire scan).  In this case, the split key will have its default value of "\xff", which is defined to be greater
    than any document's scan key.  So when restarted the scan will not output any documents.

    Because this mechanism relies on cancellation of actors, it won't work as expected if any of the plan actors are
    on the call stack when stopAndCheckpoint() is called.  Callers are therefore responsible for making sure they have
    a clean call stack in this respect before calling stopAndCheckpoint().  (A delay(0) prior to the call will work in
    a pinch).
*/
Reference<PlanCheckpoint> PlanCheckpoint::stopAndCheckpoint() {
	boundsWanted = true;
	stop();
	boundsWanted = false;

	Reference<PlanCheckpoint> rest(new PlanCheckpoint);
	rest->scans.resize(scans.size());
	for (int i = 0; i < scans.size(); i++)
		rest->scans[i].bounds = KeyRangeRef(scans[i].split, scans[i].bounds.end);
	rest->states.reserve(states.size());
	for (auto& s : states)
		rest->states.emplace_back(s.split);

	return rest;
}

ACTOR void uncancellableHoldActor(Future<Void> held) {
	wait(held);
}

void PlanCheckpoint::stop() {
	// Cancel the operations in a topological sort
	// Cancellation exception handlers, if `splitBoundWanted()`, call `splitBound()` and modify `scans[?].split`
	for (auto& op : ops)
		op.actors.cancel();
	// Operations don't send errors to their outputs when cancelled, because those could cause subsequent actors
	// to die out of topological order.  So we would send broken_promise errors when we clear `ops` below.  Send
	// operation_cancelled to the final output instead (e.g. these are expected by NonIsolatedRW)
	if (!ops.empty())
		ops.back().output.sendError(operation_cancelled());
	ops.clear();
	scansAdded = 0;
	stateAdded = 0;
	// Make sure this PlanCheckpoint is not destroyed until all of the scans that it owned are off the stack.
	uncancellableHoldActor(holdWhile(Reference<PlanCheckpoint>::addRef(this), delay(0.0)));
}

void PlanCheckpoint::addOperation(Future<Void> actors, PromiseStream<Reference<ScanReturnedContext>> output) {
	ops.emplace_back(actors, output);
}

int PlanCheckpoint::addScan() {
	int s = scansAdded++;
	if (s >= scans.size())
		scans.resize(s + 1);
	return s;
}

FDB::KeyRange PlanCheckpoint::getBounds(int whichScan) {
	if (whichScan >= scans.size())
		return KeyRangeRef(StringRef(), LiteralStringRef("\xff")); //< Necessary?
	return scans[whichScan].bounds;
}

FDB::Key& PlanCheckpoint::splitBound(int whichScan) {
	ASSERT(whichScan >= 0 && whichScan < scans.size());
	return scans[whichScan].split;
}

bool PlanCheckpoint::splitBoundWanted() {
	return boundsWanted;
}

int64_t& PlanCheckpoint::getIntState(int64_t defaultValue) {
	int s = stateAdded++;
	if (s == states.size())
		states.emplace_back(defaultValue);
	states.back().split = states.back().begin;
	return states.back().split;
}

std::string PlanCheckpoint::toString() {
	std::string s;
	s.append(format("scans: %d  states: %d\n", scans.size(), states.size()));
	for (int i = 0; i < scans.size(); ++i) {
		s.append(format("\t scan %d begin: %s\n", i, printable(scans[i].bounds.begin).c_str()));
		s.append(format("\t scan %d split: %s\n", i, printable(scans[i].split).c_str()));
		s.append(format("\t scan %d end:   %s\n", i, printable(scans[i].bounds.end).c_str()));
	}
	return s;
}
