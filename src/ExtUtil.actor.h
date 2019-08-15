/*
 * ExtUtil.actor.h
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

// When actually compiled (NO_INTELLISENSE), include the generated version of this file.  In intellisense use the source
// version.
#if defined(NO_INTELLISENSE) && !defined(_EXT_UTIL_ACTOR_G_H_)
#define _EXT_UTIL_ACTOR_G_H_
#include "ExtUtil.actor.g.h"
#elif !defined(_EXT_UTIL_ACTOR_H_)
#define _EXT_UTIL_ACTOR_H_

#include "ExtStructs.h"
#include "QLContext.h"
#include "QLPredicate.h"
#include "flow/actorcompiler.h" // This must be the last #include.
#include <string>

#define QLOG(...)                                                                                                      \
	{ fprintf(stderr, __VA_ARGS__); }

template <class T>
Reference<T> ref(T* newVal) {
	return Reference<T>(newVal);
}

struct IdInfo {
	Standalone<StringRef> keyEncoded;
	Standalone<StringRef> valueEncoded;
	Optional<bson::BSONObj> objValue;

	IdInfo(Standalone<StringRef> keyEncoded, Standalone<StringRef> valueEncoded, Optional<bson::BSONObj> objValue)
	    : keyEncoded(keyEncoded), valueEncoded(valueEncoded), objValue(objValue) {}
};

/**
 * Depth-first recursive function for finding the first instance of an element with an operator fieldname within a BSON
 * object.
 *
 * Throws an exception unless every fieldname is an operator at the specified depth
 * (ensureEveryOrNoFieldnameAtThisDepthIsAnOperator). (Set ensureEveryOrNoFieldnameAtThisDepthIsAnOperator to -1 to
 * disregard this requirement.) The most common case just makes use of the defaults, e.g.
 * hasOperatorFieldnames(msg->update).
 *
 * @return 	0 	if no operator was found.
 * 		   +ve  representing the 1-indexed depth at which the first
 * 				operator was found.
 */
int hasOperatorFieldnames(bson::BSONObj obj,
                          int maxDepth = -1,
                          int ensureEveryFieldnameBeyondThisDepthIsAnOperator = -1,
                          int currentDepth = 0);

template <class T>
bool isin(std::list<T>, T);

bson::BSONObj transformOperatorQueryToUpdatableDocument(bson::BSONObj selector,
                                                        std::string parentKey = "",
                                                        int depth = 0);

std::string upOneLevel(std::string maybeDottedFieldName);
std::string getLastPart(std::string maybeDottedFieldName);
Key encodeMaybeDotted(std::string fieldname);

/**
 * The usual way of inserting an element
 */
int insertElementRecursive(const bson::BSONElement& elem, Reference<IReadWriteContext> cx);

/**
 * An overload that is used only in bizarre cases involving upserting things with compound ids.
 */
int insertElementRecursive(std::string fn, bson::BSONObj const& obj, Reference<IReadWriteContext> cx);

/**
 * Utility overloads used by some of the array update operators
 */
int insertElementRecursive(int fn, bson::BSONElement const& elem, Reference<IReadWriteContext> cx);
int insertElementRecursive(int fn, bson::BSONObj const& obj, Reference<IReadWriteContext> cx);
int insertElementRecursive(int fn, bson::BSONArray const& arr, Reference<IReadWriteContext> cx);

ACTOR Future<Void> ensureValidObject(Reference<IReadWriteContext> cx,
                                     std::string objectRoot,
                                     std::string objectSubfield,
                                     bool createRoot);

/**
 * Returns the length if it is an array, -1 if a non-array value, and -2 if the field is not set
 */
ACTOR Future<int> isArray(Reference<IReadWriteContext> docCx, std::string arrayRoot);

/**
 * Places the "top-level" elements of the array stored in `document` under `arrayField` into `p`.
 */
ACTOR Future<Void> getArrayStream(Reference<IReadWriteContext> document,
                                  Standalone<StringRef> arrayField,
                                  PromiseStream<DataValue> p);

bool is_literal_match(bson::BSONObj const& obj);
Reference<IPredicate> eq_predicate(const bson::BSONElement& el, const std::string& prefix);
Reference<IPredicate> any_predicate(std::string unencoded_path, Reference<IPredicate> pred, bool inElemMatch = false);

Reference<IPredicate> re_predicate(const bson::BSONElement& el, const std::string& prefix);

/**
 * Return a BSONObj with the same elements as `obj`, with fields ordered by fieldnames,
 * that owns its own memory
 */
bson::BSONObj sortBsonObj(bson::BSONObj obj);

/**
 * Return a difference between bson objects `original` and `updated`
 */
bson::BSONObj getUpdatedObjectsDifference(bson::BSONObj original, bson::BSONObj updated, bool isSet = true);

/**
 * Return a pair of key-encoded and value-encoded strings of whatever was in the _id
 * field of `obj`, plus the value itself if it was a bson object.
 */
Optional<IdInfo> extractEncodedIds(bson::BSONObj obj);

/**
 * As above, except looks within $set and $setOnInsert operator expressions in `update`.
 */
Optional<IdInfo> extractEncodedIdsFromUpdate(bson::BSONObj update);

Future<Reference<Plan>> getIndexesForCollectionPlan(Namespace const& ns,
                                                    Reference<DocTransaction> tr,
                                                    Reference<MetadataManager> mm);
Reference<Plan> getIndexesForCollectionPlan(Reference<UnboundCollectionContext> indexesCollection, Namespace const& ns);
ACTOR Future<std::vector<bson::BSONObj>> getIndexesTransactionally(Reference<Plan> indexMetadataPlan,
                                                                   Reference<DocTransaction> tr);

/**
 * Utility overload of mapAsync that returns the mapped stream to you
 */
template <class T, class F, class U = decltype(fake<F>()(fake<T>()).getValue())>
static GenFutureStream<U> mapAsync(GenFutureStream<T> const& input, F const& actorFunc) {
	PromiseStream<U> ps;
	GenFutureStream<U> result = ps.getFuture();
	result.actor = mapAsync2(input, actorFunc, ps);
	return result;
}

/**
 * Consume an entire stream, and return a vector of the results
 */
ACTOR template <class T>
static Future<std::vector<T>> consumeAll(GenFutureStream<T> stream) {
	state std::vector<T> results;
	try {
		loop {
			T t = waitNext(stream);
			results.push_back(t);
		}
	} catch (Error& e) {
		if (e.code() == error_code_end_of_stream)
			return results;
		throw;
	}
}

/**
 * Maps a stream with an asynchronous function
 */
ACTOR template <class T, class F, class U>
Future<Void> mapAsync2(GenFutureStream<T> input, F actorFunc, PromiseStream<U> output) {
	state Deque<Future<U>> futures;

	loop {
		try {
			choose {
				when(T nextInput = waitNext(input)) { futures.push_back(actorFunc(nextInput)); }
				when(U nextOutput = wait(futures.size() == 0 ? Never() : futures.front())) {
					output.send(nextOutput);
					futures.pop_front();
				}
			}
		} catch (Error& e) {
			if (e.code() == error_code_end_of_stream) {
				break;
			} else {
				output.sendError(e);
				throw;
			}
		}
	}

	while (futures.size()) {
		try {
			U nextOutput = wait(futures.front());
			output.send(nextOutput);
			futures.pop_front();
		} catch (Error& e) {
			output.sendError(e);
			throw;
		}
	}

	output.sendError(end_of_stream());

	return Void();
}

ACTOR template <class Function>
Future<decltype(fake<Function>()(Reference<DocTransaction>()).getValue())>
runRYWTransaction(Reference<FDB::Database> cx, Function func, int64_t retryLimit, int64_t timeout) {

	state Reference<FDB::Transaction> tr = cx->createTransaction();
	try {
		loop {
			try {
				tr->setOption(FDB_TR_OPTION_CAUSAL_READ_RISKY);
				tr->setOption(FDB_TR_OPTION_RETRY_LIMIT, StringRef((uint8_t*)&(retryLimit), sizeof(int64_t)));
				tr->setOption(FDB_TR_OPTION_TIMEOUT, StringRef((uint8_t*)&(timeout), sizeof(int64_t)));
				state Reference<DocTransaction> dtr = DocTransaction::create(tr);
				state decltype(fake<Function>()(Reference<DocTransaction>()).getValue()) result = wait(func(dtr));

				// Even though all references to actors called by func() have been dropped, some of them may still be on
				// the physical call stack, which means that they could continue to run even after actor_cancel has been
				// set. This can cause a used_during_commit() in some rare cases, especially if
				// FDBPlugin_getDescendants() is involved, since it may be in a state where it issues another getRange()
				// before its next wait(). The delay(0,0) which we wait on here gives us a new call stack, and prevents
				// this (rare but real) problem.
				wait(delay(0.0));

				wait(tr->commit());

				return result;
			}

			catch (Error& e) {
				if (e.code() == error_code_commit_unknown_result)
					throw;
				wait(tr->onError(e));
			}
		}
	} catch (Error& e) {
		throw;
	}
}

ACTOR template <class Function>
Future<Future<decltype(fake<Function>()(Reference<DocTransaction>()).getValue())>>
runTransactionAsync(Reference<FDB::Database> cx, Function func, int64_t retryLimit, int64_t timeout) {

	state Reference<FDB::Transaction> tr = cx->createTransaction();
	try {
		loop {
			try {
				tr->setOption(FDB_TR_OPTION_CAUSAL_READ_RISKY);
				tr->setOption(FDB_TR_OPTION_RETRY_LIMIT, StringRef((uint8_t*)&(retryLimit), sizeof(int64_t)));
				tr->setOption(FDB_TR_OPTION_TIMEOUT, StringRef((uint8_t*)&(timeout), sizeof(int64_t)));
				state Reference<DocTransaction> dtr = DocTransaction::create(tr);
				state Future<decltype(fake<Function>()(Reference<DocTransaction>()).getValue())> result = func(dtr);

				// Even though all references to actors called by func() have been dropped, some of them may still be on
				// the physical call stack, which means that they could continue to run even after actor_cancel has been
				// set. This can cause a used_during_commit() in some rare cases, especially if
				// FDBPlugin_getDescendants() is involved, since it may be in a state where it issues another getRange()
				// before its next wait(). The delay(0,0) which we wait on here gives us a new call stack, and prevents
				// this (rare but real) problem.
				wait(delay(0.0));

				wait(tr->commit());

				return result;
			}

			catch (Error& e) {
				if (e.code() == error_code_commit_unknown_result)
					throw;
				wait(tr->onError(e));
			}
		}
	} catch (Error& e) {
		throw;
	}
}

inline bool startsWith(const char* str, const char* pre) {
	size_t lenpre = strlen(pre), lenstr = strlen(str);
	return lenstr < lenpre ? false : strncmp(pre, str, lenpre) == 0;
}

#endif /* _EXT_UTIL_ACTOR_H_ */
