/*
 * ExtStructs.actor.cpp
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

#include "ExtStructs.h"
#include "QLPlan.h"

Reference<DocTransaction> ExtConnection::getOperationTransaction() {
	return NonIsolatedPlan::newTransaction(docLayer->database);
}

Reference<Plan> ExtConnection::wrapOperationPlan(Reference<Plan> plan,
                                                 bool isReadOnly,
                                                 Reference<UnboundCollectionContext> cx) {
	return Reference<Plan>(new NonIsolatedPlan(plan, isReadOnly, cx, docLayer->database, mm));
}

Reference<Plan> ExtConnection::isolatedWrapOperationPlan(Reference<Plan> plan) {
	return isolatedWrapOperationPlan(plan, options.timeoutMillies, options.retryLimit);
}

Reference<Plan> ExtConnection::isolatedWrapOperationPlan(Reference<Plan> plan, int64_t timeout, int64_t retryLimit) {
	return Reference<Plan>(new RetryPlan(plan, timeout, retryLimit, docLayer->database));
}

ACTOR Future<Void> housekeeping_impl(Reference<ExtConnection> ec) {
	loop {
		Void _ = wait(delay(DOCLAYER_KNOBS->CURSOR_EXPIRY));
		try {
			Cursor::prune(ec->cursors);
		} catch (Error& e) {
			TraceEvent(SevError, "BD_Cursor_housekeeping").error(e);
		}
	}
}

void ExtConnection::startHousekeeping() {
	housekeeping = housekeeping_impl(Reference<ExtConnection>::addRef(this));
}

ACTOR Future<WriteResult> lastErrorOrLastResult(Future<WriteResult> previous,
                                                Future<WriteResult> next,
                                                FlowLock* lock,
                                                int releasePermits) {
	try {
		state WriteResult next_wr = wait(next); // might throw
		WriteResult _ = wait(previous); // might throw
		lock->release(releasePermits);
		return next_wr;
	} catch (...) {
		lock->release(releasePermits);
		throw;
	}
}

/**
 * beforeWrite's future is set when the current write may *begin* while satisfying all ordering and pipelining
 * constraints The return value has no impact on when *subsequent* writes may be consumed from the network and/or
 * started; that is controlled by afterWrite().  But this code stashes some information which the next call to
 * afterWrite() uses.
 */
Future<Void> ExtConnection::beforeWrite(int desiredPermits) {
	if (options.pipelineCompatMode)
		return ready(lastWrite);
	// printf("beforeWrite: lock %d, requested %d\n", lock.activePermits(), desiredPermits);
	currentWriteLocked =
	    lock->take(TaskDefaultYield, std::min(desiredPermits, DOCLAYER_KNOBS->CONNECTION_MAX_PIPELINE_DEPTH / 2));
	return currentWriteLocked;
}

/**
 * afterWrite()'s return future is set when the next write may be consumed from the network (and beforeWrite()
 * called).
 */
Future<Void> ExtConnection::afterWrite(Future<WriteResult> writeResult, int releasePermits) {
	if (options.pipelineCompatMode) {
		lastWrite = writeResult;
		return ready(lastWrite);
	} else {
		lastWrite = lastErrorOrLastResult(lastWrite, writeResult, lock.getPtr(),
		                                  std::min(releasePermits, DOCLAYER_KNOBS->CONNECTION_MAX_PIPELINE_DEPTH / 2));
		return currentWriteLocked;
	}
}
