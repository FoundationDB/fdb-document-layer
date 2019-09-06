/*
 * BufferedConnection.h
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

#ifndef _BUFFERED_CONNECTION_H_
#define _BUFFERED_CONNECTION_H_

#pragma once

#include "flow/flow.h"
#include "flow/network.h"

struct BufferedConnection : ReferenceCounted<BufferedConnection> {
	explicit BufferedConnection(Reference<IConnection> connection);
	~BufferedConnection();

	/**
	 * Returns when count bytes are buffered and available to peek (or may
	 * throw an error if the connection dies).
	 */
	Future<Void> onBytesAvailable(int count);

	/**
	 * Returns exactly count bytes. The returned memor
	 * y is guaranteed to be
	 * valid until the next call to pop() or read().
	 *
	 * NOTE: count MUST BE no greater than bytesAvailable()
	 */
	StringRef peekExact(int count);

	/**
	 * Reads exactly count bytes into the buffer beginning at buf. All
	 * previously returned values from peekSome() and pookExact() are
	 * invalidated.
	 *
	 * WARNING: If you have previously advanced but not popped, will
	 * only pop the number of bytes read! (TODO?)
	 */
	Future<Void> read(uint8_t* buf, int count);

	/**
	 * Advances the point at which peekSome or peekExact will start by count
	 * bytes, but does not invalidate the results of any previous calls.
	 *
	 * NOTE: count must be no larger than bytesAvailable()
	 */
	void advance(int count);

	/**
	 * Releases count bytes from the beginning of the buffer. All previously
	 * returned values from peekSome() and peekExact() are invalidated.
	 *
	 * WARNING: count MUST BE no greater than the number of bytes previously
	 * advance()d (TODO?)
	 */
	void pop(int count);

	/**
	 * Returns when the connection is writable (?)
	 */
	Future<Void> onWritable();

	/**
	 * Write buf to the connection. Returns immediately before the underlying
	 * write succeeds.
	 */
	void write(StringRef buf);

	/**
	 * Returns when the underlying connection is closed.
	 */
	Future<Void> onClosed();

	/**
	 * Returns the network address and port of the other end of the connection.
	 */
	NetworkAddress getPeerAddress();

private:
	struct BufferedConnectionData* self;
};

#endif /* _BUFFERED_CONNECTION_H_ */
