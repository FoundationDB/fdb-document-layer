/*
 * BufferedConnection.actor.cpp
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

#include "BufferedConnection.h"

#include "flow/Net2Packet.h"
#include "flow/actorcompiler.h" // This must be the last #include.

#define BC_BLOCK_SIZE 4096

struct BCBlock : FastAllocated<BCBlock> /*See below, NonCopyable */ {
	Arena arena;
	enum { DATA_SIZE = BC_BLOCK_SIZE - sizeof(Arena) };
	uint8_t data[DATA_SIZE];
	BCBlock() { static_assert(sizeof(BCBlock) == BC_BLOCK_SIZE, "BCBlock size mismatch"); }
	// This is copied from NonCopyable, because MSVC doesn't implement properly "Empty Base Class Optimization"
	// for more details take a look here :
	// http://stackoverflow.com/questions/12701469/why-empty-base-class-optimization-is-not-working
	BCBlock(const BCBlock&) = delete;
	BCBlock& operator=(const BCBlock&) = delete;
};

struct BufferedConnectionData {
	explicit BufferedConnectionData(Reference<IConnection> connection);
	~BufferedConnectionData() {
		conn.cancel();
		connection->close();
	}

	Reference<IConnection> connection;
	Future<Void> conn;

	// Blocks that have been advance()d away but not fully popped.  memory needs to be held, but that's it
	std::list<BCBlock*> deadlist;
	int deadlist_begin_offset;
	std::list<BCBlock*> buffer;
	int buffer_begin_offset, buffer_end_offset;
	AsyncVar<int> total_bytes;
	AsyncVar<int> desired_bytes;

	AsyncTrigger on_data_write;
	UnsentPacketQueue unsent;

	void copyInto(uint8_t* buf, int count);
};

ACTOR Future<Void> reader(BufferedConnectionData* self) {
	loop {
		loop {
			wait(self->connection->onReadable());
			wait(delay(0, TaskReadSocket));

			int to_read = self->desired_bytes.get() + BCBlock::DATA_SIZE - self->total_bytes.get();
			if (to_read <= 0)
				break;

			if (self->buffer_end_offset == BCBlock::DATA_SIZE || self->buffer.empty()) {
				// Last block is full, add one
				self->buffer.push_back(new BCBlock());
				self->buffer_end_offset = 0;
			}

			to_read = std::min(to_read, BCBlock::DATA_SIZE - self->buffer_end_offset);

			uint8_t* buf = self->buffer.back()->data + self->buffer_end_offset;

			int rb = self->connection->read(buf, buf + to_read);

			self->buffer_end_offset += rb;
			self->total_bytes.set(self->total_bytes.get() + rb);
		}

		wait(self->desired_bytes.onChange());
	}
}

ACTOR Future<Void> writer(BufferedConnectionData* self) {
	loop {
		wait(self->on_data_write.onTrigger());

		loop {
			int wb = self->connection->write(self->unsent.getUnsent());

			if (wb) {
				self->unsent.sent(wb);
			}

			if (self->unsent.empty()) {
				break;
			}

			wait(self->connection->onWritable());
			wait(delay(0, TaskWriteSocket));
		}
	}
}

ACTOR Future<Void> connectionKeeper(BufferedConnectionData* self) {
	try {
		wait(reader(self) || writer(self));
		ASSERT(false);
		throw internal_error();
	} catch (Error& e) {
		return Void();
	}
}

BufferedConnectionData::BufferedConnectionData(Reference<IConnection> connection)
    : connection(connection),
      total_bytes(0),
      desired_bytes(0),
      deadlist_begin_offset(0),
      buffer_begin_offset(0),
      buffer_end_offset(0) {
	conn = connectionKeeper(this);
}

BufferedConnection::BufferedConnection(Reference<IConnection> connection)
    : self(new BufferedConnectionData(connection)) {}

BufferedConnection::~BufferedConnection() {
	delete self;
}

void BufferedConnectionData::copyInto(uint8_t* buf, int count) {
	uint8_t* ptr = buf;
	int offset = buffer_begin_offset;

	auto it = buffer.begin();

	int remaining = count;

	while (remaining) {
		int to_copy = std::min(BCBlock::DATA_SIZE - offset, remaining);
		memcpy(ptr, (*it)->data + offset, to_copy);
		++it;
		offset = 0;
		ptr += to_copy;
		remaining -= to_copy;
	}
}

StringRef BufferedConnection::peekExact(int count) {
	ASSERT(count <= self->total_bytes.get());

	if (self->buffer_begin_offset + count <= BCBlock::DATA_SIZE) {
		/* requested byte range is in a single block */
		return {self->buffer.front()->data + self->buffer_begin_offset, count};
	} else {
		/* find the _last_ block containing data for this range */
		int remaining = count - (BCBlock::DATA_SIZE - self->buffer_begin_offset);

		auto it = ++(self->buffer.begin());

		while (remaining > BCBlock::DATA_SIZE) {
			++it;
			remaining -= BCBlock::DATA_SIZE;
		}

		/* Allocate memory in the arena of the last block from which we are copying, and copy the requested range into
		 * the newly allocated memory */
		auto* buf = new ((*it)->arena) uint8_t[count];

		self->copyInto(buf, count);

		return {buf, count};
	}
}

void BufferedConnection::advance(int count) {
	ASSERT(count <= self->total_bytes.get());

	while (count) {
		int to_remove = std::min(count, BCBlock::DATA_SIZE - self->buffer_begin_offset);

		self->desired_bytes.set(self->desired_bytes.get() - to_remove);
		self->total_bytes.set(self->total_bytes.get() - to_remove);

		self->buffer_begin_offset += to_remove;
		if (self->buffer_begin_offset == BCBlock::DATA_SIZE) {
			self->deadlist.push_back(self->buffer.front());
			self->buffer.pop_front();
			self->buffer_begin_offset = 0;
		}

		count -= to_remove;
	}
}

void BufferedConnection::pop(int count) {
	while (count) {
		int to_remove = std::min(count, BCBlock::DATA_SIZE - self->deadlist_begin_offset);

		self->deadlist_begin_offset += to_remove;
		if (self->deadlist_begin_offset == BCBlock::DATA_SIZE) {
			delete self->deadlist.front();
			self->deadlist.pop_front();
			self->deadlist_begin_offset = 0;
		}

		count -= to_remove;
	}
}

void BufferedConnection::write(StringRef buf) {
	int remaining = buf.size();
	const uint8_t* ptr = buf.begin();

	PacketBuffer* pb = self->unsent.getWriteBuffer();

	while (remaining) {
		int writable = PacketBuffer::DATA_SIZE - pb->bytes_written;
		if (!writable) {
			pb->next = new PacketBuffer();
			self->unsent.setWriteBuffer(pb->nextPacketBuffer());
			pb = pb->nextPacketBuffer();
			writable = PacketBuffer::DATA_SIZE;
		}

		int to_write = std::min(remaining, writable);

		memcpy(pb->data + pb->bytes_written, ptr, to_write);
		pb->bytes_written += to_write;
		ptr += to_write;

		remaining -= to_write;
	}

	self->on_data_write.trigger();
}

ACTOR Future<Void> doOnBytesAvailable(BufferedConnectionData* self, int count) {
	if (count > self->desired_bytes.get()) {
		self->desired_bytes.set(count);
	}

	if (self->total_bytes.get() >= count)
		return Void();

	loop {
		wait(self->total_bytes.onChange());

		if (self->total_bytes.get() >= count) {
			wait(delay(0, TaskDefaultPromiseEndpoint));
			return Void();
		}
	}
}

Future<Void> BufferedConnection::onBytesAvailable(int count) {
	return doOnBytesAvailable(self, count);
}

ACTOR Future<Void> doRead(BufferedConnection* bc, BufferedConnectionData* self, uint8_t* buf, int count) {
	wait(bc->onBytesAvailable(count));
	self->copyInto(buf, count);
	bc->advance(count);
	bc->pop(count);
	return Void();
}

Future<Void> BufferedConnection::read(uint8_t* buf, int count) {
	return doRead(this, self, buf, count);
}

Future<Void> BufferedConnection::onClosed() {
	return self->conn;
}

Future<Void> BufferedConnection::onWritable() {
	return self->connection->onWritable();
}

NetworkAddress BufferedConnection::getPeerAddress() {
	return self->connection->getPeerAddress();
}
