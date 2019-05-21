/*
 * QLTypes.cpp
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

#include "QLTypes.h"
#include "DocumentError.h"
#include "bindings/flow/fdb_flow.h"
#include "bson.h"
#include "flow/flow.h"
#include "oid.h"
#include "util/hex.h"

using namespace FDB;

std::string DataValue::toString() const {

	if (representation.size() > 1)
		return wrap("x").getField("x").toString().substr(3);

	std::string s;
	s.reserve(representation.size() * 4);

	for (int i = 0; i < representation.size(); i++) {
		s += format("\\x%02x", representation[i]);
	}

	return s;
}

int DataValue::compare(DataValue const& other) const {
	return encode_key_part().compare(other.encode_key_part());
}

DVTypeCode DataValue::getSortType() const {
	return DVTypeCode(representation[0]);
}

bson::BSONType DataValue::getBSONType() const {
	switch (getSortType()) {
	case DVTypeCode::NUMBER:
		return (bson::BSONType)representation[11];
	case DVTypeCode::STRING:
		return bson::BSONType::String;
	case DVTypeCode::OBJECT:
	case DVTypeCode::PACKED_OBJECT:
		return bson::BSONType::Object;
	case DVTypeCode::ARRAY:
	case DVTypeCode::PACKED_ARRAY:
		return bson::BSONType::Array;
	case DVTypeCode::BYTES:
		return bson::BSONType::BinData;
	case DVTypeCode::OID:
		return bson::BSONType::jstOID;
	case DVTypeCode::BOOL:
		return bson::BSONType::Bool;
	case DVTypeCode::DATE:
		return bson::BSONType::Date;
	case DVTypeCode::NULL_ELEMENT:
		return bson::BSONType::jstNULL;
	case DVTypeCode::REGEX:
		return bson::BSONType::RegEx;
	case DVTypeCode::MIN_KEY:
		return bson::BSONType::MinKey;
	case DVTypeCode::MAX_KEY:
		return bson::BSONType::MaxKey;
	}

	TraceEvent(SevError, "BD_getBSONType_UnknownType").error(internal_error());
	throw internal_error();
}

std::string DataValue::escape_nulls() const {
	std::string ret;
	ret += representation[0];

	size_t last_pos = 0;
	const uint8_t* bytes = representation.begin() + 1;
	size_t len = representation.size() - 1;

	for (size_t pos = 0; pos < len; ++pos) {
		if (bytes[pos] == '\x00') {
			ret.append((const char*)bytes + last_pos, pos - last_pos);
			ret += '\x00';
			ret += '\xff';
			last_pos = pos + 1;
		}
	}

	ret.append((const char*)bytes + last_pos, len - last_pos);
	ret += '\x00';

	return ret;
}

static std::string unescape_nulls(StringRef key) {
	size_t e = key.size();
	std::string s; // FIXME: worth reserving space?
	s.append((const char*)key.begin(), 1);

	int b = 1;

	for (int i = 1; i < e; ++i) {
		if (key[i] == '\x00') {
			s += key.substr(b, i - b + 1).toString();
			i += 1;
			b = i + 1;
		}
	}

	s.resize(s.size() - 1); // terminating \x00 got included
	return s;
}

std::string DataValue::encode_key_part() const {
	switch (getSortType()) {
	case DVTypeCode::NUMBER:
		return representation.substr(0, 11).toString();
	case DVTypeCode::STRING:
		return escape_nulls();
	case DVTypeCode::PACKED_ARRAY:
	case DVTypeCode::PACKED_OBJECT:
		return escape_nulls();
	default:
		return representation.toString();
	}
}

std::string DataValue::encode_value() const {
	return representation.toString();
}

DataValue DataValue::decode_key_part(StringRef key) {
	auto type = (DVTypeCode)key[0];
	try {
		if (type == DVTypeCode::STRING || type == DVTypeCode::PACKED_ARRAY || type == DVTypeCode::PACKED_OBJECT) {
			return DataValue(StringRef(unescape_nulls(key)));
		} else if (type == DVTypeCode::NUMBER) {
			return decode_key_part(key, bson::BSONType::NumberDouble);
		} else {
			return DataValue(key);
		}
	} catch (Error& e) {
		TraceEvent(SevError, "BD_decode_key_part_error").detail("nonNumKey", printable(key)).error(e);
		throw;
	}
}

DataValue DataValue::decode_key_part(StringRef numKey, bson::BSONType numCode) {
	if ((DVTypeCode)numKey[0] == DVTypeCode::NUMBER) {
		if (numCode == bson::BSONType::NumberInt || numCode == bson::BSONType::NumberLong ||
		    numCode == bson::BSONType::NumberDouble) {
			Standalone<StringRef> s = makeString(12);
			uint8_t* buf = mutateString(s);
			memcpy(buf, numKey.begin(), 11);
			*(buf + 11) = numCode;
			return DataValue(s);
		}
	}

	TraceEvent(SevError, "BD_decode_key_part_error")
	    .detail("numKey", printable(numKey))
	    .detail("numCode", numCode)
	    .error(internal_error());
	throw internal_error();
}

DataValue DataValue::decode_value(StringRef val) {
	return DataValue(val);
}

DataValue DataValue::arrayOfLength(uint32_t length) {
	uint8_t buf[5];
	*buf = uint8_t(DVTypeCode::ARRAY);
	memcpy(buf + 1, &length, 4);
	return DataValue(StringRef(buf, 5));
}

DataValue DataValue::subObject() {
	auto code = uint8_t(DVTypeCode::OBJECT);
	return DataValue(StringRef(&code, 1));
}

DataValue DataValue::nullValue() {
	auto code = uint8_t(DVTypeCode::NULL_ELEMENT);
	return DataValue(StringRef(&code, 1));
}

bool DataValue::isSimpleType() const {
	return !(getBSONType() == bson::Object || getBSONType() == bson::Array);
}

// ******************************* Initializers ************************

static void store_long_double(uint8_t* dst, LongDouble val) {
	uint8_t* orig_dst = dst;
	uint8_t* src = (uint8_t*)&val + 9;

	// Copy everything backwards
	for (int i = 0; i < 10; i++) {
		*dst++ = *src--;
	}

	// Check sign bit, which is now the first bit
	if (*orig_dst & 0x80) {
		// Flip all the bits
		for (int i = 0; i < 10; i++) {
			*orig_dst = ~(*orig_dst);
			orig_dst++;
		}
	} else {
		// Just flip the sign bit
		*orig_dst = static_cast<uint8_t>(*orig_dst ^ 0x80);
	}
}

static void load_long_double(uint8_t* dst, LongDouble val) {
	uint8_t* orig_dst = dst;
	uint8_t* src = (uint8_t*)&val + 9;

	// Copy everything backwards
	for (int i = 0; i < 10; i++) {
		*dst++ = *src--;
	}

	// Back up one since we fell off the end of the array
	dst--;

	// Check sign bit, which is now the last bit
	if (*dst & 0x80) {
		// Just flip the sign bit
		*dst = static_cast<uint8_t>(*dst ^ 0x80);

	} else {
		// Flip all the bits
		for (int i = 0; i < 10; i++) {
			*orig_dst = ~(*orig_dst);
			orig_dst++;
		}
	}
}

void DataValue::init(const uint8_t* bytes, int len, DVTypeCode code) {
	Standalone<StringRef> s = makeString(len + 1);
	uint8_t* buf = mutateString(s);
	*buf = (uint8_t)code;
	memcpy(buf + 1, bytes, len);
	representation = s;
}

void DataValue::init_bindata(bson::BSONElement elem) {
	if (elem.binDataType() == bson::BinDataType::ByteArrayDeprecated)
		throw no_old_binary();
	int len = 0;
	const char* data = elem.binData(len);
	Standalone<StringRef> s = makeString(len + 6);
	uint8_t* buf = mutateString(s);
	*buf = (uint8_t)DVTypeCode::BYTES;
	memcpy(buf + 1, &len, 4);
	*(buf + 5) = (uint8_t)elem.binDataType();
	memcpy(buf + 6, data, len);
	representation = s;
}

void DataValue::init_int(int n) {
	uint8_t buf[12];
	*buf = uint8_t(DVTypeCode::NUMBER);
	store_long_double(buf + 1, n);
	*(buf + 11) = 16;
	representation = StringRef(buf, 12);
}

void DataValue::init_double(double d) {
	uint8_t buf[12];
	*buf = uint8_t(DVTypeCode::NUMBER);
	store_long_double(buf + 1, d);
	*(buf + 11) = 1;
	representation = StringRef(buf, 12);
}

void DataValue::init_long(long long n) {
	uint8_t buf[12];
	*buf = uint8_t(DVTypeCode::NUMBER);
	store_long_double(buf + 1, n);
	*(buf + 11) = 18;
	representation = StringRef(buf, 12);
}

void DataValue::init_bool(bool b) {
	representation = std::string().append(1, uint8_t(DVTypeCode::BOOL)).append(1, b ? 1 : 0);
}

void DataValue::init_date(bson::Date_t date) {
	uint8_t buf[11];
	*buf = uint8_t(DVTypeCode::DATE);
	store_long_double(buf + 1, date.millis);
	representation = StringRef(buf, 11);
}

void DataValue::init_id(bson::OID id) {
	uint8_t buf[13];
	*buf = uint8_t(DVTypeCode::OID);
	memcpy(buf + 1, id.getData(), 12);
	representation = StringRef(buf, 13);
}

void DataValue::init_array(uint32_t length) {
	uint8_t buf[5];
	*buf = uint8_t(DVTypeCode::ARRAY);
	memcpy(buf + 1, &length, 4);
	representation = StringRef(buf, 5);
}

void DataValue::init_bare_typecode(DVTypeCode type) {
	auto code = uint8_t(type);
	representation = StringRef(&code, 1);
}

void DataValue::init_packed_array(const char* arrdata, int arrsize) {
	Standalone<StringRef> s = makeString(arrsize + 1);
	uint8_t* buf = mutateString(s);
	*buf = (uint8_t)DVTypeCode::PACKED_ARRAY;
	memcpy(buf + 1, arrdata, arrsize);
	representation = s;
}

void DataValue::init_packed_object(const char* objdata, int objsize) {
	Standalone<StringRef> s = makeString(objsize + 1);
	uint8_t* buf = mutateString(s);
	*buf = (uint8_t)DVTypeCode::PACKED_OBJECT;
	memcpy(buf + 1, objdata, objsize);
	representation = s;
}

// ******************************* Constructors ************************

// Raw constructor. Do not use
DataValue::DataValue(StringRef rawValue) {
	representation = rawValue;
}

DataValue::DataValue(const uint8_t* bytes, int len, DVTypeCode code) {
	init(bytes, len, code);
}
DataValue::DataValue(const char* str, DVTypeCode code) {
	init((uint8_t*)str, strlen(str), code);
}
DataValue::DataValue(const char* str, int len, DVTypeCode code) {
	init((uint8_t*)str, len, code);
}
DataValue::DataValue(StringRef str, DVTypeCode code) {
	init(str.begin(), str.size(), code);
}
DataValue::DataValue(std::string str) {
	init((uint8_t*)str.data(), str.size(), DVTypeCode::STRING);
}

DataValue::DataValue(bool p) {
	init_bool(p);
}
DataValue::DataValue(int n) {
	init_int(n);
}
DataValue::DataValue(long long n) {
	init_long(n);
}
DataValue::DataValue(double d) {
	init_double(d);
}

DataValue::DataValue(bson::OID id) {
	init_id(id);
}
DataValue::DataValue(bson::Date_t date) {
	init_date(date);
}

DataValue::DataValue(bson::BSONElement el) {
	switch (el.type()) {
	case bson::BSONType::String:
		init((uint8_t*)el.String().data(), el.String().size(), DVTypeCode::STRING);
		break;
	case bson::BSONType::Bool:
		init_bool(el.Bool());
		break;
	case bson::BSONType::Date:
		init_date(el.Date());
		break;
	case bson::BSONType::jstOID:
		init_id(el.OID());
		break;
	case bson::BSONType::NumberInt:
		init_int(el.Int());
		break;
	case bson::BSONType::NumberLong:
		init_long(el.Long());
		break;
	case bson::BSONType::NumberDouble:
		init_double(el.Double());
		break;
	case bson::BSONType::BinData:
		init_bindata(el);
		break;
	case bson::BSONType::jstNULL:
		init_bare_typecode(DVTypeCode::NULL_ELEMENT);
		break;
	case bson::BSONType::RegEx:
		init((uint8_t*)el.rawdata(), el.size(), DVTypeCode::REGEX);
		break;
	case bson::BSONType::Object:
		init_bare_typecode(DVTypeCode::OBJECT);
		break;
	case bson::BSONType::Array:
		init_array(el.Array().size());
		break;
	case bson::BSONType::MaxKey:
		init_bare_typecode(DVTypeCode::MAX_KEY);
		break;
	case bson::BSONType::MinKey:
		init_bare_typecode(DVTypeCode::MIN_KEY);
		break;
	case bson::BSONType::Timestamp:
		throw no_mongo_timestamps();
	case bson::BSONType::Symbol:
		throw no_symbol_type();
	default:
		TraceEvent(SevError, "BD_DataValue_error").detail("type", el.type());
		ASSERT(false);
	}
}

DataValue::DataValue(bson::BSONObj obj) {
	init_packed_object(obj.objdata(), obj.objsize());
}
DataValue::DataValue(bson::BSONArray arr) {
	init_packed_array(arr.objdata(), arr.objsize());
}

DataValue DataValue::firstOfType(const DataValue& other) {
	DataValue dv = DataValue(StringRef());
	dv.init(nullptr, 0, other.getSortType());
	return dv;
}

DataValue DataValue::firstOfnextType(const DataValue& other) {
	DataValue dv = DataValue(StringRef());
	dv.init(nullptr, 0, (DVTypeCode)((int)other.getSortType() + 1));
	return dv;
}

// *************************** Extractors *********************

bool DataValue::getBool() const {
	ASSERT(representation.size() == 2);
	ASSERT(getSortType() == DVTypeCode::BOOL);

	return (bool)representation[1];
}

bson::Date_t DataValue::getDate() const {
	ASSERT(representation.size() == 11);
	ASSERT(getSortType() == DVTypeCode::DATE);

	LongDouble r;
	load_long_double((uint8_t*)&r, *(LongDouble*)(representation.begin() + 1));

	return {(unsigned long long)r};
}

bson::OID DataValue::getId() const {
	ASSERT(representation.size() == 13);
	ASSERT(getSortType() == DVTypeCode::OID);

	unsigned char buf[12];
	memcpy(buf, representation.begin() + 1, 12);

	return bson::OID(bson::toHexLower(buf, 12));
}

std::string DataValue::getString() const {
	ASSERT(getSortType() == DVTypeCode::STRING);

	return representation.substr(1).toString();
}

StringRef DataValue::getBinaryData() const {
	ASSERT(getSortType() == DVTypeCode::BYTES);

	return representation.substr(1);
}

int32_t DataValue::getInt() const {
	ASSERT(representation.size() == 12);
	ASSERT(getSortType() == DVTypeCode::NUMBER);
	ASSERT(representation[11] == 16);

	LongDouble r;

	load_long_double((uint8_t*)&r, *(LongDouble*)(representation.begin() + 1));
	return (int32_t)r;
}

int64_t DataValue::getLong() const {
	ASSERT(representation.size() == 12);
	ASSERT(getSortType() == DVTypeCode::NUMBER);
	ASSERT(representation[11] == 18);

	LongDouble r;

	load_long_double((uint8_t*)&r, *(LongDouble*)(representation.begin() + 1));
	return (int64_t)r;
}

double DataValue::getDouble() const {
	ASSERT(representation.size() == 12);
	ASSERT(getSortType() == DVTypeCode::NUMBER);
	ASSERT(representation[11] == 1);

	LongDouble r;

	load_long_double((uint8_t*)&r, *(LongDouble*)(representation.begin() + 1));
	return (double)r;
}

int DataValue::getArraysize() const {
	ASSERT(representation.size() == 5);
	ASSERT(getSortType() == DVTypeCode::ARRAY);

	int n;
	memcpy(&n, representation.begin() + 1, 4);

	return n;
}

bson::BSONElement DataValue::getRegExObject() const {
	ASSERT(getSortType() == DVTypeCode::REGEX);
	return bson::BSONElement((const char*)representation.substr(1).begin(), representation.size());
}

bson::BSONObj DataValue::getPackedObject() const {
	ASSERT(getSortType() == DVTypeCode::PACKED_OBJECT);
	return bson::BSONObj((const char*)representation.substr(1).begin());
}

bson::BSONArray DataValue::getPackedArray() const {
	ASSERT(getSortType() == DVTypeCode::PACKED_ARRAY);
	return bson::BSONArray(bson::BSONObj((const char*)representation.substr(1).begin()));
}

bson::BSONObj DataValue::wrap(const char* fieldname) const {
	switch (getBSONType()) {
	case bson::BSONType::NumberInt:
		return BSON(fieldname << getInt());
	case bson::BSONType::NumberLong:
		return BSON(fieldname << (long long)getLong());
	case bson::BSONType::NumberDouble:
		return BSON(fieldname << getDouble());
	case bson::BSONType::String:
		return BSON(fieldname << getString());
	case bson::BSONType::BinData:
		return bson::BSONObjBuilder()
		    .appendBinData(fieldname, getBinaryData().size() - 5, (bson::BinDataType) * (getBinaryData().begin() + 4),
		                   getBinaryData().begin() + 5)
		    .obj();
	case bson::BSONType::Bool:
		return BSON(fieldname << getBool());
	case bson::BSONType::jstOID:
		return BSON(fieldname << getId());
	case bson::BSONType::Date:
		return BSON(fieldname << getDate());
	case bson::BSONType::jstNULL:
		return bson::BSONObjBuilder().appendNull(fieldname).obj();
	case bson::BSONType::Object:
		return BSON(fieldname << getPackedObject());
	case bson::BSONType::Array:
		return BSON(fieldname << getPackedArray());
	case bson::BSONType::RegEx:
		return BSON(fieldname << bson::BSONElement((const char*)getBinaryData().begin(), getBinaryData().size()));
	case bson::BSONType::MaxKey:
		return bson::BSONObjBuilder().appendMaxKey(fieldname).obj();
	case bson::BSONType::MinKey:
		return bson::BSONObjBuilder().appendMinKey(fieldname).obj();
	default: {
		TraceEvent(SevError, "BD_wrap_error").detail("type", getBSONType());
		throw internal_error();
	}
	}
}

// FIXME: assumes well encoded
// FIXME: Create a different version without bounds checking for when we know we're in the middle of an encoded key, so
// there's no risk of reading invalid memory?
static size_t find_string_terminator(const uint8_t* bytes, size_t max_length_plus_one) {
	size_t i = 0;

	while (i < max_length_plus_one - 1) {
		if (bytes[i] == 0 && bytes[i + 1] != 255) {
			break;
		}
		i += (bytes[i] == 0 ? 2 : 1);
	}
	if (i >= max_length_plus_one)
		i = max_length_plus_one - 1;
	if (bytes[i] != 0)
		throw internal_error();

	return i;
}

static int getKeyPartLength(const uint8_t* ptr, size_t max_length_plus_one) {
	switch (DVTypeCode(*ptr)) {
	case DVTypeCode::NUMBER:
		return 11;

	case DVTypeCode::STRING:
	case DVTypeCode::PACKED_ARRAY:
	case DVTypeCode::PACKED_OBJECT:
		return find_string_terminator(ptr, max_length_plus_one) + 1;

	case DVTypeCode::ARRAY:
		return 5;

	case DVTypeCode::OBJECT:
	case DVTypeCode::MIN_KEY:
	case DVTypeCode::MAX_KEY:
	case DVTypeCode::NULL_ELEMENT:
		return 1;

	case DVTypeCode::OID:
		return 13;

	case DVTypeCode::BOOL:
		return 2;

	case DVTypeCode::DATE:
		return 11;

	case DVTypeCode::BYTES:
		return *(int*)(ptr + 1) + 6;

	default: {
		TraceEvent(SevError, "BD_getKeyPartLength_error").detail("BadValue", *ptr);
		throw internal_error();
	}
	}
}

/**
 * Get a StringRef to a substring of bytes that represents the given itemNumber
 * in the encoded bytes string.  itemNumber is 0-based.
 */
StringRef DataKey::decode_item(StringRef bytes, int itemNumber) {
	auto start = bytes.begin();
	int remaining = bytes.size();
	int items = 0;
	try {
		while (remaining > 0) {
			int itemLen = getKeyPartLength(start, remaining);
			if (itemNumber == items++)
				return {start, itemLen};
			start += itemLen;
			remaining -= itemLen;
		}
	} catch (Error& e) {
		TraceEvent(SevError, "BD_decode_bytes_error").detail("BadString", printable(bytes));
		throw;
	}

	return {};
}

/**
 * Get a StringRef to a substring of bytes that represents the given itemNumber,
 * counting from the end.  itemNumber is 0-based so 0 is the last item, 1 is the
 * second to last, etc.  If pTotalItems is not NULL it will be updated with the total
 * number of encoded items found in bytes.
 */
StringRef DataKey::decode_item_rev(StringRef bytes, int itemNumber, int* pTotalItems) {
	int qsize = itemNumber + 1;
	ASSERT(qsize > 0);
	auto* queue = (StringRef*)alloca(sizeof(StringRef) * qsize);
	int qstart = 0;
	int nItems = 0;

	auto start = bytes.begin();
	int remaining = bytes.size();
	try {
		while (remaining > 0) {
			int itemLen = getKeyPartLength(start, remaining);
			queue[qstart] = StringRef(start, itemLen);
			if (++qstart == qsize)
				qstart = 0;
			++nItems;
			start += itemLen;
			remaining -= itemLen;
		}

		if (nItems > itemNumber) {
			if (pTotalItems != nullptr)
				*pTotalItems = nItems;
			return queue[qstart];
		}
	} catch (Error& e) {
		TraceEvent(SevError, "BD_decode_bytes_error").detail("BadString", printable(bytes));
		throw;
	}

	return {};
}

DataKey DataKey::decode_bytes(Standalone<StringRef> bytes) {
	DataKey ret;

	ret.representation = bytes.toString();

	auto ptr = bytes.begin();
	size_t so_far = 0;

	try {
		while (so_far < bytes.size()) {
			ret.offsets.push_back(so_far);
			ptr += getKeyPartLength(ptr, bytes.size() - so_far);
			so_far = ptr - bytes.begin();
		}
	} catch (Error& e) {
		TraceEvent(SevError, "BD_decode_bytes_error").detail("BadString", printable(bytes));
		throw;
	}

	if (ret.offsets.empty()) {
		ret.offsets.push_back(0);
	}

	return ret;
}

Standalone<StringRef> DataKey::operator[](int i) const {
	const uint8_t* ptr = (const uint8_t*)representation.c_str() + offsets[i];
	size_t amount_left = representation.size() - offsets[i];
	return Standalone<StringRef>(StringRef(ptr, getKeyPartLength(ptr, amount_left)));
}

Standalone<StringRef> DataKey::prefix(int i) const {
	if (i > offsets.size()) {
		throw internal_error();
	}
	if (i == offsets.size())
		return bytes();
	else
		return Standalone<StringRef>(StringRef((const uint8_t*)representation.c_str(), offsets[i]));
}

DataKey DataKey::keyPrefix(int i) const {
	return DataKey(prefix(i).toString(), std::vector<int>(offsets.begin(), offsets.begin() + i));
}

bool DataKey::startsWith(DataKey const& other) const {
	return size() >= other.size() && prefix(other.size()) == other.bytes();
}
