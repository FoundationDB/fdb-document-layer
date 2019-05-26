/*
 * QLTypes.h
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

#ifndef _QL_TYPES_H_
#define _QL_TYPES_H_

#pragma once

#include "bson.h"
#include "flow/Arena.h"
#include "flow/flow.h"
#include "oid.h"
#include <stdint.h>

#ifdef _MSC_VER
#include "FPUUtils.h"
#include <type_traits>

#pragma pack(push)
#pragma pack(2)
typedef struct LongDouble final {
	unsigned char _data[10];

public:
	LongDouble() { memset(_data, 0, sizeof(_data)); }

	template <typename T>
	LongDouble(T value) {
		if (std::is_floating_point<T>()) {
			double max_var = (double)value;
			convert64dto80((unsigned char*)&max_var, _data);
		} else {
			long long max_var = (long long)value;
			convert64ito80((unsigned char*)&max_var, _data);
		}
	}

	template <typename T>
	operator T() {
		if (std::is_floating_point<T>()) {
			double value = 0;
			convert80to64d(_data, (unsigned char*)&value);
			return T(value);
		}
		long long value = 0;
		convert80to64i(_data, (unsigned char*)&value);
		return T(value);
	}

	LongDouble& operator+=(const LongDouble& value) {
		add80to80(_data, value._data, _data);
		return *this;
	}
	LongDouble& operator*=(const LongDouble& value) {
		multiply80to80(_data, value._data, _data);
		return *this;
	}
	LongDouble operator+(const LongDouble& value) const { return LongDouble(*this) += value; }
	LongDouble operator*(const LongDouble& value) const { return LongDouble(*this) *= value; }
} LongDouble;
#pragma pack(pop)

#else
typedef long double LongDouble;
#endif

enum class DVTypeCode : uint8_t {
	MIN_KEY = 0,
	NULL_ELEMENT = 20,
	NUMBER = 30,
	STRING = 40,
	OBJECT = 50,
	PACKED_OBJECT = 51,
	ARRAY = 60,
	PACKED_ARRAY = 61,
	BYTES = 70,
	OID = 80,
	BOOL = 90,
	DATE = 100,
	REGEX = 110,
	MAX_KEY = 255
};

struct DataValue {
	DataValue() = default;
	DataValue(const uint8_t* bytes, int len, DVTypeCode code);
	DataValue(const char* str, DVTypeCode code);
	DataValue(const char* str, int len, DVTypeCode code);
	DataValue(StringRef str, DVTypeCode code);
	explicit DataValue(std::string str);
	explicit DataValue(bool p);
	explicit DataValue(int n);
	explicit DataValue(long long n);
	explicit DataValue(double d);
	explicit DataValue(bson::OID id);
	explicit DataValue(bson::Date_t date);
	explicit DataValue(bson::BSONElement el);
	explicit DataValue(bson::BSONArray arr);
	explicit DataValue(bson::BSONObj obj);

	int compare(DataValue const& other) const;

	Standalone<StringRef> encode_key_part() const;
	StringRef encode_value() const;

	static DataValue decode_key_part(StringRef nonNumKey);
	static DataValue decode_key_part(StringRef numKey, bson::BSONType numCode);
	static DataValue decode_value(StringRef val);

	static DataValue arrayOfLength(uint32_t length);
	static DataValue subObject();
	static DataValue nullValue();

	static DataValue firstOfType(const DataValue& other);
	static DataValue firstOfnextType(const DataValue& other);

	bson::BSONType getBSONType() const;
	DVTypeCode getSortType() const;

	bool getBool() const;
	bson::Date_t getDate() const;
	bson::OID getId() const;
	std::string getString() const;
	StringRef getBinaryData() const;
	int32_t getInt() const;
	int64_t getLong() const;
	double getDouble() const;

	int getArraysize() const;

	bson::BSONElement getRegExObject() const;
	bson::BSONObj getPackedObject() const;
	bson::BSONArray getPackedArray() const;

	bson::BSONObj wrap(const char* fieldname) const;

	std::string toString() const;

	bool isSimpleType() const;

	inline bool operator==(const DataValue& rhs) const { return compare(rhs) == 0; }
	inline bool operator!=(const DataValue& rhs) const { return !operator==(rhs); }
	inline bool operator<(const DataValue& rhs) const { return compare(rhs) < 0; }
	inline bool operator>(const DataValue& rhs) const { return compare(rhs) > 0; }
	inline bool operator<=(const DataValue& rhs) const { return !operator>(rhs); }
	inline bool operator>=(const DataValue& rhs) const { return !operator<(rhs); }

private:
	explicit DataValue(StringRef rawValue);
	Standalone<StringRef> representation;

	void init(const uint8_t* bytes, int len, DVTypeCode code);
	void init_bindata(bson::BSONElement elem);
	void init_int(int n);
	void init_double(double d);
	void init_long(long long n);
	void init_bool(bool b);
	void init_date(bson::Date_t date);
	void init_id(bson::OID id);
	void init_array(uint32_t length);
	void init_bare_typecode(DVTypeCode type);
	void init_packed_array(const char* arrData, int arrSize);
	void init_packed_object(const char* objData, int objSize);

	std::string escape_nulls() const;
};

struct DataKey {
	DataKey() = default;

	static DataKey decode_bytes(Standalone<StringRef> bytes);
	static StringRef decode_item(StringRef bytes, int itemNumber);
	static StringRef decode_item_rev(StringRef bytes, int itemNumber, int* ptotalItems = nullptr);

	DataKey& append(StringRef el) {
		offsets.push_back(representation.length());
		representation += el.toString();
		return *this;
	}

	DataKey operator+(const DataKey& rhs) {
		DataKey ret(*this);

		auto offset = ret.representation.size();

		for (auto i : rhs.offsets) {
			ret.offsets.push_back((i == -1) ? i : i + offset);
		}

		ret.representation += rhs.representation;

		return ret;
	}

	DataKey(const DataKey&) = default;

	DataKey(std::string const& repr, std::vector<int> offsets) : representation(repr), offsets(offsets) {}

	DataKey(DataKey&& other) {
		representation = std::move(other.representation);
		offsets = std::move(other.offsets);
	}

	DataKey& operator=(DataKey&& other) {
		representation = std::move(other.representation);
		offsets = std::move(other.offsets);
		return *this;
	}

	// returns the ith item (0-indexed) of this DataKey
	Standalone<StringRef> operator[](int i) const;

	// returns the first i items (1-indexed)
	Standalone<StringRef> prefix(int i) const;

	// returns a DataKey holding the first i items (1-indexed) of this one in its own memory
	DataKey keyPrefix(int i) const;

	// returns the number of items (1-indexed) in this DataKey
	int size() const { return offsets.size(); }

	int byteSize() const { return representation.size(); }

	bool startsWith(DataKey const& other) const;

	Standalone<StringRef> bytes(int offset = 0) const { // FIXME: return stringref
		return Standalone<StringRef>(representation.substr(offsets[offset]));
	}

	const std::string& toString() const { return representation; }

private:
	std::string representation;
	std::vector<int> offsets;
};

#endif /* _QL_TYPES_H_ */
