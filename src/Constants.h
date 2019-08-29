/*
 * Constants.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2019 Apple Inc. and the FoundationDB project authors
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

#ifndef FDB_DOC_LAYER_CONSTANTS_H
#define FDB_DOC_LAYER_CONSTANTS_H

#include <cstdint>
#include <string>

namespace DocLayerConstants {

// On a par with MongoDB limits
// https://docs.mongodb.com/manual/reference/limits/#indexes
extern const uint64_t INDEX_KEY_LENGTH_LIMIT; // This is set to 10K bytes, instead of 1K because why not.

// Document Layer Limits
extern const uint64_t FDB_KEY_LENGTH_LIMIT;
extern const uint64_t FDB_VALUE_LENGTH_LIMIT;

// Size of metadata cache in entries, number of collections
extern const uint64_t METADATA_CACHE_SIZE;
extern const uint64_t METADATA_INVALID_VERSION;

// KVS DocLayer internal keys
extern const std::string METADATA;
extern const std::string VERSION_KEY;
extern const std::string INDICES_KEY;

extern const std::string SYSTEM_INDEXES;
extern const std::string SYSTEM_NAMESPACES;

// BSON Field name constants
extern const char* ID_FIELD;
extern const char* KEY_FIELD;
extern const char* BUILD_ID_FIELD;
extern const char* STATUS_FIELD;
extern const char* NAME_FIELD;
extern const char* UNIQUE_FIELD;
extern const char* BACKGROUND_FIELD;
extern const char* METADATA_VERSION_FIELD;
extern const char* NS_FIELD;
extern const char* QUERY_FIELD;
extern const char* CURRENTLY_PROCESSING_DOC_FIELD;

// Mongo Operators
extern const std::string RENAME;
extern const std::string SET;
extern const std::string SET_ON_INSERT;
extern const std::string INC;
extern const std::string MUL;
extern const std::string MIN;
extern const std::string MAX;
extern const std::string PUSH;
extern const std::string CURRENT_DATE;
extern const std::string ADD_TO_SET;
extern const std::string QUERY_OPERATOR;

// Index status strings
extern const char* INDEX_STATUS_READY;
extern const char* INDEX_STATUS_BUILDING;
extern const char* INDEX_STATUS_ERROR;

// Metrics
extern const char* MT_GUAGE_ACTIVE_CONNECTIONS;
extern const char* MT_GUAGE_ACTIVE_CURSORS;
extern const char* MT_HIST_MESSAGE_SZ;
extern const char* MT_TIME_QUERY_LATENCY_US;
extern const char* MT_HIST_KEYS_PER_DOCUMENT;
extern const char* MT_HIST_DOCUMENT_SZ;
extern const char* MT_HIST_DOCS_PER_INSERT;
extern const char* MT_TIME_INSERT_LATENCY_US;
extern const char* MT_HIST_INSERT_SZ;
extern const char* MT_HIST_TR_PER_REQUEST;
extern const char* MT_RATE_IDX_REBUILD;
extern const char* MT_RATE_TABLE_SCAN_DOCS;
extern const char* MT_RATE_IDX_SCAN_DOCS;
extern const char* MT_GUAGE_CPU_PERCENTAGE;
extern const char* MT_GUAGE_MEMORY_USAGE;

} // namespace DocLayerConstants

#endif // FDB_DOC_LAYER_CONSTANTS_H
