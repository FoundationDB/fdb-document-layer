/*
 * Constants.cpp
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

#include "Constants.h"

namespace DocLayerConstants {

const uint64_t INDEX_KEY_LENGTH_LIMIT = (uint64_t)1e4;

const uint64_t FDB_KEY_LENGTH_LIMIT = (uint64_t)1e4;
const uint64_t FDB_VALUE_LENGTH_LIMIT = (uint64_t)1e5;

const uint64_t METADATA_CACHE_SIZE = (uint64_t)100;
const uint64_t METADATA_INVALID_VERSION = (uint64_t)-1;

const std::string METADATA = "metadata";
const std::string VERSION_KEY = "version";
const std::string INDICES_KEY = "indices";

const std::string SYSTEM_INDEXES = "system.indexes";
const std::string SYSTEM_NAMESPACES = "system.namespaces";

const double OPLOG_EXPIRATION_TIME = 172800000; // delete oplog 2 days ago
const double OPLOG_CLEAN_INTERVAL = 600; // clean every 10 minutes

const std::string OPLOG_DB = "local";
const std::string OPLOG_COL = "oplog.rs";

const std::string OP_UPDATE = "u";
const std::string OP_INSERT = "i";
const std::string OP_DELETE = "d";

const std::string DESCRIBE_DELETE_DOC = "delete document";

const char* OP_FIELD_TS = "ts";
const char* OP_FIELD_H = "h";
const char* OP_FIELD_V = "v";
const char* OP_FIELD_OP = "op";
const char* OP_FIELD_NS = "ns";
const char* OP_FIELD_O2 = "o2";
const char* OP_FIELD_O = "o";

const char* ID_FIELD = "_id";
const char* KEY_FIELD = "key";
const char* BUILD_ID_FIELD = "build id";
const char* STATUS_FIELD = "status";
const char* NAME_FIELD = "name";
const char* UNIQUE_FIELD = "unique";
const char* BACKGROUND_FIELD = "background";
const char* METADATA_VERSION_FIELD = "metadata version";
const char* NS_FIELD = "ns";
const char* QUERY_FIELD = "query";
const char* CURRENTLY_PROCESSING_DOC_FIELD = "currently processing document";

const std::string RENAME = "$rename";
const std::string SET = "$set";
const std::string SET_ON_INSERT = "$setOnInsert";
const std::string INC = "$inc";
const std::string MUL = "$mul";
const std::string MIN = "$min";
const std::string MAX = "$max";
const std::string PUSH = "$push";
const std::string CURRENT_DATE = "$currentDate";
const std::string ADD_TO_SET = "$addToSet";
const std::string QUERY_OPERATOR = "$query";

const char* INDEX_STATUS_READY = "ready";
const char* INDEX_STATUS_BUILDING = "building";
const char* INDEX_STATUS_ERROR = "error";

const char* MT_GUAGE_ACTIVE_CONNECTIONS = "dl_active_connections";
const char* MT_RATE_NEW_CONNECTIONS = "dl_new_connections";
const char* MT_GUAGE_ACTIVE_CURSORS = "dl_active_cursors";
const char* MT_HIST_MESSAGE_SZ = "dl_message_size_bytes";
const char* MT_TIME_QUERY_LATENCY_US = "dl_query_latency_useconds";
const char* MT_HIST_KEYS_PER_DOCUMENT = "dl_keys_per_doc";
const char* MT_HIST_DOCUMENT_SZ = "dl_doc_size_bytes";
const char* MT_HIST_DOCS_PER_INSERT = "dl_docs_per_insert";
const char* MT_TIME_INSERT_LATENCY_US = "dl_insert_latency_useconds";
const char* MT_HIST_INSERT_SZ = "dl_insert_size_bytes";
const char* MT_HIST_TR_PER_REQUEST = "dl_tr_per_request";
const char* MT_RATE_IDX_REBUILD = "dl_index_rebuild_rate";
const char* MT_RATE_TABLE_SCAN_DOCS = "dl_table_scan_rate";
const char* MT_RATE_IDX_SCAN_DOCS = "dl_index_scan_rate";
const char* MT_GUAGE_CPU_PERCENTAGE = "dl_cpu_percentage";
const char* MT_GUAGE_MAIN_THREAD_CPU_PERCENTAGE = "dl_main_th_cpu_percentage";
const char* MT_GUAGE_MEMORY_USAGE = "dl_memory_usage_bytes";

} // namespace DocLayerConstants