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
const char* GET_MORE_CMD_BATCH_SIZE_FIELD = "batchSize";
const char* GET_MORE_CMD_CURSOR_ID_FIELD = "getMore";
const char* GET_MORE_CMD_CURSOR_COLLECTION_FIELD = "collection";
const char* FIND_CMD_FIND_FIELD = "find";
const char* FIND_CMD_FILTER_FIELD = "filter";
const char* FIND_CMD_SORT_FIELD = "sort";
const char* FIND_CMD_PROJECTION_FIELD = "projection";
const char* FIND_CMD_HINT_FIELD = "hint";
const char* FIND_CMD_SKIP_FIELD = "skip";
const char* FIND_CMD_LIMIT_FIELD = "limit";
const char* FIND_CMD_BATCH_SIZE_FIELD = "batchSize";
const char* FIND_CMD_MAX_TIME_MS_FIELD = "maxTimeMS";
const char* FIND_CMD_REPLY_CURSOR_FIELD = "cursor";
const char* FIND_CMD_REPLY_CURSOR_ID_FIELD = "id";
const char* FIND_CMD_REPLY_CURSOR_NS_FIELD = "ns";
const char* FIND_CMD_REPLY_CURSOR_FIRST_BATCH_FIELD = "firstBatch";
const char* FIND_CMD_REPLY_CURSOR_NEXT_BATCH_FIELD = "nextBatch";
const char* KILL_CURSORS_CMD_KILL_CURSORS_FIELD = "killCursors";
const char* KILL_CURSORS_CMD_CURSORS_FIELD = "cursors";
const char* EXPLAIN_CMD_FIELD = "explain";

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

} // namespace DocLayerConstants