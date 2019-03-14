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

} // namespace DocLayerConstants