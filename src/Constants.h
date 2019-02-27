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

namespace DocLayerConstants {

// On a par with MongoDB limits
// https://docs.mongodb.com/manual/reference/limits/#indexes
extern const uint64_t INDEX_KEY_LENGTH_LIMIT; // This is set to 10K bytes, instead of 1K because why not.

// Document Layer Limits
extern const uint64_t FDB_KEY_LENGTH_LIMIT;
extern const uint64_t FDB_VALUE_LENGTH_LIMIT;

} // namespace DocLayerConstants

#endif // FDB_DOC_LAYER_CONSTANTS_H
