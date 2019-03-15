/*
 * Ext.h
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

#ifndef _EXT_H_
#define _EXT_H_

#pragma once

#include "bson.h"
#include "flow/flow.h"

#define EXT_SERVER_VERSION "3.0.0"
#define EXT_SERVER_VERSION_ARRAY BSON_ARRAY(3 << 0 << 0)
#define EXT_MIN_WIRE_VERSION 0
#define EXT_MAX_WIRE_VERSION 3

#endif /* _EXT_H_ */
