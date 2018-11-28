/*
 * DocumentError.h
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

#ifndef DOCUMENT_ERROR_H
#define DOCUMENT_ERROR_H
#pragma once

#include "flow/Error.h"

#undef DOCLAYER_ERROR
#define DOCLAYER_ERROR(name, number, comment)                                                                          \
	inline Error name() { return Error(number); };                                                                     \
	enum { error_code_##name = number };
#include "error_definitions.h"

static void init_document_error() {
#undef DOCLAYER_ERROR
#define DOCLAYER_ERROR(name, number, comment)                                                                          \
	Error::errorCodeTable().addCode(number, #name, #comment);                                                          \
	enum { Duplicate_Error_Code_##number = 0 };
#include "error_definitions.h"
}

#endif
