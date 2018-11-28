/*
 * ExtOperator.h
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

#ifndef _EXT_OPERATOR_H_
#define _EXT_OPERATOR_H_

#pragma once

#include "IDispatched.h"
#include "QLPredicate.h"

#include "bson.h"

struct ExtValueOperator
    : IDispatched<ExtValueOperator,
                  std::string,
                  std::function<Reference<IPredicate>(std::string const&, bson::BSONElement const&)>> {
	static Reference<IPredicate> toPredicate(std::string const& op,
	                                         std::string const& unencoded_path,
	                                         bson::BSONElement const& element) {
		return dispatch(op)(unencoded_path, element);
	}
};

#define REGISTER_VALUE_OPERATOR(Op, Key)                                                                               \
	const char* Op::name = Key;                                                                                        \
	REGISTER_COMMAND(ExtValueOperator, Op, name, toPredicate);

struct ExtBoolOperator
    : IDispatched<ExtBoolOperator, std::string, std::function<Reference<IPredicate>(bson::BSONObj const&)>> {
	static Reference<IPredicate> toPredicate(std::string const& op, bson::BSONObj const& query) {
		return dispatch(op)(query);
	}
};

#define REGISTER_BOOL_OPERATOR(Op, Key)                                                                                \
	const char* Op::name = Key;                                                                                        \
	REGISTER_COMMAND(ExtBoolOperator, Op, name, toPredicate);

struct ExtUpdateOperator
    : IDispatched<
          ExtUpdateOperator,
          std::string,
          std::function<Future<Void>(Reference<IReadWriteContext>, StringRef const&, bson::BSONElement const&)>> {
	static Future<Void> execute(std::string const& op,
	                            Reference<IReadWriteContext> cx,
	                            StringRef const& path,
	                            bson::BSONElement const& element) {
		return dispatch(op)(cx, path, element);
	}
};

#define REGISTER_UPDATE_OPERATOR(Op, Key)                                                                              \
	const char* Op::name = Key;                                                                                        \
	REGISTER_COMMAND(ExtUpdateOperator, Op, name, execute);

#endif /* _EXT_OPERATOR_H_ */
