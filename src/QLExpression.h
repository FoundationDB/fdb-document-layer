/*
 * QLExpression.h
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

#ifndef _QL_EXPRESSION_H_
#define _QL_EXPRESSION_H_

#pragma once

#include "flow/flow.h"

#include "QLContext.h"
#include "QLTypes.h"

/**
 * An expression represents a pure function from a subdocument value to zero or more subdocument values
 */
struct IExpression {
	virtual void addref() = 0;
	virtual void delref() = 0;

	virtual GenFutureStream<Reference<IReadContext>> evaluate(Reference<IReadContext> const& document) = 0;

	virtual std::string toString() const = 0;

	/**
	 * Return the name of the index which, if it exists, indexes by the values of this expression
	 */
	virtual std::string get_index_key() const { return {}; }
};

Key encodeMaybeDotted(std::string fieldname);
/**
 * This expression implements a MongoDB dot-separated path expansion (it returns all subdocuments
 * patching the given path, expanding arrays as necessary).
 */
struct ExtPathExpression : IExpression, ReferenceCounted<ExtPathExpression>, FastAllocated<ExtPathExpression> {

	Standalone<StringRef> path;
	std::string strPath;
	bool expandLastArray;
	bool imputeNulls;

	void addref() { ReferenceCounted<ExtPathExpression>::addref(); }
	void delref() { ReferenceCounted<ExtPathExpression>::delref(); }

	std::string toString() const override { return "ExtPath(" + strPath + ")"; }

	ExtPathExpression(std::string const& strPath, bool const& expandLastArray, bool const& imputeNulls)
	    : strPath(strPath), expandLastArray(expandLastArray), imputeNulls(imputeNulls) {
		path = encodeMaybeDotted(strPath);
	}

	GenFutureStream<Reference<IReadContext>> evaluate(Reference<IReadContext> const& document) override;

	std::string get_index_key() const override {
		return expandLastArray ? strPath : std::string();
	} // FIXME: a.$n?.b.$n
};

#endif
