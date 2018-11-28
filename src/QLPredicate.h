/*
 * QLPredicate.h
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

#ifndef _QL_PREDICATE_H_
#define _QL_PREDICATE_H_

#pragma once

#include "flow/flow.h"

#include "QLContext.h"
#include "QLExpression.h"
#include "QLTypes.h"
#include "pcrecpp.h" // needed for pcrecpp::RE_Options

struct IPredicate {
	virtual void addref() = 0;
	virtual void delref() = 0;

	enum TypeCode {
		ANY,
		ALL,
		NONE,
		AND,
		OR,
		EQ,
		NOT,
		RANGE,
		BSON_TYPE,
		EXISTS,
		ARRAY_SIZE,
		ARRAY_ALL,
		ARRAY_ELEM_MATCH,
		LITERAL_SUBOBJECT_MATCH,
		IS_OBJECT,
		REGEX,
		DISTINCT
	};

	struct SimplifyAndContext {
		SimplifyAndContext() : isNone(false) {}
		Reference<struct RangePredicate> rangeLike;
		std::vector<Reference<IPredicate>> other;
		bool isNone;
	};

	virtual TypeCode getTypeCode() const = 0;

	virtual Future<bool> evaluate(Reference<IReadContext> const& context) = 0;
	virtual Reference<IPredicate> simplify() { return Reference<IPredicate>::addRef(this); }
	virtual std::string toString() = 0;

	/**
	 * simplify_and MUST do exactly one of the following
	 *   - Update a member of the SimplifyAndContext to represent the logical AND of this with that member
	 *   - Add this to cx.other (hopefully because this can't be combined into anything else in cx)
	 * The default implemenation performs the latter.
	 */
	virtual void simplify_and(SimplifyAndContext& cx) { cx.other.push_back(Reference<IPredicate>::addRef(this)); }

	/**
	 * Returns (via out params) a minimum and maximum data value that could possibly be evaluated to true by this
	 * predicate That is, if (min.present()&&x<min)||(max.present()&&x>max) then evaluate(x) == false.
	 */
	virtual void get_range(Optional<DataValue>& min, Optional<DataValue>& max) {}

	/**
	 * Returns whether every key falling in the range returned by get_range would be evaluated to true by this
	 * predicate. I.e., if true, then (min.present()&&x<min)||(max.present()&&x>max) if and only if evaluate(x) ==
	 * false.
	 */
	virtual bool range_is_tight() { return false; }

	/**
	 * Controls whether an ExtPathExpression directly wrapping this predicate will generate null contexts in relevant
	 * situations.
	 */
	virtual bool wantsNulls() { return true; }

	friend struct NotPredicate;

protected:
	virtual Reference<IPredicate> simplify_not();
};

/**
 * AnyPredicate(expr, pred) yields true iff any of the subdocuments yielded by expr are true when evaluated with pred.
 */
struct AnyPredicate : IPredicate, ReferenceCounted<AnyPredicate>, FastAllocated<AnyPredicate> {
	void addref() override { ReferenceCounted<AnyPredicate>::addref(); }
	void delref() override { ReferenceCounted<AnyPredicate>::delref(); }

	TypeCode getTypeCode() const override { return TypeCode::ANY; }

	Reference<IExpression> expr;
	Reference<IPredicate> pred;

	Future<bool> evaluate(Reference<IReadContext> const& context) override;
	Reference<IPredicate> simplify() override;

	AnyPredicate(Reference<IExpression> expr, Reference<IPredicate> pred) : expr(expr), pred(pred) {}

	std::string toString() override {
		return format("ANY(%s matching %s)", expr->toString().c_str(), pred->toString().c_str());
	}
	bool wantsNulls() override { return false; }
};

struct AllPredicate : IPredicate, ReferenceCounted<AllPredicate>, FastAllocated<AllPredicate> {
	void addref() override { ReferenceCounted<AllPredicate>::addref(); }
	void delref() override { ReferenceCounted<AllPredicate>::delref(); }

	TypeCode getTypeCode() const override { return TypeCode::ALL; }

	Future<bool> evaluate(Reference<IReadContext> const& context) override { return true; }
	std::string toString() override { return "ALL()"; }

	Reference<IPredicate> simplify_not() override;
	void simplify_and(SimplifyAndContext& combinable) override;
	bool wantsNulls() override { return false; }
};

struct NonePredicate : IPredicate, ReferenceCounted<NonePredicate>, FastAllocated<NonePredicate> {
	void addref() override { ReferenceCounted<NonePredicate>::addref(); }
	void delref() override { ReferenceCounted<NonePredicate>::delref(); }

	TypeCode getTypeCode() const override { return TypeCode::NONE; }

	Future<bool> evaluate(Reference<IReadContext> const& context) override { return false; }
	std::string toString() override { return "NONE()"; }

	Reference<IPredicate> simplify_not() override;
	void simplify_and(SimplifyAndContext& cx) override;
};

struct AndPredicate : IPredicate, ReferenceCounted<AndPredicate>, FastAllocated<AndPredicate> {
	void addref() override { ReferenceCounted<AndPredicate>::addref(); }
	void delref() override { ReferenceCounted<AndPredicate>::delref(); }

	TypeCode getTypeCode() const override { return TypeCode::AND; }

	std::vector<Reference<IPredicate>> terms;

	explicit AndPredicate(std::vector<Reference<IPredicate>> const& terms) : terms(terms) {}
	AndPredicate(Reference<IPredicate> const& term1, Reference<IPredicate> const& term2) : terms({term1, term2}) {}

	Future<bool> evaluate(Reference<IReadContext> const& context) override;
	Reference<IPredicate> simplify() override;
	std::string toString() override;

	Reference<IPredicate> simplify_not() override;
	void simplify_and(SimplifyAndContext& combinable) override;
};

struct OrPredicate : IPredicate, ReferenceCounted<OrPredicate>, FastAllocated<OrPredicate> {
	void addref() override { ReferenceCounted<OrPredicate>::addref(); }
	void delref() override { ReferenceCounted<OrPredicate>::delref(); }

	TypeCode getTypeCode() const override { return TypeCode::OR; }

	std::vector<Reference<IPredicate>> terms;

	explicit OrPredicate(std::vector<Reference<IPredicate>> const& terms) : terms(terms) {}

	Future<bool> evaluate(Reference<IReadContext> const& context) override;
	Reference<IPredicate> simplify() override;
	std::string toString() override;

	Reference<IPredicate> simplify_not() override;
};

struct BSONTypePredicate : IPredicate, ReferenceCounted<BSONTypePredicate>, FastAllocated<BSONTypePredicate> {
	void addref() override { ReferenceCounted<BSONTypePredicate>::addref(); }
	void delref() override { ReferenceCounted<BSONTypePredicate>::delref(); }

	TypeCode getTypeCode() const override { return TypeCode::BSON_TYPE; }

	bson::BSONType type;

	explicit BSONTypePredicate(bson::BSONType type) : type(type) {}

	Future<bool> evaluate(Reference<IReadContext> const& context) override;
	std::string toString() override;
	bool wantsNulls() override { return false; }
};

struct IsObjectPredicate : IPredicate, ReferenceCounted<IsObjectPredicate>, FastAllocated<IsObjectPredicate> {
	void addref() override { ReferenceCounted<IsObjectPredicate>::addref(); }
	void delref() override { ReferenceCounted<IsObjectPredicate>::delref(); }

	TypeCode getTypeCode() const override { return TypeCode::IS_OBJECT; }

	IsObjectPredicate() = default;

	Future<bool> evaluate(Reference<IReadContext> const& context) override;
	std::string toString() override;

	void get_range(Optional<DataValue>& min, Optional<DataValue>& max) override {
		min = DataValue::subObject();
		max = DataValue::arrayOfLength(std::numeric_limits<uint32_t>::max());
	}
};

struct EqPredicate : IPredicate, ReferenceCounted<EqPredicate>, FastAllocated<EqPredicate> {
	void addref() override { ReferenceCounted<EqPredicate>::addref(); }
	void delref() override { ReferenceCounted<EqPredicate>::delref(); }

	TypeCode getTypeCode() const override { return TypeCode::EQ; }

	DataValue value;

	explicit EqPredicate(DataValue const& value) : value(value) {}

	Future<bool> evaluate(Reference<IReadContext> const& context) override;
	// EqPredicate can't be simplified, so no simplify()
	std::string toString() override;

	void simplify_and(SimplifyAndContext& combinable) override;

	bool range_is_tight() override { return true; }

	void get_range(Optional<DataValue>& min, Optional<DataValue>& max) override {
		min = value;
		max = value;
	}
	// Not(Eq) could be translated to Or(Range,Range), but it's not
	// obvious that this is always a win without knowing about
	// available indexes, so no simplify_not() here.
};

struct NotPredicate : IPredicate, ReferenceCounted<NotPredicate>, FastAllocated<NotPredicate> {
	void addref() override { ReferenceCounted<NotPredicate>::addref(); }
	void delref() override { ReferenceCounted<NotPredicate>::delref(); }

	TypeCode getTypeCode() const override { return TypeCode::NOT; }

	Reference<IPredicate> term;

	explicit NotPredicate(Reference<IPredicate> const& term) : term(term) {}

	Future<bool> evaluate(Reference<IReadContext> const& context) override;
	Reference<IPredicate> simplify() override;
	std::string toString() override;

	Reference<IPredicate> simplify_not() override;
};

struct RangePredicate : IPredicate, ReferenceCounted<RangePredicate>, FastAllocated<RangePredicate> {
	void addref() override { ReferenceCounted<RangePredicate>::addref(); }
	void delref() override { ReferenceCounted<RangePredicate>::delref(); }

	TypeCode getTypeCode() const override { return TypeCode::RANGE; }

	DataValue min_value;
	bool min_closed;
	DataValue max_value;
	bool max_closed;

	RangePredicate(DataValue const& min_value, bool min_closed, DataValue const& max_value, bool max_closed)
	    : min_value(min_value), min_closed(min_closed), max_value(max_value), max_closed(max_closed) {}

	Future<bool> evaluate(Reference<IReadContext> const& context) override;
	Reference<IPredicate> simplify() override;
	std::string toString() override;
	bool range_is_tight() override { return min_closed && max_closed; }

	void get_range(Optional<DataValue>& min, Optional<DataValue>& max) override;
	// virtual Reference<IPredicate> simplify_not();
	void simplify_and(SimplifyAndContext& combinable) override;

	Reference<RangePredicate> intersect_with(Reference<RangePredicate> const& ref_other);
};

struct ArraySizePredicate : IPredicate, ReferenceCounted<ArraySizePredicate>, FastAllocated<ArraySizePredicate> {
	void addref() override { ReferenceCounted<ArraySizePredicate>::addref(); }
	void delref() override { ReferenceCounted<ArraySizePredicate>::delref(); }

	TypeCode getTypeCode() const override { return TypeCode::ARRAY_SIZE; }

	Future<bool> evaluate(Reference<IReadContext> const& context) override;
	std::string toString() override;

	// Is this correct?
	void get_range(Optional<DataValue>& min, Optional<DataValue>& max) override {
		DataValue arrayHeader = DataValue::arrayOfLength(size);
		min = arrayHeader;
		max = arrayHeader;
	}

	uint32_t size;

	explicit ArraySizePredicate(uint32_t size) : size(size) {}
};

// RegExPredicate(expr, pred) to be defined ...
struct RegExPredicate : IPredicate, ReferenceCounted<RegExPredicate>, FastAllocated<RegExPredicate> {
	void addref() override { ReferenceCounted<RegExPredicate>::addref(); }
	void delref() override { ReferenceCounted<RegExPredicate>::delref(); }

	TypeCode getTypeCode() const override { return TypeCode::REGEX; }

	RegExPredicate(const std::string& _pattern, const std::string& _options);

	Future<bool> evaluate(Reference<IReadContext> const& context) override;

	std::string toString() override;

	bool range_is_tight() override;

	void get_range(Optional<DataValue>& min, Optional<DataValue>& max) override;

	virtual void setOptions(const std::string& _options);

private:
	void calculateRange();

private:
	pcrecpp::RE_Options options;
	std::shared_ptr<pcrecpp::RE> re;
	std::string min_limit;
	std::string max_limit;
};

// Distinct Predicate
struct DistinctPredicate : IPredicate, ReferenceCounted<DistinctPredicate>, FastAllocated<DistinctPredicate> {
	void addref() override { ReferenceCounted<DistinctPredicate>::addref(); }
	void delref() override { ReferenceCounted<DistinctPredicate>::delref(); }

	explicit DistinctPredicate(const std::string& fieldName);
	TypeCode getTypeCode() const override { return TypeCode::DISTINCT; }
	std::string toString() override { return format("(DISTINCT BY : %s)", _fieldName.c_str()); }
	Future<bool> evaluate(Reference<IReadContext> const& context) override;
	bool wantsNulls() override { return false; }

	void collectDataValues(bson::BSONArrayBuilder& arrayBuilder);

private:
	std::string _fieldName;
	std::set<DataValue> _distinct;
};

#endif /* _QL_PREDICATE_H_ */
