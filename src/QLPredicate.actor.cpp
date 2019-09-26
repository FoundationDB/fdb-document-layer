/*
 * QLPredicate.actor.cpp
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

#include "ExtUtil.actor.h"

#include "QLExpression.h"
#include "QLPredicate.h"
#include "flow/UnitTest.h"
#include "flow/actorcompiler.h" // This must be the last #include.

ACTOR static Future<bool> evaluateAnyPredicate(Reference<IReadContext> cx,
                                               Reference<IExpression> expr,
                                               Reference<IPredicate> pred) {
	state GenFutureStream<Reference<IReadContext>> paths = expr->evaluate(cx);
	try {
		loop {
			Reference<IReadContext> scx = waitNext(paths);
			bool result = wait(pred->evaluate(scx));
			if (result) {
				return result;
			}
		}
	} catch (Error& e) {
		if (e.code() == error_code_end_of_stream) {
			return false;
		}
		throw;
	}
}

Future<bool> AnyPredicate::evaluate(const Reference<IReadContext>& cx) {
	return evaluateAnyPredicate(cx, expr, pred);
}

Reference<IPredicate> AnyPredicate::simplify() {
	Reference<IPredicate> s = pred->simplify();
	if (s->getTypeCode() == IPredicate::NONE)
		return s;
	return ref(new AnyPredicate(expr, s));
}

Reference<IPredicate> IPredicate::simplify_not() {
	return ref(new NotPredicate(Reference<IPredicate>::addRef(this)));
}

Reference<IPredicate> AllPredicate::simplify_not() {
	return ref(new NonePredicate);
}

void AllPredicate::simplify_and(SimplifyAndContext& combinable) {}

Reference<IPredicate> NonePredicate::simplify_not() {
	return ref(new AllPredicate);
}

void NonePredicate::simplify_and(SimplifyAndContext& cx) {
	cx.rangeLike.clear();
	cx.other.clear();
	cx.other.push_back(Reference<IPredicate>::addRef(this));
	cx.isNone = true;
}

ACTOR static Future<bool> doAndEvaluate(std::vector<Reference<IPredicate>> terms, Reference<IReadContext> context) {
	// Short-circuit, could be parallel instead?

	for (const auto& term : terms) {
		bool et = wait(term->evaluate(context));
		if (!et)
			return false;
	}

	return true;
}

Future<bool> AndPredicate::evaluate(Reference<IReadContext> const& context) {
	return doAndEvaluate(terms, context);
}

void AndPredicate::simplify_and(SimplifyAndContext& cx) {
	for (const auto& term : this->terms) {
		auto s = term->simplify();
		s->simplify_and(cx);
		if (cx.isNone)
			break;
	}
}

Reference<IPredicate> AndPredicate::simplify() {
	SimplifyAndContext combine;

	simplify_and(combine);

	if (combine.rangeLike) {
		// The result of range simplification could be a degenerate range, so simplify again
		auto r = combine.rangeLike->simplify();
		if (r->getTypeCode() == TypeCode::NONE)
			return r;
		combine.other.push_back(r);
	}

	// Nothing left matches everything
	if (combine.other.empty()) {
		return ref(new AllPredicate);
	}

	// AND of one term is just that term
	if (combine.other.size() == 1) {
		return combine.other[0];
	}

	return ref(new AndPredicate(combine.other));
}

std::string AndPredicate::toString() {
	std::string s = "AND(";
	int addcomma = 0;
	for (const auto& t : terms) {
		if (addcomma++)
			s += ", ";
		s += t->toString();
	}
	s += ")";
	return s;
}

Reference<IPredicate> AndPredicate::simplify_not() {
	std::vector<Reference<IPredicate>> terms;

	for (const auto& term : this->terms) {
		terms.emplace_back(new NotPredicate(term));
	}

	return OrPredicate(terms).simplify();
}

ACTOR Future<bool> doOrEvaluate(std::vector<Reference<IPredicate>> terms, Reference<IReadContext> context) {
	state std::vector<Future<bool>> ets;
	for (const auto& term : terms) {
		Future<bool> et = term->evaluate(context);
		ets.push_back(et);
	}
	bool any = wait(shortCircuitAny(ets));
	return any;
}

Future<bool> OrPredicate::evaluate(Reference<IReadContext> const& context) {
#if 0
	// Short-circuit, could be parallel instead?

	for ( auto t : terms ) {
		bool et = wait( t->evaluate( context ) );
		if (et) return true;
	}

	return false;
#endif

	// or ...

	return doOrEvaluate(terms, context);
}

Reference<IPredicate> OrPredicate::simplify() {
	std::vector<Reference<IPredicate>> terms;

	for (auto& term : this->terms) {
		auto s = term->simplify();

		// SOMEDAY: should this be structured more like
		// AndPredicate/simplify_and (which would cover NONE and also
		// union ranges)?
		switch (s->getTypeCode()) {
		case TypeCode::ALL:
			return s;
		case TypeCode::NONE:
			break;
		default:
			terms.push_back(s);
		}
	}

	// Nothing left matches nothing
	if (terms.empty()) {
		return ref(new NonePredicate);
	}

	// OR of one term is just that term
	if (terms.size() == 1) {
		return terms[0];
	}

	return ref(new OrPredicate(terms));
}

std::string OrPredicate::toString() {
	std::string s = "OR(";
	int addcomma = 0;
	for (const auto& term : terms) {
		if (addcomma++)
			s += ", ";
		s += term->toString();
	}
	s += ")";
	return s;
}

Reference<IPredicate> OrPredicate::simplify_not() {
	std::vector<Reference<IPredicate>> terms;

	for (const auto& term : this->terms) {
		terms.emplace_back(new NotPredicate(term));
	}

	return AndPredicate(terms).simplify();
}

Future<bool> BSONTypePredicate::evaluate(Reference<IReadContext> const& context) {
	Future<Optional<DataValue>> fdv = context->get(StringRef());
	bson::BSONType myType = type;
	return map(fdv, [myType](Optional<DataValue> dv) { return dv.get().getBSONType() == myType; });
}

std::string BSONTypePredicate::toString() {
	return format("TYPE_OF(%d)", (int)type);
}

Future<bool> IsObjectPredicate::evaluate(Reference<IReadContext> const& context) {
	Future<Optional<DataValue>> fdv = context->get(StringRef());
	return map(fdv, [](Optional<DataValue> dv) {
		return dv.get().getBSONType() == bson::BSONType::Object || dv.get().getBSONType() == bson::BSONType::Array;
	});
}

std::string IsObjectPredicate::toString() {
	return "IS_OBJECT()";
}

Future<bool> ArraySizePredicate::evaluate(Reference<IReadContext> const& context) {
	Future<Optional<DataValue>> fdv = context->get(StringRef());
	uint32_t mySize = size;
	return map(fdv,
	           [mySize](Optional<DataValue> dv) { return dv.get().compare(DataValue::arrayOfLength(mySize)) == 0; });
}

std::string ArraySizePredicate::toString() {
	return format("ARRAY_SIZE(%d)", size);
}

Future<bool> EqPredicate::evaluate(Reference<IReadContext> const& context) {
	DataValue v = value;
	Future<DataValue> fdv = getMaybeRecursive(context, StringRef());
	return map(fdv, [v](DataValue dv) { return dv.compare(v) == 0; });
}

std::string EqPredicate::toString() {
	return format("EQUALS('%s')", value.toString().c_str());
}

/**
 * x == 3 ==> 3 <= x <= 3, combine with other ranges.
 * This has the charming property that we don't have to worry
 * about intersecting equality with ranges, conflicting
 * equalities, etc. since the final intersected range will be
 * simplified after all and terms are combined, which could reduce
 * it back to an equality or a none. This may not be the most
 * efficient, however?
 */
void EqPredicate::simplify_and(SimplifyAndContext& cx) {
	auto range_equiv = ref(new RangePredicate(value, true, value, true));
	if (cx.rangeLike)
		cx.rangeLike = range_equiv->intersect_with(cx.rangeLike);
	else
		cx.rangeLike = range_equiv;
}

ACTOR Future<bool> doNotEvaluate(Reference<IPredicate> term, Reference<IReadContext> context) {
	bool et = wait(term->evaluate(context));
	return !et;
}

Future<bool> NotPredicate::evaluate(Reference<IReadContext> const& context) {
	return doNotEvaluate(term, context);
}

Reference<IPredicate> NotPredicate::simplify() {
	auto t = term->simplify();
	return t->simplify_not();
}

std::string NotPredicate::toString() {
	return "!" + term->toString();
}

Reference<IPredicate> NotPredicate::simplify_not() {
	return term;
}

ACTOR static Future<bool> doRangeEvaluate(Future<DataValue> fdv,
                                          DataValue min_value,
                                          DataValue max_value,
                                          bool min_closed,
                                          bool max_closed) {
	DataValue v = wait(fdv);

	if (v.compare(min_value) <= (min_closed ? -1 : 0))
		return false;

	return v.compare(max_value) < (max_closed ? 1 : 0);
}

Future<bool> RangePredicate::evaluate(Reference<IReadContext> const& context) {
	Future<DataValue> fdv = getMaybeRecursive(context, StringRef());
	return doRangeEvaluate(fdv, min_value, max_value, min_closed, max_closed);
}

Reference<IPredicate> RangePredicate::simplify() {
	// Closed on both ends with min == max is equality
	if (min_closed && max_closed && min_value.compare(max_value) == 0) {
		return ref(new EqPredicate(min_value));
	}

	// Unsatisfiable range
	if (min_value.compare(max_value) >= 0) {
		return ref(new NonePredicate);
	}

	return Reference<IPredicate>::addRef(this);
}

std::string RangePredicate::toString() {
	std::string s = "RANGE(";

	s += format("%s <", min_value.toString().c_str());
	if (min_closed) {
		s += "=";
	}
	s += " x <";
	if (max_closed) {
		s += "=";
	}
	s += " " + max_value.toString();
	s += ")";

	return s;
}

void RangePredicate::get_range(Optional<DataValue>& min, Optional<DataValue>& max) {
	min = min_value;
	max = max_value;
}

// Commented out, because this breaks semantics of comparing vs. null elements. Maybe SOMEDAY we can restore something
// like it?

/* Reference<IPredicate> RangePredicate::simplify_not() {
    // Range unbounded on the lower end ( -inf < x < 3 ) => (3 <= x < inf )
    if ( !min_set ) {
        //return ref( new RangePredicate(max_value, !max_closed, path ) );
        return ref( new RangePredicate( max_value, !max_closed, path ) );
    }
    // Range unbounded on the upper end ( 3 < x < inf ) => (-inf < x <= 3 )
    if ( !max_set ) {
        return ref( new RangePredicate( path, min_value, !min_closed ) );
    }
    // Range bounded on both ends negates to two ranges, each bounded on only one end
    // ( 3 < x < 5 ) => ( ( x <= 3 ) || ( x >= 5 ) )
    std::vector< Reference<IPredicate> > terms;

    terms.push_back( ref( new RangePredicate( path, min_value, !min_closed ) ) );
    terms.push_back( ref( new RangePredicate( max_value, !max_closed, path ) ) );

    return ref( new OrPredicate( terms ) );
} */

void RangePredicate::simplify_and(SimplifyAndContext& cx) {
	if (cx.rangeLike)
		cx.rangeLike = intersect_with(cx.rangeLike);
	else
		cx.rangeLike = Reference<RangePredicate>::addRef(this);
}

Reference<RangePredicate> RangePredicate::intersect_with(Reference<RangePredicate> const& other) {
	DataValue new_min_value;
	bool new_min_closed;
	DataValue new_max_value;
	bool new_max_closed;

	int cmp = min_value.compare(other->min_value);
	if (cmp < 0) {
		new_min_value = other->min_value;
		new_min_closed = other->min_closed;
	} else if (cmp > 0) {
		new_min_value = min_value;
		new_min_closed = min_closed;
	} else {
		new_min_value = min_value;
		new_min_closed = (min_closed && other->min_closed);
	}

	cmp = max_value.compare(other->max_value);
	if (cmp < 0) {
		new_max_value = max_value;
		new_max_closed = max_closed;
	} else if (cmp > 0) {
		new_max_value = other->max_value;
		new_max_closed = other->max_closed;
	} else {
		new_max_value = max_value;
		new_max_closed = (max_closed && other->max_closed);
	}

	return ref(new RangePredicate(new_min_value, new_min_closed, new_max_value, new_max_closed));
}

RegExPredicate::RegExPredicate(const std::string& _pattern, const std::string& _options) {
	re.reset(new pcrecpp::RE(_pattern));
	setOptions(_options);
}

void RegExPredicate::calculateRange() {
	max_limit = "";
	min_limit = "";
	// we do not support calculating ranges if 'm' and 'x' are specified
	if (options.multiline() || options.extended())
		return;

	// remove the special "^" if exist in regexPattern
	const auto pattern = re->pattern();
	// we don't want to calculate any limits if we don't have ^ or \A at the beginning of the pattern
	const bool startChevron = (!pattern.empty() && pattern.at(0) == '^');
	const bool startString = (pattern.size() > 1 && (pattern.at(0) == '\\' && pattern.at(1) == 'A'));
	if (startChevron || startString) {
		auto sValue = pattern.substr(startChevron ? 1 : (startString ? 2 : 0));
		const static std::string sSpecial = ".\\[]()*?+|,${}!="; // define an array with the special chars
		for (int ii = 0; ii < sValue.length(); ++ii) {
			if (sSpecial.find(sValue[ii]) != std::string::npos) {
				// We need to take special care for ? and * cases
				const int oneLess = (sValue[ii] == '?' || sValue[ii] == '*') ? 1 : 0;
				sValue = sValue.substr(0, ii - oneLess);
				break;
			}
		}

		if (sValue.length() > 0) {
			max_limit = sValue;
			min_limit = sValue;

			if (options.caseless()) {
				std::transform(max_limit.begin(), max_limit.end(), max_limit.begin(), ::tolower);
				std::transform(min_limit.begin(), min_limit.end(), min_limit.begin(), ::toupper);
			}
		}
	}
}

void RegExPredicate::setOptions(const std::string& _options) {
	// According to http://docs.mongodb.org/manual/reference/operator/query/regex/
	// Options can be only "i", "x", "m" and "s"
	options = pcrecpp::RE_Options();
	options.set_caseless(_options.find('i') != std::string::npos);
	options.set_multiline(_options.find('m') != std::string::npos);
	options.set_extended(_options.find('x') != std::string::npos);
	options.set_dotall(_options.find('s') != std::string::npos);

	const auto& pattern = re->pattern();
	re.reset(new pcrecpp::RE(pattern, options));

	// we need to recalculate this after each change of options
	calculateRange();
}

Future<bool> RegExPredicate::evaluate(const Reference<IReadContext>& context) {
	Future<DataValue> fdv = getMaybeRecursive(context, StringRef());
	return map(fdv, [this](DataValue dv) {
		if (dv.getBSONType() == bson::BSONType::String) {
			return re->PartialMatch(dv.getString());
		}
		return false;
	});
}

std::string RegExPredicate::toString() {
	return format("REGEX(matching '%s')", re ? re->pattern().c_str() : "Regex not specified");
}

void RegExPredicate::get_range(Optional<DataValue>& min, Optional<DataValue>& max) {
	if (min_limit.length() == 0 || max_limit.length() == 0)
		return;
	min = DataValue(min_limit);
	max = DataValue(strinc(max_limit).toString()); // This needs to be handled
}

bool RegExPredicate::range_is_tight() {
	// the range is tight only if it's min_level == max_level
	// return min_limit != "" && min_limit == max_limit;
	// FIXME : The above code is commented out, because there is a imperfection in our code where max range is
	// incremented after encoding leading to incorrect result
	return false;
}

// Distinct Predicate
DistinctPredicate::DistinctPredicate(const std::string& fieldName) : _fieldName(fieldName) {}

Future<bool> DistinctPredicate::evaluate(Reference<IReadContext> const& context) {
	Future<DataValue> fdv = getMaybeRecursive(context, StringRef());
	return map(fdv, [this](DataValue dv) {
		switch (dv.getBSONType()) {
		case bson::BSONType::Array: {
			// Since MongoDB allows same field having different data type, and once we get an array when processing
			// a distinct operator on this field, we need to `unpack` all elements out of the array and then insert
			// into the set.
			bson::BSONArray _array = std::move(dv.getPackedArray());
			for (auto it = _array.begin(); it.more();) {
				auto elm = it.next();
				switch (elm.type()) {
				case bson::BSONType::Array:
					_distinct.insert(DataValue(bson::BSONArray(elm.Obj())));
					break;
				case bson::BSONType::Object:
					_distinct.insert(DataValue(elm.Obj()));
					break;
				default:
					_distinct.insert(DataValue(elm));
				}
			}
			// always return true for array fields.
			// FIXME: audit if this is the correct behavior
			return true;
		}
		default:
			if (_distinct.find(dv) == _distinct.end()) {
				_distinct.insert(dv);
				return true;
			}
			return false;
		}
	});
}

void DistinctPredicate::collectDataValues(bson::BSONArrayBuilder& arrayBuilder) {
	for (const DataValue& itCur : _distinct) {
		arrayBuilder << itCur.wrap("temp_fdb_field").getField("temp_fdb_field");
	}
}
