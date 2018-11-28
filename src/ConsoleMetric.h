/*
 * ConsoleMetric.h
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

#ifndef FDB_DOC_LAYER_CONSOLEMETRIC_H
#define FDB_DOC_LAYER_CONSOLEMETRIC_H

#include <IMetric.h>

#include <ctime>
#include <iostream>
#include <string>

#include "flow/Arena.h"
#include "flow/ThreadPrimitives.h"
#include "flow/Trace.h"
#include "flow/flow.h"

#include <boost/dll.hpp>
#include <boost/filesystem.hpp>
#include <boost/function.hpp>

// Hold the aggregated statistics for the `mId`
struct MetricStat : NonCopyable {
	std::string mId;
	IMetricType mType;
	std::string typeName;
	int64_t count, sum, min, max;
	int64_t percentile25;
	int64_t percentile50;
	int64_t percentile90;
	int64_t percentile99;
	int64_t percentile9999;
	double avg;
	bool hasNewData = false;
	std::vector<int64_t> values; // hold all values logged for the given id

	MetricStat() = default;
	MetricStat(std::string mId, IMetricType mType);
	// Move ctor
	MetricStat(MetricStat&& other) noexcept;
	// Move operator
	MetricStat& operator=(MetricStat&& other) noexcept;

	MetricStat& captureNewValue(int64_t val);
	// Reset all statistics, including forwarding the `startTimeSeconds` to the time calling this.
	// This means that the aggregated running avg and rate are calculated over the `flushIntervalSeconds`
	void reset();

private:
	uint64_t startTimeNanoSeconds;
};

// ConsoleMetric will aggregate the metric points, and periodically `flush` them into one/multiple `TraceEvent`s
class ConsoleMetric : public IMetricReporter, public IMetricReporterFactory<ConsoleMetric> {
public:
	// ctor
	explicit ConsoleMetric(std::string& config);

	// Since the metric reporter will have a static lifetime, it's OK to simply use the default dtor
	~ConsoleMetric() override = default;

	void captureMetric(const std::string& metricName, int64_t metricValue, IMetricType metricType) override;
	void publish();

	// factory creator
	static ConsoleMetric* CreatPluginImpl(std::string& config) { return new ConsoleMetric(config); }

	int64_t flushIntervalSeconds = 5; // default to aggregate and publish metrics over 5 seconds.
private:
	std::string config;
	std::unordered_map<std::string, MetricStat> metricStats;
	Future<Void> publisherHandle;
};

BOOST_DLL_ALIAS(ConsoleMetric::CreatPlugin, // <-- this function is exported with...
                CreatPlugin // <-- ...this alias name
)

#endif // FDB_DOC_LAYER_CONSOLEMETRIC_H
