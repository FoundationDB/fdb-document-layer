/*
 * ConsoleMetric.actor.cpp
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

#include "ConsoleMetric.h"

MetricStat::MetricStat(std::string mId, IMetricType mType)
    : mId(std::move(mId)),
      mType(mType),
      sum(0),
      avg(0.0),
      count(0),
      max(std::numeric_limits<int64_t>::min()),
      min(std::numeric_limits<int64_t>::max()),
      values(std::vector<int64_t>()),
      percentile25(0),
      percentile50(0),
      percentile90(0),
      percentile99(0),
      percentile9999(0) {
	startTimeNanoSeconds = timer_int();
	switch (mType) {
	case IMetricType::COUNT:
		typeName = std::string("COUNT");
		break;
	case IMetricType::TIMER:
		typeName = std::string("TIMER");
		break;
	case IMetricType::GAUGE:
		typeName = std::string("GAUGE");
		break;
	case IMetricType::METER:
		typeName = std::string("METER (rate per second)");
		break;
	case IMetricType::HISTOGRAMS:
		typeName = std::string("HISTOGRAMS");
		break;
	}
}

MetricStat::MetricStat(MetricStat&& other) noexcept
    : mId(std::move(other.mId)),
      mType(other.mType),
      typeName(std::move(other.typeName)),
      sum(other.sum),
      avg(other.avg),
      count(other.count),
      max(other.max),
      min(other.min),
      values(std::move(other.values)),
      percentile25(other.percentile25),
      percentile50(other.percentile50),
      percentile90(other.percentile90),
      percentile99(other.percentile99),
      percentile9999(other.percentile9999),
      startTimeNanoSeconds(other.startTimeNanoSeconds) {}

MetricStat& MetricStat::operator=(MetricStat&& other) noexcept {
	mId = std::move(other.mId);
	mType = other.mType;
	typeName = std::move(other.typeName);
	sum = other.sum;
	avg = other.avg;
	count = other.count;
	max = other.max;
	min = other.min;
	values = std::move(other.values);
	percentile25 = other.percentile25;
	percentile50 = other.percentile50;
	percentile90 = other.percentile90;
	percentile99 = other.percentile99;
	percentile9999 = other.percentile9999;
	startTimeNanoSeconds = other.startTimeNanoSeconds;
	return *this;
}

MetricStat& MetricStat::captureNewValue(int64_t val) {
	hasNewData = true;
	switch (mType) {
	case IMetricType::GAUGE:
		// For gauge, we only care the current value
		count = 1;
		sum = val;
		avg = (double)val;
		max = val;
		min = val;
		percentile25 = val;
		percentile50 = val;
		percentile90 = val;
		percentile99 = val;
		percentile9999 = val;
		return *this;
	default:
		// For all non-gauge types, we need to do some bookkeeping
		values.emplace_back(val);
		max = val > max ? val : max;
		min = val < min ? val : min;
		switch (mType) {
		case IMetricType::COUNT:
			count += val;
			sum += val;
			avg = (double)sum / (double)count;
			return *this;
		case IMetricType::TIMER:
			count += 1;
			sum += val;
			avg = (double)sum / (double)count;
			return *this;
		case IMetricType::GAUGE:
			// This is actually non-reachable code. Keep this case match branch here so that we don't need to
			// use `default`, which means that compiler will remind us in the future
			// this became a non-exhaustive match.
			return *this;
		case IMetricType::METER: {
			uint64_t currentTimeNanoSeconds = timer_int();
			count += 1;
			sum += val;
			avg =
			    (sum * 1e9) / (double)(currentTimeNanoSeconds -
			                           startTimeNanoSeconds); // calculate running avg rate while keeping the precision
			return *this;
		}
		case IMetricType::HISTOGRAMS:
			count += 1;
			sum += val;
			avg = (double)sum / (double)count;
			return *this;
		}
	}
}

void MetricStat::reset() {
	sum = 0;
	avg = 0.0;
	count = 0;
	max = std::numeric_limits<int64_t>::min();
	min = std::numeric_limits<int64_t>::max();
	values.clear();
	percentile25 = 0;
	percentile50 = 0;
	percentile90 = 0;
	percentile99 = 0;
	percentile9999 = 0;
	startTimeNanoSeconds = timer_int();
	hasNewData = false;
}

ACTOR static Future<Void> publisher(ConsoleMetric* self) {
	loop {
		Void _ = wait(delay(self->flushIntervalSeconds));
		self->publish();
	}
}

ConsoleMetric::ConsoleMetric(const char* config) : IMetricReporter(config) {
	// Start the publishing loop
	publisherHandle = publisher(this);
}

void ConsoleMetric::captureMetric(const char* metricName, int64_t metricValue, IMetricType metricType) {
	if (metricStats.find(metricName) == metricStats.end()) {
		// insert new stat
		auto metric = MetricStat(metricName, metricType);
		metric.captureNewValue(metricValue);
		metricStats[metricName] = std::move(metric);
	} else {
		// update stat
		metricStats[metricName].captureNewValue(metricValue);
	}
}

void ConsoleMetric::publish() {
	for (auto& metricStat : metricStats) {
		if (metricStat.second.hasNewData) {
			switch (metricStat.second.mType) {
			case IMetricType::GAUGE:
				// For gauge, we only care the current value
				TraceEvent(SevInfo, "ConsoleMetric")
				    .detail("MetricId", metricStat.second.mId)
				    .detail("MetricType", metricStat.second.typeName)
				    .detail("Value", metricStat.second.avg);
				break;
			default:
				// sort the values to calculate percentiles
				std::sort(metricStat.second.values.begin(), metricStat.second.values.end());
				metricStat.second.percentile25 = metricStat.second.values[(metricStat.second.count * 25) / 100];
				metricStat.second.percentile50 = metricStat.second.values[(metricStat.second.count * 50) / 100];
				metricStat.second.percentile90 = metricStat.second.values[(metricStat.second.count * 90) / 100];
				metricStat.second.percentile99 = metricStat.second.values[(metricStat.second.count * 99) / 100];
				metricStat.second.percentile9999 = metricStat.second.values[(metricStat.second.count * 9999) / 10000];
				TraceEvent(SevInfo, "ConsoleMetric")
				    .detail("MetricId", metricStat.second.mId)
				    .detail("MetricType", metricStat.second.typeName)
				    .detail("Count", metricStat.second.count)
				    .detail("Sum", metricStat.second.sum)
				    .detail("Avg", metricStat.second.avg)
				    .detail("Max", metricStat.second.max)
				    .detail("Min", metricStat.second.min)
				    .detail("Top25%", metricStat.second.percentile25)
				    .detail("Top50%", metricStat.second.percentile50)
				    .detail("Top90%", metricStat.second.percentile90)
				    .detail("Top99%", metricStat.second.percentile99)
				    .detail("Top99.99%", metricStat.second.percentile9999);
			}
			metricStat.second.reset();
		}
	}
}
