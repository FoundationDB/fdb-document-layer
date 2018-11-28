/*
 * IMetric.h
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

#ifndef FDB_DOC_LAYER_IMETRIC_H
#define FDB_DOC_LAYER_IMETRIC_H

#include <string>
#include <unordered_map>

enum class IMetricType {
	COUNT, // Measures how many times an event appears. E.g. total requests.
	TIMER, // Measures how fast an event happens/got processed. E.g. request processing time.
	GAUGE, // Measures an instantaneous value. E.g. show the current queue size.
	METER, // Measures the rate of a certain kind of events over time. E.g. requests per second.
	HISTOGRAMS // Measures the statistical distribution of values in a stream of data.
};

/**
 * All concrete metric reporter class need to implement this interface.
 */
class IMetricReporter {
public:
	explicit IMetricReporter(std::string& config) : config(config){};
	// Move ctor
	IMetricReporter(IMetricReporter&& reporter) noexcept : config(std::move(reporter.config)){};
	IMetricReporter() = delete;
	virtual ~IMetricReporter() = default;

	virtual void captureMetric(const std::string& metricName, int64_t metricValue, IMetricType metricType) = 0;

	void captureCount(const std::string& metricName);
	void captureTime(const std::string& metricName, int64_t metricValue);
	void captureGauge(const std::string& metricName, int64_t metricValue);
	void captureMeter(const std::string& metricName, int64_t metricValue);
	void captureHistogram(const std::string& metricName, int64_t metricValue);
	/**
	 * Load the dylib and call the static creator function defined to get a reference to the plugin.
	 */
	static IMetricReporter* init(const char* libPath, const std::string& libConfig);

protected:
	std::string config;

private:
	static constexpr auto pluginCreatorName = "CreatPlugin";
};

/**
 * All concrete metric reporter class need to implement this factory interface.
 * @tparam MetricReportImpl The concrete reporter class type
 * @tparam MetricReporterInterface The interface type.
 */
template <class MetricReportImpl>
class IMetricReporterFactory {
public:
	static MetricReportImpl* CreatPlugin(std::string& config) { return MetricReportImpl::CreatPluginImpl(config); }
};

#endif // FDB_DOC_LAYER_IMETRIC_H
