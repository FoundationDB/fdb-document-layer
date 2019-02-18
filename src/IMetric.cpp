/*
 * IMetric.cpp
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

#include "IMetric.h"

#include <boost/dll.hpp>
#define BOOST_NO_CXX11_SCOPED_ENUMS
#include <boost/filesystem.hpp>
#undef BOOST_NO_CXX11_SCOPED_ENUMS
#include <boost/function.hpp>

static boost::dll::shared_library _loadedLib;

IMetricReporter* IMetricReporter::init(const char* libPath, const char* libConfig) {
	typedef IMetricReporter*(creator_t)(const char*);

	// Code from Boost::DLL::import_alias begin
	typedef typename boost::dll::detail::import_type<creator_t>::base_type type;
	// Load the lib and bind it to the static symbol to make sure the loaded lib's lifetime is long enough.
	auto lib = boost::make_shared<boost::dll::shared_library>(libPath, boost::dll::load_mode::append_decorations);
	auto creator = type(lib, lib->get<creator_t*>(pluginCreatorName));
	IMetricReporter* reporter = creator(libConfig);
	// Code from Boost::DLL::import_alias end.

	_loadedLib = std::move(*lib);
	return reporter;
}

void IMetricReporter::captureCount(const char* metricName) {
	captureMetric(metricName, 1, IMetricType::COUNT);
}
void IMetricReporter::captureTime(const char* metricName, int64_t metricValue) {
	captureMetric(metricName, metricValue, IMetricType::TIMER);
}
void IMetricReporter::captureGauge(const char* metricName, int64_t metricValue) {
	captureMetric(metricName, metricValue, IMetricType::GAUGE);
}
void IMetricReporter::captureMeter(const char* metricName, int64_t metricValue) {
	captureMetric(metricName, metricValue, IMetricType::METER);
}
void IMetricReporter::captureHistogram(const char* metricName, int64_t metricValue) {
	captureMetric(metricName, metricValue, IMetricType::HISTOGRAMS);
}
