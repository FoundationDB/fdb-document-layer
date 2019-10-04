/*
 * HostInfo.h
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

#pragma once

#include "bson.h"

namespace diagnostics {
class HOSTINFO {
public:
	HOSTINFO() {}
	~HOSTINFO() {}

	// returns bson object with physical system information
	bson::BSONObj getSystemInfo();

	// returns bson object with Operating System information
	bson::BSONObj getOsInfo();

	// returns bson object with CPU,kernel related information
	bson::BSONObj getExtraInfo();

private:
	// returns 64 or 32 bit machine
	int getCpuAddressSize();
	// returns total memory of machine
	int getMemSizeMB();
	// returns total number of CPU cores in this machine
	int getNumCores();
	// returns system supports NUMA or not
	bool checkNumaEnabled();
	// returns kernel version
	std::string getVersionString();
	// returns supported features of this CPU
	std::string getCpuFeatures();
#if defined(__linux__)
	// returns matching string value from given path
	std::string readFromFile(std::string findName, std::string delimiter, std::string filePath);
	// returns a line from a file
	std::string readLineFromFile(const char* fileName);
	// returns OS name and version
	void getOsRelease(std::string& osName, std::string& osVersion);
	// returns linux kernel version and upstream version
	std::string getVersionSignature();
	// returns dynamic cpu frequency
	std::string getCpuFrequencyMHZ();
	// returns version of glibc
	std::string getLibcVersion();
	// returns number of pages of physical memory
	int getNumPages();
	// returns maximum number of files a process can open
	int getMaxOpenFiles();
	// returns size of a page in bytes
	long long getPageSize();
	// returns cgroup memory limit
	long long getMemLimitMB();
#elif defined(__APPLE__)
	// returns kernel information of Mac OS in string format
	std::string getSysctlByName(const char* sysctlName);
	// returns kernel information of Mac OS in long long format
	long long getSysctlByValue(const char* sysctlName);
	// returns OS version
	std::string getOsRelease();
	// returns VFS related information
	int getAlwaysFullSync();
	int getNfsAsync();
	// returns Mac Model number
	std::string getModel();
	// returns physical core count
	int getPhysicalCores();
	// returns dynamic cpu frequency
	int getCpuFrequencyMHZ();
	// returns CPU information
	std::string getCpuString();
	// returns size of a page in bytes
	int getPageSize();
	// returns OS scheduler information
	std::string getScheduler();
#endif
};
} // namespace diagnostics
