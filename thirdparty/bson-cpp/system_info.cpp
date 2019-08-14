/*
 * system_info.cpp
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

#include <iostream>
#include "system_info.h"
#include <sys/utsname.h>

#if defined(__linux__)
#include <string>
#include <fstream>
#include <gnu/libc-version.h>
#include <unistd.h>
#include <boost/algorithm/string/trim.hpp>
#include <boost/filesystem.hpp>
#include <bson.h>
#elif defined(__APPLE__)
#include <sys/mman.h>
#include <sys/sysctl.h>
#include <sys/types.h>
#include <netdb.h>
#endif


namespace bson {

#if defined(__linux__)

	// Read the file and returns matching paramater value
	std::string HOSTINFO::readFromFile(std::string findParam, std::string delimiter, std::string filePath) {
		std::string paramValue;
		std::string key;
		std::ifstream fin;
		fin.open(filePath);

		if(fin.good()) {
			while(getline(fin, paramValue)) {
				size_t pos = 0;
				pos = paramValue.find(delimiter);
				if (pos != std::string::npos) {
					key = paramValue.substr(0, pos);
					if(key.compare(findParam) == 0) {
						paramValue.erase(0, pos + delimiter.length());
						break;
					}
				}
			}
		}

		fin.close();
		// returns parameter value or empty string
		return paramValue;
	}

	std::string HOSTINFO::readLineFromFile(const char* fileName) {
		FILE* f;
		char fstr[1024] = {0};

		f = fopen(fileName, "r");
		if (f != nullptr) {
			if (fgets(fstr, 1023, f) != nullptr) {
				// Assign NULL character at end of the string
				fstr[strlen(fstr) < 1 ? 0 : strlen(fstr) - 1] = '\0';
			}
			fclose(f);
		}
		return fstr;
    }

	int HOSTINFO::getCpuAddressSize() {
		std::string cpuAddressSize;
		cpuAddressSize = readFromFile("cache_alignment\t", ":", "/proc/cpuinfo");
		if(!cpuAddressSize.empty()) {
			return std::stol(cpuAddressSize, nullptr);
		}
		return -1;
	}

	int HOSTINFO::getMemSizeMB() {
		std::string memSizeMB;
		memSizeMB = readFromFile("MemTotal", ":", "/proc/meminfo");
		if(!memSizeMB.empty()) {
			std::string::size_type sz;
			long MB = std::stol(memSizeMB, &sz);
			MB = MB/1024;
			return MB;
		}
		return -1;
	}

	long long HOSTINFO::getMemLimitMB() {
		std::string cgmemlimit;
		long long memLimit;
		long long sysMemBytes = getMemSizeMB();
		if (sysMemBytes != -1) {
			cgmemlimit = readLineFromFile("/sys/fs/cgroup/memory/memory.limit_in_bytes");
			if (!cgmemlimit.empty()) {
				//Converting Megabytes to Bytes
				sysMemBytes = sysMemBytes * 1024 * 1024;
				memLimit = std::stoll(cgmemlimit, nullptr);
				memLimit = std::min(sysMemBytes, memLimit);
				return (memLimit/(1024*1024));
			}
		}
		return sysMemBytes;
	}

	int HOSTINFO::getNumCores() {
		return sysconf(_SC_NPROCESSORS_ONLN);
	}

	bool HOSTINFO::checkNumaEnabled() {
		//std::ifstream fin;
		std::string line;
		bool hasMultipleNodes = false;
		bool hasNumaMaps = false;
		hasMultipleNodes = boost::filesystem::exists("/sys/devices/system/node/node1");
		hasNumaMaps = boost::filesystem::exists("/proc/self/numa_maps");
		if (hasMultipleNodes && hasNumaMaps) {
			line = readLineFromFile("/proc/self/numa_maps");
			if (!line.empty()) {
				size_t pos = line.find(' ');
				if (pos != std::string::npos) {
					if (line.substr(pos + 1, 10).find("interleave") == std::string::npos) {
						return true;
					}
				}
			}
		}
		return false;
	}

	void HOSTINFO::getOsRelease(std::string& osName,std::string& osVersion) {
		if(boost::filesystem::exists("/etc/lsb-release")) {
			osName = readFromFile("DISTRIB_ID", "=", "/etc/lsb-release");
			osVersion = readFromFile("DISTRIB_RELEASE", "=", "/etc/lsb-release");
		} else if (boost::filesystem::exists("/etc/centos-release")) {
			osName = readLineFromFile("/etc/centos-release");
			std::string version = readLineFromFile("/proc/sys/kernel/osrelease");
			if (!version.empty()) {
				osVersion = "Kernel " + version;
			}
		} else {
			osName = readFromFile("ID", "=", "/etc/os-release");
			osVersion = readFromFile("VERSION_ID", "=", "/etc/os-release");
		}
	}

	std::string HOSTINFO::getVersionString() {
		return readLineFromFile("/proc/version");
	}

	std::string HOSTINFO::getVersionSignature() {
		return readLineFromFile("/proc/version_signature");
	}

	std::string HOSTINFO::getCpuFrequencyMHZ() {
		return readFromFile("cpu MHz\t\t", ":", "/proc/cpuinfo");
	}

	std::string HOSTINFO::getCpuFeatures() {
		return readFromFile("flags\t\t", ":", "/proc/cpuinfo");
	}

	std::string HOSTINFO::getLibcVersion() {
		return gnu_get_libc_version();
	}

	long long HOSTINFO::getPageSize() {
		return sysconf(_SC_PAGESIZE);
	}

	int HOSTINFO::getNumPages() {
		return sysconf(_SC_PHYS_PAGES);
	}

	int HOSTINFO::getMaxOpenFiles() {
		return sysconf(_SC_OPEN_MAX);
	}

	// Returns bson object of system related information
	bson::BSONObj HOSTINFO::getSystemInfo() {
		bson::BSONObjBuilder bsystem;
		struct utsname buffer;
		int value;
		int uname_ret = uname(&buffer);

		bsystem.appendDate("currentTime", jsTime());

		if (uname_ret != -1) {
			bsystem.append("hostname", buffer.nodename);
		}

		value = getCpuAddressSize();
		if (value != -1) {
			bsystem.append("cpuAddrSize", value);
		}

		value = getMemSizeMB();
		if (value != -1) {
			bsystem.append("memSizeMB", value);
		}

		value = static_cast<int>(getMemLimitMB());
		if (value != -1) {
			bsystem.append("memLimitMB", value);
		}

		value = getNumCores();
		if (value != -1) {
			bsystem.append("numCores", value);
		}

		if (uname_ret != -1) {
			bsystem.append("cpuArch", buffer.machine);
		}

		bsystem.append("numaEnabled", checkNumaEnabled());
		return bsystem.obj();
	}

	// Returns bson object of OS related information
	bson::BSONObj HOSTINFO::getOsInfo() {
		struct utsname buffer;
		std::string osName;
		std::string osVersion;
		int uname_ret = uname(&buffer);
		bson::BSONObjBuilder bos;

		getOsRelease(osName, osVersion);

		if (uname_ret != -1) {
			bos.append("type", buffer.sysname);
		}

		if(!osName.empty()) {
			bos.append("name", osName);
		}
		if(!osVersion.empty()) {
			bos.append("version", osVersion);
		}
		return bos.obj();
	}

	// Returns bson object of generic information
	bson::BSONObj HOSTINFO::getExtraInfo() {
		struct utsname buffer;
		std::string versionSignature = getVersionSignature();
		std::string versionString = getVersionString();
		std::string cpuFrequency = getCpuFrequencyMHZ();
		std::string libcVersion = getLibcVersion();
		std::string cpuFeatures = getCpuFeatures();
		bson::BSONObjBuilder bExtra;

		int uname_ret = uname(&buffer);
		int value;

		if(!versionString.empty()) {
			bExtra.append("versionString", versionString);
		}

		if(!libcVersion.empty()) {
			bExtra.append("libcVersion", libcVersion );
		}

		if(!versionSignature.empty()) {
			bExtra.append("versionSignature", versionSignature);
		}

		if (uname_ret != -1) {
			bExtra.append("kernelVersion", buffer.release);
		}

		if(!cpuFrequency.empty()) {
			boost::algorithm::trim_left(cpuFrequency);
			bExtra.append("cpuFrequencyMHz", cpuFrequency );
		}

		if(!cpuFeatures.empty()) {
			boost::algorithm::trim_left(cpuFeatures);
			bExtra.append("cpuFeatures", cpuFeatures );
		}

		long long pageSize = getPageSize();
		if (pageSize != -1) {
			bExtra.append("pageSize", pageSize);
		}

		value = getNumPages();
		if (value != -1) {
			bExtra.append("numPages", value);
		}

		value = getMaxOpenFiles();
		if (value != -1) {
			bExtra.append("maxOpenFiles", value);
		}

		return bExtra.obj();
	}

#elif defined(__APPLE__)
	// Get a sysctl string value by name
	std::string HOSTINFO::getSysctlByName(const char* sysctlName) {
		std::string value;
		size_t len;
		int status;
		do {
			status = sysctlbyname(sysctlName, nullptr, &len, nullptr, 0);
			if (status == -1)
				break;
			value.resize(len);
			status = sysctlbyname(sysctlName, &*value.begin(), &len, nullptr, 0);
		} while (status == -1 && errno == ENOMEM);

		if (status == -1) {
			// returns empty string
			return std::string();
		}

		return value.c_str();
	}

	// Get a sysctl integer value by name
	long long HOSTINFO::getSysctlByValue(const char* sysctlName) {
		long long value = 0;
		size_t len = sizeof(value);

		if (sysctlbyname(sysctlName, &value, &len, nullptr, 0) < 0) {
			return -1;
		}

		// returns not more than 8 bytes since type is long long.
		if (len > 8) {
			return -1;
		}

		return value;
	}

	int HOSTINFO::getCpuAddressSize() {
		int cpuAddressSize = getSysctlByValue("hw.cpu64bit_capable");

		if (cpuAddressSize == -1) {
			return -1;
		}

		return (cpuAddressSize ? 64 : 32);
	}

	int HOSTINFO::getMemSizeMB() {
		long long memSize = getSysctlByValue("hw.memsize");
		if (memSize == -1)
			return -1;

		memSize = memSize/(1024*1024);
		return memSize;
	}

	int HOSTINFO::getNumCores() {
		return getSysctlByValue("hw.ncpu");
	}

	bool HOSTINFO::checkNumaEnabled() {
		return false;
	}

	std::string HOSTINFO::getOsRelease() {
		return getSysctlByName("kern.osrelease");
	}

	std::string HOSTINFO::getVersionString() {
		return getSysctlByName("kern.version");
	}

	int HOSTINFO::getAlwaysFullSync() {
		return getSysctlByValue("vfs.generic.always_do_fullfsync");
	}

	int HOSTINFO::getNfsAsync() {
		return getSysctlByValue("vfs.generic.nfs.client.allow_async");
	}

	std::string HOSTINFO::getModel() {
		return getSysctlByName("hw.model");
	}

	int HOSTINFO::getPhysicalCores() {
		return getSysctlByValue("machdep.cpu.core_count");
	}

	int HOSTINFO::getCpuFrequencyMHZ() {
		return (getSysctlByValue("hw.cpufrequency") / (1000 * 1000));
	}

	std::string HOSTINFO::getCpuString() {
		return getSysctlByName("machdep.cpu.brand_string");
	}

	std::string HOSTINFO::getCpuFeatures() {
		std::string cpuFeatures = getSysctlByName("machdep.cpu.features") + std::string(" ") + 	getSysctlByName("machdep.cpu.extfeatures");
		return cpuFeatures;
	}

	int HOSTINFO::getPageSize() {
		return getSysctlByValue("hw.pagesize");
	}

	std::string HOSTINFO::getScheduler() {
		return getSysctlByName("kern.sched");
	}

	// Returns bson object of system related information
	bson::BSONObj HOSTINFO::getSystemInfo() {
		bson::BSONObjBuilder bsystem;
		struct utsname buffer;
		int value;
		int uname_ret = uname(&buffer);

		bsystem.appendDate("currentTime", jsTime());

		if (uname_ret != -1) {
			bsystem.append("hostname", buffer.nodename);
		}

		value = getCpuAddressSize();
		if (value != -1) {
			bsystem.append("cpuAddrSize", value);
		}

		value = getMemSizeMB();
		if (value != -1) {
			bsystem.append("memSizeMB", value);
			bsystem.append("memLimitMB", value);
		}

		value = getNumCores();
		if (value != -1) {
			bsystem.append("numCores", value);
		}

		if (uname_ret != -1) {
			bsystem.append("cpuArch", buffer.machine);
		}

		bsystem.append("numaEnabled", checkNumaEnabled());

		return bsystem.obj();
	}

	// Returns bson object of OS related information
	bson::BSONObj HOSTINFO::getOsInfo() {
		bson::BSONObjBuilder bos;
		std::string osVersion;

		osVersion = getOsRelease();

		bos.append("type", "Darwin");
		bos.append("name", "Mac OS X");

		if(!osVersion.empty()) {
			bos.append("version", osVersion);
		}

		return bos.obj();
	}

	// Returns bson object of generic information
	bson::BSONObj HOSTINFO::getExtraInfo() {
		bson::BSONObjBuilder bExtra;
		std::string versionString = getVersionString();
		std::string model = getModel();
		std::string cpuString = getCpuString();
		std::string cpuFeatures = getCpuFeatures();
		std::string scheduler = getScheduler();

		int value;

		if(!versionString.empty()) {
			bExtra.append("versionString", versionString);
		}

		value = getAlwaysFullSync();
		if (value != -1) {
			bExtra.append("alwaysFullSync", value);
		}

		value = getNfsAsync();
		if (value != -1) {
			bExtra.append("nfsAsync", value);
		}

		if(!model.empty()) {
			bExtra.append("model", model);
		}

		value = getPhysicalCores();
		if (value != -1) {
			bExtra.append("physicalCores", value);
		}

		value = getCpuFrequencyMHZ();
		if (value != -1) {
			bExtra.append("cpuFrequencyMHz", value);
		}

		if(!cpuString.empty()) {
			bExtra.append("cpuString",cpuString);
		}

		if(!cpuFeatures.empty()) {
			bExtra.append("cpuFeatures",cpuFeatures );
		}

		value = getPageSize();
		if (value != -1) {
			bExtra.append("pageSize", value);
		}

		if(!scheduler.empty()) {
			bExtra.append("scheduler", scheduler );
		}

		return bExtra.obj();
	}
#endif
}
