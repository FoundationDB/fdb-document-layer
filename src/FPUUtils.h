/*
 * FPUUtils.h
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

#ifndef __FPU_UTILS_H__
#define __FPU_UTILS_H__
#pragma once

#ifdef __cplusplus
extern "C" {
#endif

#ifdef _MSC_VER
void __fastcall convert80to64i(unsigned char* value, unsigned char* result);
void __fastcall convert64ito80(unsigned char* value, unsigned char* result);

void __fastcall convert80to64d(unsigned char* value, unsigned char* result);
void __fastcall convert64dto80(unsigned char* value, unsigned char* result);

void __fastcall add80to80(unsigned char* ldA, const unsigned char* ldB, unsigned char* result);
void __fastcall multiply80to80(unsigned char* ldA, const unsigned char* ldB, unsigned char* result);
#endif

#ifdef __cplusplus
}
#endif

#endif // __FPU_UTILS_H__