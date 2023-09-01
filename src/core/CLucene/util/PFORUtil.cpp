// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
#include "PFORUtil.h"
#include "vp4.h"

namespace {
using DEC_FUNC = size_t (*)(unsigned char *__restrict, size_t, uint32_t *__restrict);
using ENC_FUNC = size_t (*)(uint32_t *__restrict in, size_t n, unsigned char *__restrict out);
DEC_FUNC g_p4nd1dec;
DEC_FUNC g_p4nzdec;
ENC_FUNC g_p4nd1enc;
ENC_FUNC g_p4nzenc;
} // anonymous namespace

/*extern DEC_FUNC p4nd1dec128v32;
extern DEC_FUNC p4nzdec128v32;
extern DEC_FUNC p4nd1dec256v32;
extern DEC_FUNC p4nzdec256v32;
extern DEC_FUNC p4nd1dec32;
extern DEC_FUNC p4nzdec32;
extern ENC_FUNC p4nd1enc128v32;
extern ENC_FUNC p4nzenc128v32;
extern ENC_FUNC p4nd1enc256v32;
extern ENC_FUNC p4nzenc256v32;
extern ENC_FUNC p4nd1enc32;
extern ENC_FUNC p4nzenc32;*/

// When this translation unit is initialized, figure out the current CPU and
// assign the correct function for this architecture.
//
// This avoids an expensive 'cpuid' call in the hot path, and also avoids
// the cost of a 'std::once' call.
__attribute__((constructor)) void SelectPFORFunctions() {
    uint32_t eax, ebx, ecx, edx;
    __cpuid(1, eax, ebx, ecx, edx);

    bool sse2 = (edx & bit_SSE2) != 0;
    bool sse42 = (ecx & bit_SSE4_2) != 0;
#if (defined(__i386) || defined(__x86_64__))
#if defined(USE_AVX2)
    g_p4nd1dec = p4nd1dec256v32;
    g_p4nzdec = p4nzdec256v32;
    g_p4nd1enc = p4nd1enc256v32;
    g_p4nzenc = p4nzenc256v32;
#else
    if (sse42) {
        g_p4nd1dec = p4nd1dec128v32;
        g_p4nzdec = p4nzdec128v32;
        g_p4nd1enc = p4nd1enc128v32;
        g_p4nzenc = p4nzenc128v32;
    } else {
        g_p4nd1dec = p4nd1dec32;
        g_p4nzdec = p4nzdec32;
        g_p4nd1enc = p4nd1enc32;
        g_p4nzenc = p4nzenc32;
    }
#endif
#else
    g_p4nd1dec = p4nd1dec32;
    g_p4nzdec = p4nzdec32;
    g_p4nd1enc = p4nd1enc32;
    g_p4nzenc = p4nzenc32;
#endif
}

size_t P4DEC(unsigned char *__restrict in, size_t n, uint32_t *__restrict out) {
    return g_p4nd1dec(in, n, out);
}
size_t P4NZDEC(unsigned char *__restrict in, size_t n, uint32_t *__restrict out) {
    return g_p4nzdec(in, n, out);
}

size_t P4ENC(uint32_t *__restrict in, size_t n, unsigned char *__restrict out) {
    return g_p4nd1enc(in, n, out);
}

size_t P4NZENC(uint32_t *__restrict in, size_t n, unsigned char *__restrict out) {
    return g_p4nzenc(in, n, out);
}
