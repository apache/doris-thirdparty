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
#if (defined(__i386) || defined(__x86_64__))
#include <cpuid.h>
#endif

namespace {
using DEC_FUNC = size_t (*)(unsigned char *__restrict, size_t, uint32_t *__restrict);
using ENC_FUNC = size_t (*)(uint32_t *__restrict in, size_t n, unsigned char *__restrict out);
DEC_FUNC g_p4nd1dec;
DEC_FUNC g_p4nzdec;
ENC_FUNC g_p4nd1enc;
ENC_FUNC g_p4nzenc;
} // anonymous namespace

size_t DefaultDEC(unsigned char *__restrict in, size_t n, uint32_t *__restrict out) {
    size_t bufferSize = 0;
    for (uint32_t i = 0; i < n; i++) {
        uint8_t b = in[bufferSize++];
        int32_t docCode = b & 0x7F;
        for (int32_t shift = 7; (b & 0x80) != 0; shift += 7) {
            b = in[bufferSize++];
            docCode |= (b & 0x7F) << shift;
        }
        out[i] = docCode;
    }
    return n;
}

size_t DefaultDDEC(unsigned char *__restrict in, size_t n, uint32_t *__restrict out) {
    uint32_t docDelta = 0;
    size_t bufferSize = 0;
    for (uint32_t i = 0; i < n; i++) {
        uint8_t b = in[bufferSize++];
        int32_t docCode = b & 0x7F;
        for (int32_t shift = 7; (b & 0x80) != 0; shift += 7) {
            b = in[bufferSize++];
            docCode |= (b & 0x7F) << shift;
        }
        docDelta += docCode;  // Corrected line: Removed right shift
        out[i] = docDelta;
    }
    return n;
}

size_t DefaultDENC(uint32_t *__restrict in, size_t n, unsigned char *__restrict out) {
    int outIndex = 0;
    uint32_t lastDoc = 0;
    for (int32_t i = 0; i < n; i++) {
        uint32_t curDoc = in[i];
        uint32_t delta = curDoc - lastDoc;
        while ((delta & ~0x7F) != 0) {
            out[outIndex++] = (uint8_t)((delta & 0x7f) | 0x80);
            delta >>= 7; //doing unsigned shift
        }
        out[outIndex++] = (uint8_t)delta;
        lastDoc = curDoc;
    }
    return outIndex;
}

size_t DefaultENC(uint32_t *__restrict in, size_t n, unsigned char *__restrict out) {
    int outIndex = 0;
    for (int32_t i = 0; i < n; i++) {
        uint32_t curDoc = in[i];
        while ((curDoc & ~0x7F) != 0) {
            out[outIndex++] = (uint8_t)((curDoc & 0x7f) | 0x80);
            curDoc >>= 7; //doing unsigned shift
        }
        out[outIndex++] = (uint8_t)curDoc;
    }
    return outIndex;
}

__attribute__((constructor)) void SelectPFORFunctions() {
#if (defined(__i386) || defined(__x86_64__))
    uint32_t eax, ebx, ecx, edx;
    __cpuid(1, eax, ebx, ecx, edx);

    bool sse2 = (edx & bit_SSE2) != 0;
    bool sse42 = (ecx & bit_SSE4_2) != 0;
#if defined(USE_AVX2)
    g_p4nd1dec = p4nd1dec256v32;
    g_p4nzdec = p4nzdec256v32;
    g_p4nd1enc = p4nd1enc256v32;
    g_p4nzenc = p4nzenc256v32;
#else
    g_p4nd1dec = DefaultDDEC;
    g_p4nzdec = DefaultDEC;
    g_p4nd1enc = DefaultDENC;
    g_p4nzenc = DefaultENC;
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
