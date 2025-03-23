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
#include "CLucene/debug/error.h"
#include "CLucene/index/CodeMode.h"
#include "vp4.h"
#if (defined(__i386) || defined(__x86_64__))
#include <cpuid.h>
#endif

CL_NS_DEF(util)
using DEC_FUNC = size_t (*)(unsigned char *__restrict, size_t, uint32_t *__restrict);
using ENC_FUNC = size_t (*)(uint32_t *__restrict in, size_t n, unsigned char *__restrict out);
DEC_FUNC g_p4nd1dec;
DEC_FUNC g_p4nzdec;
ENC_FUNC g_p4nd1enc;
ENC_FUNC g_p4nzenc;

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
void pfor_encode(store::IndexOutput* out, std::vector<uint32_t>& docDeltaBuffer, std::vector<uint32_t>& freqBuffer, bool has_prox) {
#ifdef __AVX2__
    out->writeByte((char)index::CodeMode::kPfor256);
    out->writeVInt(docDeltaBuffer.size());
    std::vector<uint8_t> compress(4 * docDeltaBuffer.size() + PFOR_BLOCK_SIZE);
    size_t size = 0;
    size = p4nd1enc256v32(docDeltaBuffer.data(), docDeltaBuffer.size(), compress.data());
    out->writeVInt(size);
    out->writeBytes(reinterpret_cast<const uint8_t*>(compress.data()), size);
    if (has_prox) {
        size = p4nzenc256v32(freqBuffer.data(), freqBuffer.size(), compress.data());
        out->writeVInt(size);
        out->writeBytes(reinterpret_cast<const uint8_t*>(compress.data()), size);
    }
#elif (defined(__SSSE3__) || defined(__ARM_NEON))
    out->writeByte((char)index::CodeMode::kPfor128);
    out->writeVInt(docDeltaBuffer.size());
    std::vector<uint8_t> compress(4 * docDeltaBuffer.size() + PFOR_BLOCK_SIZE);
    size_t size = 0;
    size = p4nd1enc32(docDeltaBuffer.data(), docDeltaBuffer.size(), compress.data());
    out->writeVInt(size);
    out->writeBytes(reinterpret_cast<const uint8_t*>(compress.data()), size);
    if (has_prox) {
        size = p4nzenc32(freqBuffer.data(), freqBuffer.size(), compress.data());
        out->writeVInt(size);
        out->writeBytes(reinterpret_cast<const uint8_t*>(compress.data()), size);
    }
#else
    out->writeByte((char)index::CodeMode::kDefault);
    out->writeVInt(docDeltaBuffer.size());
    uint32_t lastDoc = 0;
    for (int32_t i = 0; i < docDeltaBuffer.size(); i++) {
        uint32_t curDoc = docDeltaBuffer[i];
        if (has_prox) {
            uint32_t newDocCode = (curDoc - lastDoc) << 1;
            lastDoc = curDoc;
            uint32_t freq = freqBuffer[i];
            if (1 == freq) {
                out->writeVInt(newDocCode | 1);
            } else {
                out->writeVInt(newDocCode);
                out->writeVInt(freq);
            }
        } else {
            out->writeVInt(curDoc - lastDoc);
            lastDoc = curDoc;
        }
    }
#endif
    docDeltaBuffer.resize(0);
    freqBuffer.resize(0);
}

uint32_t pfor_decode(store::IndexInput* in, std::vector<uint32_t>& docs, std::vector<uint32_t>& freqs, bool has_prox, bool compatibleRead) {
    char mode = in->readByte();
    uint32_t arraySize = in->readVInt();
    // old version, need to separate read based on compatibleRead
    if (mode == (char)index::CodeMode::kPfor) {
        {
            uint32_t SerializedSize = in->readVInt();
            std::vector<uint8_t> buf(SerializedSize + PFOR_BLOCK_SIZE);
            in->readBytes(buf.data(), SerializedSize);
#if defined(USE_AVX2) && defined(__AVX2__)
            // if compatibleRead is true, means we are reading old version arm64 index in x86_64 platform.
            if (compatibleRead) {
                p4nd1dec32(buf.data(), arraySize, docs.data());
            } else {
                p4nd1dec256v32(buf.data(), arraySize, docs.data());
            }
#elif (defined(__ARM_NEON))
            // if compatibleRead is true, means we are reading old version x86_64 index in arm64 platform.
            if (compatibleRead) {
                p4nd1dec256scalarv32(buf.data(), arraySize, docs.data());
            } else {
                p4nd1dec32(buf.data(), arraySize, docs.data());
            }
#elif (defined(__SSSE3__))
            // if compatibleRead is true, means we are reading old version x86_64 index in x86_64 which does not support avx2.
            if (compatibleRead) {
                p4nd1dec256scalarv32(buf.data(), arraySize, docs.data());
            } else {
                DefaultDDEC(buf.data(), arraySize, docs.data());
            }
#else
            DefaultDDEC(buf.data(), arraySize, docs.data());
#endif
        }
        if (has_prox) {
            uint32_t SerializedSize = in->readVInt();
            std::vector<uint8_t> buf(SerializedSize + PFOR_BLOCK_SIZE);
            in->readBytes(buf.data(), SerializedSize);
#if defined(USE_AVX2) && defined(__AVX2__)
            // if compatibleRead is true, means we are reading old version arm64 index in x86_64 platform.
            if (compatibleRead) {
                p4nzdec32(buf.data(), arraySize, freqs.data());
            } else {
                p4nzdec256v32(buf.data(), arraySize, freqs.data());
            }
#elif (defined(__ARM_NEON))
            // if compatibleRead is true, means we are reading old version x86_64 index in arm64 platform.
            if (compatibleRead) {
                p4nzdec256scalarv32(buf.data(), arraySize, freqs.data());
            } else {
                p4nzdec32(buf.data(), arraySize, freqs.data());
            }
#elif (defined(__SSSE3__))
            // if compatibleRead is true, means we are reading old version x86_64 index in x86_64 which does not support avx2.
            if (compatibleRead) {
                p4nzdec256scalarv32(buf.data(), arraySize, freqs.data());
            } else {
                DefaultDEC(buf.data(), arraySize, freqs.data());
            }
#else
            DefaultDEC(buf.data(), arraySize, freqs.data());
#endif
        }
    } else if (mode == (char)index::CodeMode::kDefault) {
        uint32_t docDelta = 0;
        for (uint32_t i = 0; i < arraySize; i++) {
            uint32_t docCode = in->readVInt();
            if (has_prox) {
                docDelta += (docCode >> 1);
                docs[i] = docDelta;
                if ((docCode & 1) != 0) {
                    freqs[i] = 1;
                } else {
                    freqs[i] = in->readVInt();
                }
            } else {
                docDelta += docCode;
                docs[i] = docDelta;
            }            
        }
    } else if (mode == (char)index::CodeMode::kPfor256) {
        // new version, read based on compatibleRead
        {
            uint32_t SerializedSize = in->readVInt();
            std::vector<uint8_t> buf(SerializedSize + PFOR_BLOCK_SIZE);
            in->readBytes(buf.data(), SerializedSize);
#if defined(USE_AVX2) && defined(__AVX2__)
            p4nd1dec256v32(buf.data(), arraySize, docs.data());
#else
            _CLTHROWA(CL_ERR_CorruptIndex, "PFOR256 is not supported on this platform");
#endif
        }
        if (has_prox) {
            uint32_t SerializedSize = in->readVInt();
            std::vector<uint8_t> buf(SerializedSize + PFOR_BLOCK_SIZE);
            in->readBytes(buf.data(), SerializedSize);
#if defined(USE_AVX2) && defined(__AVX2__)
            p4nzdec256v32(buf.data(), arraySize, freqs.data());
#else
            _CLTHROWA(CL_ERR_CorruptIndex, "PFOR256 is not supported on this platform");
#endif
        }
    } else if (mode == (char)index::CodeMode::kPfor128) {
        // new version, read based on compatibleRead
         {
            uint32_t SerializedSize = in->readVInt();
            std::vector<uint8_t> buf(SerializedSize + PFOR_BLOCK_SIZE);
            in->readBytes(buf.data(), SerializedSize);
#if defined(USE_AVX2) && defined(__AVX2__)
            p4nd1dec32(buf.data(), arraySize, docs.data());
#elif (defined(__SSSE3__) || defined(__ARM_NEON))
            p4nd1dec32(buf.data(), arraySize, docs.data());
#else
            _CLTHROWA(CL_ERR_CorruptIndex, "PFOR128 is not supported on this platform");
#endif
        }
        if (has_prox) {
            uint32_t SerializedSize = in->readVInt();
            std::vector<uint8_t> buf(SerializedSize + PFOR_BLOCK_SIZE);
            in->readBytes(buf.data(), SerializedSize);
#if defined(USE_AVX2) && defined(__AVX2__)
            p4nzdec32(buf.data(), arraySize, freqs.data());
#elif (defined(__SSSE3__) || defined(__ARM_NEON))
            p4nzdec32(buf.data(), arraySize, freqs.data());
#else
            _CLTHROWA(CL_ERR_CorruptIndex, "PFOR128 is not supported on this platform");
#endif
        }
    }
    return arraySize;
}
CL_NS_END
