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

#include "CLucene/CLConfig.h"
#include "CLucene/debug/error.h"
#include "CLucene/index/CodeMode.h"
#include "CLucene/util/PForCodec.h"

CL_NS_USE(index)

namespace {

// All decoders share this shape: (in, end, n, out) -> bytes consumed.
using DecodeFn = size_t (*)(const uint8_t*, const uint8_t*, size_t, uint32_t*);

size_t DefaultDEC(const uint8_t* in, const uint8_t* /*end*/, size_t n, uint32_t* out) {
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
    return bufferSize;
}

size_t DefaultDDEC(const uint8_t* in, const uint8_t* /*end*/, size_t n, uint32_t* out) {
    uint32_t docDelta = 0;
    size_t bufferSize = 0;
    for (uint32_t i = 0; i < n; i++) {
        uint8_t b = in[bufferSize++];
        int32_t docCode = b & 0x7F;
        for (int32_t shift = 7; (b & 0x80) != 0; shift += 7) {
            b = in[bufferSize++];
            docCode |= (b & 0x7F) << shift;
        }
        docDelta += docCode;
        out[i] = docDelta;
    }
    return bufferSize;
}

size_t DefaultDENC(uint32_t* __restrict in, size_t n, unsigned char* __restrict out) {
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

size_t DefaultENC(uint32_t* __restrict in, size_t n, unsigned char* __restrict out) {
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

// The blocks of the legacy kPfor mode were written by whatever the writing platform
// used: the 256 value layout on x86_64 with AVX2, the 128 value layout on arm64 and
// plain variable length integers on x86_64 without AVX2. `compatibleRead` means the
// index was written by the other platform than the one reading it.
DecodeFn legacyDocDecoder(bool compatibleRead) {
#if defined(USE_AVX2) && defined(__AVX2__)
    return compatibleRead ? lucene::util::pfor::decodeDelta1_128
                          : lucene::util::pfor::decodeDelta1_256;
#elif defined(__ARM_NEON)
    return compatibleRead ? lucene::util::pfor::decodeDelta1_256
                          : lucene::util::pfor::decodeDelta1_128;
#elif defined(__SSSE3__)
    return compatibleRead ? lucene::util::pfor::decodeDelta1_256 : DefaultDDEC;
#else
    return DefaultDDEC;
#endif
}

DecodeFn legacyFreqDecoder(bool compatibleRead) {
#if defined(USE_AVX2) && defined(__AVX2__)
    return compatibleRead ? lucene::util::pfor::decodeZigzag128
                          : lucene::util::pfor::decodeZigzag256;
#elif defined(__ARM_NEON)
    return compatibleRead ? lucene::util::pfor::decodeZigzag256
                          : lucene::util::pfor::decodeZigzag128;
#elif defined(__SSSE3__)
    return compatibleRead ? lucene::util::pfor::decodeZigzag256 : DefaultDEC;
#else
    return DefaultDEC;
#endif
}

// Reads a length prefixed serialized array. The buffer is padded so that readers may
// look slightly past the payload; `size` is the payload length.
size_t readSerialized(IndexInput* in, std::vector<uint8_t>& buf) {
    uint32_t size = in->readVInt();
    buf.resize(size + PFOR_BLOCK_SIZE);
    in->readBytes(buf.data(), size);
    return size;
}

void decodeSerialized(IndexInput* in, DecodeFn fn, size_t n, uint32_t* out) {
    std::vector<uint8_t> buf;
    size_t size = readSerialized(in, buf);
    fn(buf.data(), buf.data() + size, n, out);
}

} // namespace

size_t P4DEC(unsigned char* __restrict in, size_t n, uint32_t* __restrict out) {
#if (defined(__i386) || defined(__x86_64__))
#if defined(USE_AVX2)
    return lucene::util::pfor::decodeDelta1_256(in, in + lucene::util::pfor::encodedSizeBound(n), n,
                                                out);
#else
    return DefaultDDEC(in, nullptr, n, out);
#endif
#else
    return lucene::util::pfor::decodeDelta1_128(in, in + lucene::util::pfor::encodedSizeBound(n), n,
                                                out);
#endif
}

size_t P4NZDEC(unsigned char* __restrict in, size_t n, uint32_t* __restrict out) {
#if (defined(__i386) || defined(__x86_64__))
#if defined(USE_AVX2)
    return lucene::util::pfor::decodeZigzag256(in, in + lucene::util::pfor::encodedSizeBound(n), n,
                                               out);
#else
    return DefaultDEC(in, nullptr, n, out);
#endif
#else
    return lucene::util::pfor::decodeZigzag128(in, in + lucene::util::pfor::encodedSizeBound(n), n,
                                               out);
#endif
}

size_t P4ENC(uint32_t* __restrict in, size_t n, unsigned char* __restrict out) {
#if (defined(__i386) || defined(__x86_64__))
#if defined(USE_AVX2)
    return lucene::util::pfor::encodeDelta1_256(in, n, out);
#else
    return DefaultDENC(in, n, out);
#endif
#else
    return lucene::util::pfor::encodeDelta1_128(in, n, out);
#endif
}

size_t P4NZENC(uint32_t* __restrict in, size_t n, unsigned char* __restrict out) {
#if (defined(__i386) || defined(__x86_64__))
#if defined(USE_AVX2)
    return lucene::util::pfor::encodeZigzag256(in, n, out);
#else
    return DefaultENC(in, n, out);
#endif
#else
    return lucene::util::pfor::encodeZigzag128(in, n, out);
#endif
}

void PforUtil::encodePos(IndexOutput* out, std::vector<uint32_t>& buffer) {
#ifdef __AVX2__
    out->writeByte((char)CodeMode::kPfor256);
    out->writeVInt(buffer.size());
    std::vector<uint8_t> compress(lucene::util::pfor::encodedSizeBound(buffer.size()));
    size_t compressSize =
            lucene::util::pfor::encodeZigzag256(buffer.data(), buffer.size(), compress.data());
    out->writeVInt(compressSize);
    out->writeBytes(reinterpret_cast<const uint8_t*>(compress.data()), compressSize);
#elif defined(__ARM_NEON)
    out->writeByte((char)CodeMode::kPfor128);
    out->writeVInt(buffer.size());
    std::vector<uint8_t> compress(lucene::util::pfor::encodedSizeBound(buffer.size()));
    size_t compressSize =
            lucene::util::pfor::encodeZigzag128(buffer.data(), buffer.size(), compress.data());
    out->writeVInt(compressSize);
    out->writeBytes(reinterpret_cast<const uint8_t*>(compress.data()), compressSize);
#else
    out->writeByte((char)CodeMode::kDefault);
    out->writeVInt(buffer.size());
    for (size_t i = 0; i < buffer.size(); i++) {
        out->writeVInt(buffer[i]);
    }
#endif
    buffer.resize(0);
}

uint32_t PforUtil::decodePos(IndexInput* in, std::vector<uint32_t>& buffer) {
    CodeMode mode = static_cast<CodeMode>(in->readByte());
    uint32_t size = in->readVInt();
    buffer.resize((size + PforUtil::blockSize - 1) / PforUtil::blockSize * PforUtil::blockSize + 3);
    if (mode == CodeMode::kPfor) {
        _CLTHROWA(CL_ERR_CorruptIndex, "PFOR old version is not supported for decodePos");
    } else if (mode == CodeMode::kPfor256) {
        decodeSerialized(in, lucene::util::pfor::decodeZigzag256, size, buffer.data());
    } else if (mode == CodeMode::kPfor128) {
        decodeSerialized(in, lucene::util::pfor::decodeZigzag128, size, buffer.data());
    } else if (mode == CodeMode::kDefault) {
        for (uint32_t i = 0; i < size; i++) {
            buffer[i] = in->readVInt();
        }
    }
    return size;
}

void PforUtil::pfor_encode(IndexOutput* out, std::vector<uint32_t>& docDeltaBuffer,
                           std::vector<uint32_t>& freqBuffer, bool has_prox) {
#ifdef __AVX2__
    out->writeByte((char)CodeMode::kPfor256);
    out->writeVInt(docDeltaBuffer.size());
    std::vector<uint8_t> compress(lucene::util::pfor::encodedSizeBound(docDeltaBuffer.size()));
    size_t size = 0;
    size = lucene::util::pfor::encodeDelta1_256(docDeltaBuffer.data(), docDeltaBuffer.size(),
                                                compress.data());
    out->writeVInt(size);
    out->writeBytes(reinterpret_cast<const uint8_t*>(compress.data()), size);
    if (has_prox) {
        size = lucene::util::pfor::encodeZigzag256(freqBuffer.data(), freqBuffer.size(),
                                                   compress.data());
        out->writeVInt(size);
        out->writeBytes(reinterpret_cast<const uint8_t*>(compress.data()), size);
    }
#elif defined(__ARM_NEON)
    out->writeByte((char)CodeMode::kPfor128);
    out->writeVInt(docDeltaBuffer.size());
    std::vector<uint8_t> compress(lucene::util::pfor::encodedSizeBound(docDeltaBuffer.size()));
    size_t size = 0;
    size = lucene::util::pfor::encodeDelta1_128(docDeltaBuffer.data(), docDeltaBuffer.size(),
                                                compress.data());
    out->writeVInt(size);
    out->writeBytes(reinterpret_cast<const uint8_t*>(compress.data()), size);
    if (has_prox) {
        size = lucene::util::pfor::encodeZigzag128(freqBuffer.data(), freqBuffer.size(),
                                                   compress.data());
        out->writeVInt(size);
        out->writeBytes(reinterpret_cast<const uint8_t*>(compress.data()), size);
    }
#else
    out->writeByte((char)CodeMode::kDefault);
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

uint32_t PforUtil::pfor_decode(IndexInput* in, std::vector<uint32_t>& docs,
                               std::vector<uint32_t>& freqs, bool has_prox, bool compatibleRead) {
    char mode = in->readByte();
    uint32_t arraySize = in->readVInt();

    DecodeFn docDecoder = nullptr;
    DecodeFn freqDecoder = nullptr;
    if (mode == (char)CodeMode::kPfor) {
        docDecoder = legacyDocDecoder(compatibleRead);
        freqDecoder = legacyFreqDecoder(compatibleRead);
    } else if (mode == (char)CodeMode::kPfor256) {
        docDecoder = lucene::util::pfor::decodeDelta1_256;
        freqDecoder = lucene::util::pfor::decodeZigzag256;
    } else if (mode == (char)CodeMode::kPfor128) {
        docDecoder = lucene::util::pfor::decodeDelta1_128;
        freqDecoder = lucene::util::pfor::decodeZigzag128;
    }

    if (docDecoder != nullptr) {
        decodeSerialized(in, docDecoder, arraySize, docs.data());
        if (has_prox) {
            decodeSerialized(in, freqDecoder, arraySize, freqs.data());
        }
    } else if (mode == (char)CodeMode::kDefault) {
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
    }
    return arraySize;
}
