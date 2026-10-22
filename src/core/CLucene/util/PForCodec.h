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
#pragma once

#include <cstddef>
#include <cstdint>

// Patched frame-of-reference codec for uint32 arrays, used by the inverted index
// postings. This is an independent implementation of the on-disk block format that
// has been written into existing indexes (CodeMode kPfor / kPfor128 / kPfor256), so
// that those indexes stay readable and newly written ones stay readable by old
// versions.
//
// Stream layout (all multi-byte fields little endian unless noted):
//
//   stream  := first-value block*
//   first-value : the array's first element, 1..5 bytes
//       x < 2^7        : x
//       x < 2^14       : 0x80|x>>8, x
//       x < 2^21       : 0xc0|x>>16, x[16]
//       x < 2^28       : 0xe0|x>>24, x>>16, x>>8, x            (big endian tail)
//       otherwise      : 0xf0, x[32]
//
// The remaining n-1 elements are turned into n-1 "deltas" t[i] (see Transform below)
// and cut into blocks of kBlock deltas; the last block holds the remainder. Every
// block starts with a header byte h = mode<<6 | b, where b (0..32) is the bit width
// of the packed part:
//
//   mode 0 (plain)  : packed(b)
//   mode 1 (list)   : count, packed(b) of the low b bits, his, count x pos
//                     his holds the hi parts (x >> b) of the exceptions: count variable
//                     length integers, or the byte 0xff followed by count plain 32-bit
//                     values. pos is the index in the block (1 byte).
//   mode 2 (bitmap) : bx, bitmap[ceil(k/8)], packed(bx) of the hi parts (x >> b) of the
//                     flagged values in index order, packed(b) of the low b bits
//   mode 3 (const)  : value[ceil(b/8)], all k deltas equal to value
//
// packed(w) of k values is LSB-first bit packing. It is "horizontal" (value i at bit
// i*w) except for a full 256 value block of the 256 layout, which is "vertical":
// value i lives in lane i%8, row i/8; each lane packs its 32 rows into w 32-bit words
// and word j of lane l is stored at word index j*8+l.
//
// Variable length integer for hi parts:
//       x < 177            : x
//       x < 177+2^14       : 0xb1+(x-177)>>8, (x-177)
//       x < 16561+2^19     : 0xf1+(x-16561)>>16, (x-16561)[16]
//       x < 2^24           : 0xf9, x[24]
//       otherwise          : 0xfa, x[32]
namespace lucene {
namespace util {
namespace pfor {

// Upper bound of the encoded size of n values, for sizing output buffers.
constexpr size_t encodedSizeBound(size_t n) {
    return 8 + n * 4 + n / 128 + 1;
}

// Delta = zigzag(in[i] - in[i-1]).
size_t encodeZigzag128(const uint32_t* in, size_t n, uint8_t* out);
size_t encodeZigzag256(const uint32_t* in, size_t n, uint8_t* out);
// Delta = in[i] - in[i-1] - 1 (strictly increasing sequences).
size_t encodeDelta1_128(const uint32_t* in, size_t n, uint8_t* out);
size_t encodeDelta1_256(const uint32_t* in, size_t n, uint8_t* out);

// Decoders write exactly n values to out and return the number of input bytes consumed.
// Reading never goes past `end`; a malformed or truncated stream throws CL_ERR_CorruptIndex.
size_t decodeZigzag128(const uint8_t* in, const uint8_t* end, size_t n, uint32_t* out);
size_t decodeZigzag256(const uint8_t* in, const uint8_t* end, size_t n, uint32_t* out);
size_t decodeDelta1_128(const uint8_t* in, const uint8_t* end, size_t n, uint32_t* out);
size_t decodeDelta1_256(const uint8_t* in, const uint8_t* end, size_t n, uint32_t* out);

} // namespace pfor
} // namespace util
} // namespace lucene
