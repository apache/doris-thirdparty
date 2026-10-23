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
#include "PForCodec.h"

#include <algorithm>
#include <array>
#include <cstring>
#include <utility>

#ifdef __AVX2__
#include <immintrin.h>
#endif

#include "CLucene/_ApiHeader.h"
#include "CLucene/debug/error.h"

static_assert(__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__, "the stream layout assumes little endian");

namespace lucene {
namespace util {
namespace pfor {

namespace {

constexpr uint32_t kModePlain = 0;
constexpr uint32_t kModeList = 1;
constexpr uint32_t kModeBitmap = 2;
constexpr uint32_t kModeConst = 3;

constexpr size_t kGroup = 32; // values per packing unit
constexpr size_t kLanes = 8;  // lanes of the vertical layout

#if defined(__clang__)
#define PFOR_UNROLL _Pragma("clang loop unroll(full)")
#elif defined(__GNUC__)
#define PFOR_UNROLL _Pragma("GCC unroll 32")
#else
#define PFOR_UNROLL
#endif

[[noreturn]] void corrupt(const char* what) {
    _CLTHROWA(CL_ERR_CorruptIndex, what);
}

inline uint32_t bitLength(uint32_t x) {
    return x == 0 ? 0 : 32 - static_cast<uint32_t>(__builtin_clz(x));
}

inline uint32_t load32(const uint8_t* p, size_t bytes) {
    uint32_t v = 0;
    memcpy(&v, p, bytes);
    return v;
}

// ---------------------------------------------------------------------------------
// Delta transforms
// ---------------------------------------------------------------------------------

// Eight 32-bit lanes; the compiler maps them to whatever vector registers the target has. The
// functions passing them by value are internal, so the ABI note of GCC does not matter.
#if defined(__GNUC__) && !defined(__clang__)
#pragma GCC diagnostic ignored "-Wpsabi"
#endif
typedef uint32_t U32x8 __attribute__((vector_size(32)));
typedef uint32_t U32x8Unaligned __attribute__((vector_size(32), aligned(4), may_alias));

// Shuffles the lanes of two vectors, indices 0-7 pick from a and 8-15 from b. Clang and GCC 12+
// know __builtin_shufflevector, older GCC only has __builtin_shuffle.
#if defined(__clang__)
#define PFOR_SHUFFLE(a, b, ...) __builtin_shufflevector(a, b, __VA_ARGS__)
#else
#define PFOR_SHUFFLE(a, b, ...) __builtin_shuffle(a, b, (U32x8) {__VA_ARGS__})
#endif

struct ZigzagDelta {
    static uint32_t forward(uint32_t cur, uint32_t prev) {
        uint32_t d = cur - prev;
        return (d << 1) ^ (0u - (d >> 31));
    }
    // The signed difference a transformed value stands for.
    static uint32_t toDelta(uint32_t t) { return (t >> 1) ^ (0u - (t & 1)); }
    static U32x8 toDelta(U32x8 t) { return (t >> 1) ^ (0u - (t & 1u)); }
};

struct Delta1 {
    static uint32_t forward(uint32_t cur, uint32_t prev) { return cur - prev - 1; }
    static uint32_t toDelta(uint32_t t) { return t + 1; }
    static U32x8 toDelta(U32x8 t) { return t + 1u; }
};

// ---------------------------------------------------------------------------------
// Bit unpacking. A unit is 32 rows x L lanes of b-bit values stored in b*L words;
// word j of lane l is at j*L+l and rows are packed LSB first. With L == 1 this is the
// plain horizontal packing of 32 consecutive values, with L == 8 the vertical layout.
// ---------------------------------------------------------------------------------

#ifdef __AVX2__
// Horizontal unpacking with AVX2. Eight consecutive b-bit values occupy exactly b bytes, so the
// pattern of a group of eight is the same for the four groups of a unit. For b <= 25 every
// value lies within four bytes: the bytes of the first four values are picked from 16 bytes
// loaded at the start of the group, those of the other four from 16 bytes loaded at byte
// 4*b/8, then shifted right by their bit offset.
constexpr int kMaxShuffledWidth = 25;
// The loads of the last group reach this many bytes past the 4*b bytes of a unit.
constexpr size_t kUnpackOverread = 16;

struct HorizontalTables {
    alignas(32) uint8_t shuffle[kMaxShuffledWidth + 1][32];
    alignas(32) uint32_t shift[kMaxShuffledWidth + 1][8];
};

constexpr HorizontalTables makeHorizontalTables() {
    HorizontalTables t = {};
    for (int b = 1; b <= kMaxShuffledWidth; ++b) {
        for (int k = 0; k < 8; ++k) {
            const int half = k / 4;
            const int rel = k * b - (half ? 8 * ((4 * b) >> 3) : 0);
            t.shift[b][k] = rel & 7;
            for (int j = 0; j < 4; ++j) {
                t.shuffle[b][half * 16 + (k % 4) * 4 + j] = (rel >> 3) + j;
            }
        }
    }
    return t;
}

constexpr HorizontalTables kHorizontalTables = makeHorizontalTables();
#else
constexpr size_t kUnpackOverread = 0;
#endif

#ifdef __AVX2__
// Eight consecutive b-bit values, which occupy the b bytes at p (plus the overread of the loads).
template <int B>
inline void unpackGroup8(const uint8_t* p, uint32_t* out) {
    if constexpr (B >= 1 && B <= kMaxShuffledWidth) {
        const __m256i shuffle =
                _mm256_loadu_si256(reinterpret_cast<const __m256i*>(kHorizontalTables.shuffle[B]));
        const __m256i shift =
                _mm256_loadu_si256(reinterpret_cast<const __m256i*>(kHorizontalTables.shift[B]));
        const __m256i mask = _mm256_set1_epi32(static_cast<int>((1u << B) - 1));
        const __m256i bytes = _mm256_inserti128_si256(
                _mm256_castsi128_si256(_mm_loadu_si128(reinterpret_cast<const __m128i*>(p))),
                _mm_loadu_si128(reinterpret_cast<const __m128i*>(p + ((4 * B) >> 3))), 1);
        _mm256_storeu_si256(
                reinterpret_cast<__m256i*>(out),
                _mm256_and_si256(_mm256_srlv_epi32(_mm256_shuffle_epi8(bytes, shuffle), shift),
                                 mask));
    }
}

using Group8Fn = void (*)(const uint8_t*, uint32_t*);

template <size_t... B>
constexpr std::array<Group8Fn, 33> makeGroup8Table(std::index_sequence<B...>) {
    return {{(B >= 1 && B <= kMaxShuffledWidth ? &unpackGroup8<static_cast<int>(B)> : nullptr)...}};
}

constexpr std::array<Group8Fn, 33> kUnpackGroup8 = makeGroup8Table(std::make_index_sequence<33> {});
#else
using Group8Fn = void (*)(const uint8_t*, uint32_t*);
constexpr std::array<Group8Fn, 33> kUnpackGroup8 = {};
#endif

template <int B, int L>
void unpackUnit(const uint8_t* in, uint32_t* out) {
    if constexpr (B == 0) {
        memset(out, 0, kGroup * L * sizeof(uint32_t));
#ifdef __AVX2__
    } else if constexpr (L == 1 && B <= kMaxShuffledWidth) {
        for (int g = 0; g < 4; ++g) {
            unpackGroup8<B>(in + g * B, out + 8 * g);
        }
#endif
    } else {
        uint32_t w[B * L];
        memcpy(w, in, sizeof(w));
        constexpr uint32_t mask = B == 32 ? ~0u : (1u << (B % 32)) - 1;
        PFOR_UNROLL
        for (int i = 0; i < 32; ++i) {
            const int bit = i * B;
            const int j = bit >> 5;
            const int s = bit & 31;
            for (int l = 0; l < L; ++l) {
                uint32_t v = w[j * L + l] >> s;
                if (s + B > 32) {
                    v |= w[(j + 1) * L + l] << (32 - s);
                }
                out[i * L + l] = v & mask;
            }
        }
    }
}

using UnpackFn = void (*)(const uint8_t*, uint32_t*);

template <int L, size_t... B>
constexpr std::array<UnpackFn, 33> makeUnpackTable(std::index_sequence<B...>) {
    return {{&unpackUnit<static_cast<int>(B), L>...}};
}

constexpr std::array<UnpackFn, 33> kUnpackHorizontal =
        makeUnpackTable<1>(std::make_index_sequence<33> {});
constexpr std::array<UnpackFn, 33> kUnpackVertical =
        makeUnpackTable<kLanes>(std::make_index_sequence<33> {});

// The inverse of unpackUnit: packs 32 rows x L lanes (values at i*L+l) into B*L words.
template <int B, int L>
void packUnit(const uint32_t* v, uint32_t* words) {
    if constexpr (B != 0) {
        uint32_t w[B * L] = {};
        constexpr uint32_t mask = B == 32 ? ~0u : (1u << (B % 32)) - 1;
        PFOR_UNROLL
        for (int i = 0; i < 32; ++i) {
            const int bit = i * B;
            const int j = bit >> 5;
            const int s = bit & 31;
            for (int l = 0; l < L; ++l) {
                const uint32_t x = v[i * L + l] & mask;
                w[j * L + l] |= x << s;
                if (s + B > 32) {
                    w[(j + 1) * L + l] |= x >> (32 - s);
                }
            }
        }
        memcpy(words, w, sizeof(w));
    }
}

using PackFn = void (*)(const uint32_t*, uint32_t*);

template <int L, size_t... B>
constexpr std::array<PackFn, 33> makePackTable(std::index_sequence<B...>) {
    return {{&packUnit<static_cast<int>(B), L>...}};
}

constexpr std::array<PackFn, 33> kPackHorizontal =
        makePackTable<1>(std::make_index_sequence<33> {});
constexpr std::array<PackFn, 33> kPackVertical =
        makePackTable<kLanes>(std::make_index_sequence<33> {});

inline size_t packedSize(size_t k, uint32_t b, bool vertical) {
    return vertical ? kGroup * b : (k * b + 7) / 8;
}

// Writes k values of width b, returns the number of bytes written.
size_t packValues(const uint32_t* t, size_t k, uint32_t b, bool vertical, uint8_t* out) {
    if (b == 0) {
        return 0;
    }
    if (vertical) {
        uint32_t words[kLanes * 32];
        kPackVertical[b](t, words);
        memcpy(out, words, kGroup * b);
        return kGroup * b;
    }
    uint8_t* o = out;
    for (size_t g = 0; g < k; g += kGroup) {
        const size_t n = std::min(kGroup, k - g);
        uint32_t words[32];
        if (n == kGroup) {
            kPackHorizontal[b](t + g, words);
        } else {
            uint32_t buf[kGroup] = {};
            memcpy(buf, t + g, n * sizeof(uint32_t));
            kPackHorizontal[b](buf, words);
        }
        const size_t bytes = n == kGroup ? 4 * b : (n * b + 7) / 8;
        memcpy(o, words, bytes);
        o += bytes;
    }
    return o - out;
}

// ---------------------------------------------------------------------------------
// Variable length integers
// ---------------------------------------------------------------------------------

constexpr uint32_t kVarint1 = 177;
constexpr uint32_t kVarint2 = kVarint1 + (64u << 8);
constexpr uint32_t kVarint3 = kVarint2 + (8u << 16);

uint8_t* putFirst(uint8_t* o, uint32_t x) {
    if (x < (1u << 7)) {
        *o++ = static_cast<uint8_t>(x);
    } else if (x < (1u << 14)) {
        *o++ = static_cast<uint8_t>(0x80 | (x >> 8));
        *o++ = static_cast<uint8_t>(x);
    } else if (x < (1u << 21)) {
        *o++ = static_cast<uint8_t>(0xc0 | (x >> 16));
        memcpy(o, &x, 2);
        o += 2;
    } else if (x < (1u << 28)) {
        *o++ = static_cast<uint8_t>(0xe0 | (x >> 24));
        *o++ = static_cast<uint8_t>(x >> 16);
        *o++ = static_cast<uint8_t>(x >> 8);
        *o++ = static_cast<uint8_t>(x);
    } else {
        *o++ = 0xf0;
        memcpy(o, &x, 4);
        o += 4;
    }
    return o;
}

// The hi parts of a list block are either that many variable length integers or, if that
// is smaller, the marker kRawList followed by plain 32-bit values.
constexpr uint32_t kRawList = 0xff;
constexpr uint32_t kVarint4 = 1u << 24;

uint8_t* putVarint(uint8_t* o, uint32_t x) {
    if (x < kVarint1) {
        *o++ = static_cast<uint8_t>(x);
    } else if (x < kVarint2) {
        x -= kVarint1;
        *o++ = static_cast<uint8_t>(0xb1 + (x >> 8));
        *o++ = static_cast<uint8_t>(x);
    } else if (x < kVarint3) {
        x -= kVarint2;
        *o++ = static_cast<uint8_t>(0xf1 + (x >> 16));
        memcpy(o, &x, 2);
        o += 2;
    } else if (x < kVarint4) {
        *o++ = 0xf9;
        memcpy(o, &x, 3);
        o += 3;
    } else {
        *o++ = 0xfa;
        memcpy(o, &x, 4);
        o += 4;
    }
    return o;
}

// Number of t[i] >= threshold. Written as a plain count so that it is vectorized.
inline size_t countAtLeast(const uint32_t* t, size_t k, uint64_t threshold) {
    if (threshold > UINT32_MAX) {
        return 0;
    }
    const uint32_t limit = static_cast<uint32_t>(threshold);
    size_t n = 0;
    for (size_t i = 0; i < k; ++i) {
        n += t[i] >= limit;
    }
    return n;
}

// Bytes of the hi parts (t >> b) of the `exceptions` values wider than b, written as variable
// length integers; maxBits is the width of the widest value.
inline size_t hiVarintBytes(const uint32_t* t, size_t k, uint32_t b, uint32_t maxBits,
                            size_t exceptions) {
    if (maxBits - b <= 7) {
        return exceptions; // every hi part is below 128, one byte each
    }
    return exceptions + countAtLeast(t, k, static_cast<uint64_t>(kVarint1) << b) +
           countAtLeast(t, k, static_cast<uint64_t>(kVarint2) << b) +
           countAtLeast(t, k, static_cast<uint64_t>(kVarint3) << b) +
           countAtLeast(t, k, static_cast<uint64_t>(kVarint4) << b);
}

// Size of the hi parts section of a list block with n exceptions; sets `raw` if the raw form
// is the smaller.
inline size_t hiListSize(size_t varints, size_t n, bool& raw) {
    const size_t rawSize = 1 + 4 * n;
    raw = rawSize < varints;
    return raw ? rawSize : varints;
}

// ---------------------------------------------------------------------------------
// Block encoder
// ---------------------------------------------------------------------------------

// Encodes the k deltas t[0..k) as one block and returns the number of bytes written.
size_t encodeBlock(const uint32_t* t, size_t k, bool vertical, uint8_t* out) {
    uint8_t* o = out;

    if (std::all_of(t, t + k, [&](uint32_t x) { return x == t[0]; })) {
        if (t[0] == 0) {
            *o++ = kModePlain << 6;
        } else {
            const uint32_t b = bitLength(t[0]);
            *o++ = static_cast<uint8_t>(kModeConst << 6 | b);
            memcpy(o, &t[0], (b + 7) / 8);
            o += (b + 7) / 8;
        }
        return o - out;
    }

    uint32_t any = 0;
    for (size_t i = 0; i < k; ++i) {
        any |= t[i];
    }
    const uint32_t maxBits = bitLength(any);

    // Choose the packed width and the way the values wider than it are patched.
    struct Plan {
        size_t cost;
        uint32_t mode;
        uint32_t b;
    };
    Plan best = {1 + packedSize(k, maxBits, vertical), kModePlain, maxBits};
    size_t fewest = 0; // the exceptions at the previous (wider) b; there are at least as many now
    for (int64_t b = static_cast<int64_t>(maxBits) - 1; b >= 0; --b) {
        const uint32_t bx = maxBits - static_cast<uint32_t>(b);
        const size_t packed = packedSize(k, static_cast<uint32_t>(b), vertical);

        // Neither the list (2 bytes + a byte per position and per hi part) nor the bitmap can be
        // smaller than this, so this width cannot win and its exceptions need not be counted.
        if (2 + packed + std::min(2 * fewest, (k + 7) / 8 + (fewest * bx + 7) / 8) >= best.cost) {
            continue;
        }
        const size_t exceptions = countAtLeast(t, k, uint64_t(1) << b);
        fewest = exceptions;

        const size_t bitmapCost = 2 + (k + 7) / 8 + (exceptions * bx + 7) / 8 + packed;
        if (bitmapCost < best.cost) {
            best = {bitmapCost, kModeBitmap, static_cast<uint32_t>(b)};
        }
        const size_t listBase = 2 + packed + exceptions;
        if (exceptions <= 255 && listBase + exceptions < best.cost) {
            bool raw;
            const size_t cost = listBase + hiListSize(hiVarintBytes(t, k, static_cast<uint32_t>(b),
                                                                    maxBits, exceptions),
                                                      exceptions, raw);
            if (cost < best.cost) {
                best = {cost, kModeList, static_cast<uint32_t>(b)};
            }
        }
    }

    const uint32_t b = best.b;
    *o++ = static_cast<uint8_t>(best.mode << 6 | b);
    if (best.mode == kModePlain) {
        o += packValues(t, k, b, vertical, o);
    } else if (best.mode == kModeList) {
        uint8_t* count = o++;
        o += packValues(t, k, b, vertical, o);
        uint8_t positions[256];
        uint32_t hi[256];
        size_t n = 0;
        for (size_t i = 0; i < k; ++i) {
            if (t[i] >> b != 0) {
                hi[n] = t[i] >> b;
                positions[n++] = static_cast<uint8_t>(i);
            }
        }
        bool raw;
        hiListSize(hiVarintBytes(t, k, b, maxBits, n), n, raw);
        if (raw) {
            *o++ = kRawList;
            memcpy(o, hi, 4 * n);
            o += 4 * n;
        } else {
            for (size_t i = 0; i < n; ++i) {
                o = putVarint(o, hi[i]);
            }
        }
        *count = static_cast<uint8_t>(n);
        memcpy(o, positions, n);
        o += n;
    } else {
        *o++ = static_cast<uint8_t>(maxBits - b);
        uint8_t* bitmap = o;
        memset(bitmap, 0, (k + 7) / 8);
        o += (k + 7) / 8;
        uint32_t hi[256];
        size_t n = 0;
        for (size_t i = 0; i < k; ++i) {
            if (t[i] >> b != 0) {
                bitmap[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
                hi[n++] = t[i] >> b;
            }
        }
        o += packValues(hi, n, maxBits - b, false, o);
        o += packValues(t, k, b, vertical, o);
    }
    return o - out;
}

// ---------------------------------------------------------------------------------
// Block decoder
// ---------------------------------------------------------------------------------

struct Reader {
    const uint8_t* p;
    const uint8_t* end;

    void need(size_t n) const {
        if (static_cast<size_t>(end - p) < n) {
            corrupt("PFOR: truncated block");
        }
    }
    uint8_t u8() {
        need(1);
        return *p++;
    }
};

uint32_t getFirst(Reader& r) {
    const uint32_t b0 = r.u8();
    if (b0 < 0x80) {
        return b0;
    }
    if (b0 < 0xc0) {
        return ((b0 & 0x3f) << 8) | r.u8();
    }
    if (b0 < 0xe0) {
        r.need(2);
        const uint32_t v = ((b0 & 0x1f) << 16) | load32(r.p, 2);
        r.p += 2;
        return v;
    }
    if (b0 < 0xf0) {
        r.need(3);
        const uint32_t v = ((b0 & 0x0f) << 24) | (static_cast<uint32_t>(r.p[0]) << 16) |
                           (static_cast<uint32_t>(r.p[1]) << 8) | r.p[2];
        r.p += 3;
        return v;
    }
    if (b0 != 0xf0) {
        corrupt("PFOR: bad first value");
    }
    r.need(4);
    const uint32_t v = load32(r.p, 4);
    r.p += 4;
    return v;
}

uint32_t getVarint(Reader& r) {
    const uint32_t b0 = r.u8();
    if (b0 < 0xb1) {
        return b0;
    }
    if (b0 <= 0xf0) {
        return kVarint1 + (((b0 - 0xb1) << 8) | r.u8());
    }
    if (b0 <= 0xf8) {
        r.need(2);
        const uint32_t v = kVarint2 + (((b0 - 0xf1) << 16) | load32(r.p, 2));
        r.p += 2;
        return v;
    }
    if (b0 == 0xf9) {
        r.need(3);
        const uint32_t v = load32(r.p, 3);
        r.p += 3;
        return v;
    }
    if (b0 == 0xfa) {
        r.need(4);
        const uint32_t v = load32(r.p, 4);
        r.p += 4;
        return v;
    }
    corrupt("PFOR: bad exception value");
}

// Reads k packed values of width b.
void unpackValues(Reader& r, size_t k, uint32_t b, bool vertical, uint32_t* t) {
    const size_t bytes = packedSize(k, b, vertical);
    r.need(bytes);
    if (vertical) {
        kUnpackVertical[b](r.p, t);
    } else {
        // Units are read with kUnpackOverread bytes more than they have, which kDecodePadding
        // allows past the end.
        static_assert(kUnpackOverread <= kDecodePadding, "see PForCodec.h");
        static_assert(8 <= kDecodePadding, "see PForCodec.h");
        size_t g = 0;
        const uint8_t* p = r.p;
        for (; g + kGroup <= k; g += kGroup, p += 4 * b) {
            kUnpackHorizontal[b](p, t + g);
        }
        // fewer than 32 values left: groups of 8 as far as possible, then each value comes from
        // the 8 bytes at its first byte
        size_t i = 0;
        if (const Group8Fn group8 = kUnpackGroup8[b]) {
            for (; g + i + 8 <= k; i += 8) {
                group8(p + (i / 8) * b, t + g + i);
            }
        }
        const uint32_t mask = b >= 32 ? ~0u : (1u << b) - 1;
        for (; g + i < k; ++i) {
            uint64_t word;
            memcpy(&word, p + ((i * b) >> 3), sizeof(word));
            t[g + i] = static_cast<uint32_t>(word >> ((i * b) & 7)) & mask;
        }
    }
    r.p += bytes;
}

#ifdef __AVX2__
// For a byte of the exception bitmap, the index into the (still packed) hi parts of the value of
// each lane: the number of exceptions before that lane.
struct ExpandTable {
    alignas(32) uint32_t index[256][8];
    alignas(32) uint32_t mask[256][8]; // all ones for the lanes whose bit is set
};

constexpr ExpandTable makeExpandTable() {
    ExpandTable t = {};
    for (int bits = 0; bits < 256; ++bits) {
        uint32_t before = 0;
        for (int lane = 0; lane < 8; ++lane) {
            t.index[bits][lane] = before;
            t.mask[bits][lane] = ((bits >> lane) & 1) ? ~0u : 0u;
            before += (bits >> lane) & 1;
        }
    }
    return t;
}

constexpr ExpandTable kExpandTable = makeExpandTable();
#endif

// t[i] |= hi[n] << b for the n-th set bit i of the bitmap. `hi` must have 8 readable entries
// after the last exception.
inline void applyBitmapExceptions(uint32_t* t, const uint8_t* bitmap, size_t k, const uint32_t* hi,
                                  uint32_t b) {
    size_t n = 0;
    size_t byte = 0;
#ifdef __AVX2__
    // where the hi parts of each byte start; computed first so that the bytes below do not wait
    // for each other
    uint32_t before[33];
    before[0] = 0;
    for (size_t j = 0; j < k / 8; ++j) {
        before[j + 1] = before[j] + __builtin_popcount(bitmap[j]);
    }
    const __m128i shift = _mm_cvtsi32_si128(static_cast<int>(b));
    for (; byte < k / 8; ++byte) {
        const uint32_t bits = bitmap[byte];
        const __m256i spread = _mm256_permutevar8x32_epi32(
                _mm256_loadu_si256(reinterpret_cast<const __m256i*>(hi + before[byte])),
                _mm256_loadu_si256(reinterpret_cast<const __m256i*>(kExpandTable.index[bits])));
        __m256i* dst = reinterpret_cast<__m256i*>(t + 8 * byte);
        _mm256_storeu_si256(
                dst, _mm256_or_si256(
                             _mm256_loadu_si256(dst),
                             _mm256_and_si256(_mm256_sll_epi32(spread, shift),
                                              _mm256_loadu_si256(reinterpret_cast<const __m256i*>(
                                                      kExpandTable.mask[bits])))));
    }
    n = before[k / 8];
#endif
    for (; byte < (k + 7) / 8; ++byte) {
        for (uint32_t bits = bitmap[byte]; bits != 0; bits &= bits - 1) {
            const size_t i = byte * 8 + __builtin_ctz(bits);
            if (i >= k) {
                corrupt("PFOR: bad exception position");
            }
            t[i] |= hi[n++] << b;
        }
    }
}

// Decodes the next block of k deltas into t.
void decodeBlock(Reader& r, size_t k, bool vertical, uint32_t* t) {
    const uint32_t header = r.u8();
    const uint32_t mode = header >> 6;
    const uint32_t b = header & 0x3f;
    if (b > 32) {
        corrupt("PFOR: bad bit width");
    }

    if (mode == kModePlain) {
        unpackValues(r, k, b, vertical, t);
    } else if (mode == kModeConst) {
        const size_t bytes = (b + 7) / 8;
        r.need(bytes);
        static_assert(4 <= kDecodePadding, "see PForCodec.h");
        // always load 4 bytes, which is cheaper than a load of a variable size
        const uint32_t v = load32(r.p, 4) & (bytes >= 4 ? ~0u : (1u << (8 * bytes)) - 1);
        r.p += bytes;
        std::fill(t, t + k, v);
    } else if (mode == kModeList) {
        if (b >= 32) {
            corrupt("PFOR: bad bit width");
        }
        const size_t count = r.u8();
        if (count > k) {
            corrupt("PFOR: bad exception count");
        }
        unpackValues(r, k, b, vertical, t);
        uint32_t hi[256];
        if (count > 0) {
            r.need(1);
            if (*r.p == kRawList) {
                ++r.p;
                r.need(4 * count);
                memcpy(hi, r.p, 4 * count);
                r.p += 4 * count;
            } else {
                for (size_t i = 0; i < count; ++i) {
                    hi[i] = getVarint(r);
                }
            }
        }
        r.need(count);
        for (size_t i = 0; i < count; ++i) {
            const size_t pos = r.p[i];
            if (pos >= k) {
                corrupt("PFOR: bad exception position");
            }
            t[pos] |= hi[i] << b;
        }
        r.p += count;
    } else {
        const uint32_t bx = r.u8();
        if (b >= 32 || bx == 0 || b + bx > 32) {
            corrupt("PFOR: bad bit width");
        }
        const size_t bitmapBytes = (k + 7) / 8;
        r.need(bitmapBytes);
        const uint8_t* bitmap = r.p;
        r.p += bitmapBytes;
        size_t count = 0;
        for (size_t i = 0; i < bitmapBytes; ++i) {
            count += __builtin_popcount(bitmap[i]);
        }
        uint32_t hi[256 + 8];
        unpackValues(r, count, bx, false, hi);
        memset(hi + count, 0, 8 * sizeof(uint32_t)); // the vector code reads whole registers
        unpackValues(r, k, b, vertical, t);
        applyBitmapExceptions(t, bitmap, k, hi, b);
    }
}

// ---------------------------------------------------------------------------------
// Whole stream
// ---------------------------------------------------------------------------------

template <class Transform, size_t kBlock, bool kVertical>
size_t encodeImpl(const uint32_t* in, size_t n, uint8_t* out) {
    if (n == 0) {
        return 0;
    }
    uint8_t* o = putFirst(out, in[0]);
    uint32_t t[kBlock];
    for (size_t i = 1; i < n;) {
        const size_t k = std::min(kBlock, n - i);
        for (size_t j = 0; j < k; ++j) {
            t[j] = Transform::forward(in[i + j], in[i + j - 1]);
        }
        o += encodeBlock(t, k, kVertical && k == kBlock, o);
        i += k;
    }
    return o - out;
}

// Inclusive prefix sum within the 8 lanes.
inline U32x8 scan8(U32x8 x) {
    const U32x8 zero = {};
    x += PFOR_SHUFFLE(x, zero, 8, 0, 1, 2, 3, 4, 5, 6);
    x += PFOR_SHUFFLE(x, zero, 8, 8, 0, 1, 2, 3, 4, 5);
    x += PFOR_SHUFFLE(x, zero, 8, 8, 8, 8, 0, 1, 2, 3);
    return x;
}

// Turns the k transformed values t into out[i] = cur + delta(t[0]) + ... + delta(t[i]) and
// returns the last value. Eight values are scanned at a time; the scan of a group does not
// depend on the running total, which only costs one add per group.
template <class Transform>
inline uint32_t accumulate(uint32_t cur, const uint32_t* t, size_t k, uint32_t* out) {
    size_t i = 0;
    if (k >= 8) {
        U32x8 total = {cur, cur, cur, cur, cur, cur, cur, cur};
        for (; i + 8 <= k; i += 8) {
            U32x8 x = scan8(Transform::toDelta(*reinterpret_cast<const U32x8Unaligned*>(t + i)));
            *reinterpret_cast<U32x8Unaligned*>(out + i) = x + total;
            total += PFOR_SHUFFLE(x, x, 7, 7, 7, 7, 7, 7, 7, 7);
        }
        cur = total[0];
    }
    for (; i < k; ++i) {
        cur += Transform::toDelta(t[i]);
        out[i] = cur;
    }
    return cur;
}

// Everything it calls is inlined: the arguments are few and the reader state stays in registers.
template <class Transform, size_t kBlock, bool kVertical>
__attribute__((flatten)) size_t decodeImpl(const uint8_t* in, const uint8_t* end, size_t n,
                                           uint32_t* out) {
    if (n == 0) {
        return 0;
    }
    Reader r = {in, end};
    uint32_t cur = getFirst(r);
    out[0] = cur;
    alignas(32) uint32_t t[kBlock];
    for (size_t i = 1; i < n;) {
        const size_t k = std::min(kBlock, n - i);
        decodeBlock(r, k, kVertical && k == kBlock, t);
        cur = accumulate<Transform>(cur, t, k, out + i);
        i += k;
    }
    return r.p - in;
}

} // namespace

size_t encodeZigzag128(const uint32_t* in, size_t n, uint8_t* out) {
    return encodeImpl<ZigzagDelta, 128, false>(in, n, out);
}
size_t encodeZigzag256(const uint32_t* in, size_t n, uint8_t* out) {
    return encodeImpl<ZigzagDelta, 256, true>(in, n, out);
}
size_t encodeDelta1_128(const uint32_t* in, size_t n, uint8_t* out) {
    return encodeImpl<Delta1, 128, false>(in, n, out);
}
size_t encodeDelta1_256(const uint32_t* in, size_t n, uint8_t* out) {
    return encodeImpl<Delta1, 256, true>(in, n, out);
}

size_t decodeZigzag128(const uint8_t* in, const uint8_t* end, size_t n, uint32_t* out) {
    return decodeImpl<ZigzagDelta, 128, false>(in, end, n, out);
}
size_t decodeZigzag256(const uint8_t* in, const uint8_t* end, size_t n, uint32_t* out) {
    return decodeImpl<ZigzagDelta, 256, true>(in, end, n, out);
}
size_t decodeDelta1_128(const uint8_t* in, const uint8_t* end, size_t n, uint32_t* out) {
    return decodeImpl<Delta1, 128, false>(in, end, n, out);
}
size_t decodeDelta1_256(const uint8_t* in, const uint8_t* end, size_t n, uint32_t* out) {
    return decodeImpl<Delta1, 256, true>(in, end, n, out);
}

} // namespace pfor
} // namespace util
} // namespace lucene
