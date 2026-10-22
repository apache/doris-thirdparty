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

inline uint32_t maskOf(uint32_t b) {
    return b >= 32 ? ~0u : (1u << b) - 1;
}

inline uint32_t load32(const uint8_t* p, size_t bytes) {
    uint32_t v = 0;
    memcpy(&v, p, bytes);
    return v;
}

// ---------------------------------------------------------------------------------
// Delta transforms
// ---------------------------------------------------------------------------------

struct ZigzagDelta {
    static uint32_t forward(uint32_t cur, uint32_t prev) {
        uint32_t d = cur - prev;
        return (d << 1) ^ (0u - (d >> 31));
    }
    // The signed difference a transformed value stands for.
    static uint32_t toDelta(uint32_t t) { return (t >> 1) ^ (0u - (t & 1)); }
};

struct Delta1 {
    static uint32_t forward(uint32_t cur, uint32_t prev) { return cur - prev - 1; }
    static uint32_t toDelta(uint32_t t) { return t + 1; }
};

// ---------------------------------------------------------------------------------
// Bit unpacking. A unit is 32 rows x L lanes of b-bit values stored in b*L words;
// word j of lane l is at j*L+l and rows are packed LSB first. With L == 1 this is the
// plain horizontal packing of 32 consecutive values, with L == 8 the vertical layout.
// ---------------------------------------------------------------------------------

template <int B, int L>
void unpackUnit(const uint8_t* in, uint32_t* out) {
    if constexpr (B == 0) {
        memset(out, 0, kGroup * L * sizeof(uint32_t));
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

// Packs 32 rows x L lanes (values at i*L+l) into b*L words.
void packUnit(const uint32_t* v, size_t lanes, uint32_t b, uint32_t* words) {
    const uint32_t mask = maskOf(b);
    for (size_t l = 0; l < lanes; ++l) {
        uint64_t acc = 0;
        uint32_t bits = 0;
        size_t j = 0;
        for (size_t i = 0; i < kGroup; ++i) {
            acc |= static_cast<uint64_t>(v[i * lanes + l] & mask) << bits;
            bits += b;
            if (bits >= 32) {
                words[j * lanes + l] = static_cast<uint32_t>(acc);
                ++j;
                acc >>= 32;
                bits -= 32;
            }
        }
    }
}

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
        packUnit(t, kLanes, b, words);
        memcpy(out, words, kGroup * b);
        return kGroup * b;
    }
    uint8_t* o = out;
    for (size_t g = 0; g < k; g += kGroup) {
        const size_t n = std::min(kGroup, k - g);
        uint32_t buf[kGroup] = {};
        memcpy(buf, t + g, n * sizeof(uint32_t));
        uint32_t words[32];
        packUnit(buf, 1, b, words);
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

inline size_t varintSize(uint32_t x) {
    return x < kVarint1 ? 1 : x < kVarint2 ? 2 : x < kVarint3 ? 3 : x < kVarint4 ? 4 : 5;
}

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

// Size of the hi parts section of a list block; sets `raw` if the raw form is the smaller.
size_t hiListSize(const uint32_t* hi, size_t n, bool& raw) {
    size_t varints = 0;
    for (size_t i = 0; i < n; ++i) {
        varints += varintSize(hi[i]);
    }
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

    uint32_t hist[33] = {};
    uint32_t maxBits = 0;
    for (size_t i = 0; i < k; ++i) {
        const uint32_t bl = bitLength(t[i]);
        ++hist[bl];
        maxBits = std::max(maxBits, bl);
    }

    // Choose the packed width and the way the values wider than it are patched.
    struct Plan {
        size_t cost;
        uint32_t mode;
        uint32_t b;
    };
    Plan best = {1 + packedSize(k, maxBits, vertical), kModePlain, maxBits};
    uint32_t exceptions = 0; // number of values wider than b
    for (int64_t b = static_cast<int64_t>(maxBits) - 1; b >= 0; --b) {
        exceptions += hist[b + 1];
        const uint32_t bx = maxBits - static_cast<uint32_t>(b);
        const size_t packed = packedSize(k, static_cast<uint32_t>(b), vertical);

        const size_t bitmapCost =
                2 + (k + 7) / 8 + (static_cast<size_t>(exceptions) * bx + 7) / 8 + packed;
        if (bitmapCost < best.cost) {
            best = {bitmapCost, kModeBitmap, static_cast<uint32_t>(b)};
        }
        const size_t listBase = 2 + packed + exceptions;
        if (exceptions <= 255 && listBase + exceptions < best.cost) {
            uint32_t hi[256];
            size_t n = 0;
            for (size_t i = 0; i < k; ++i) {
                if (t[i] >> b != 0) {
                    hi[n++] = t[i] >> b;
                }
            }
            bool raw;
            const size_t cost = listBase + hiListSize(hi, n, raw);
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
        hiListSize(hi, n, raw);
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
        size_t g = 0;
        const uint8_t* p = r.p;
        for (; g + kGroup <= k; g += kGroup, p += 4 * b) {
            kUnpackHorizontal[b](p, t + g);
        }
        if (g < k) {
            const size_t n = k - g;
            uint8_t buf[4 * 32] = {};
            memcpy(buf, p, (n * b + 7) / 8);
            uint32_t tmp[kGroup];
            kUnpackHorizontal[b](buf, tmp);
            memcpy(t + g, tmp, n * sizeof(uint32_t));
        }
    }
    r.p += bytes;
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
        const uint32_t v = load32(r.p, bytes);
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
        uint32_t hi[256];
        unpackValues(r, count, bx, false, hi);
        unpackValues(r, k, b, vertical, t);
        size_t n = 0;
        for (size_t byte = 0; byte < bitmapBytes; ++byte) {
            for (uint32_t bits = bitmap[byte]; bits != 0; bits &= bits - 1) {
                const size_t i = byte * 8 + __builtin_ctz(bits);
                if (i >= k) {
                    corrupt("PFOR: bad exception position");
                }
                t[i] |= hi[n++] << b;
            }
        }
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

// out[i] = cur + x[0] + ... + x[i] for i < k, returns the last value. The serial dependency of
// the running sum limits the speed, so large blocks are split in four segments whose sums are
// known up front and whose running sums then advance independently.
inline uint32_t prefixSum(uint32_t cur, const uint32_t* x, size_t k, uint32_t* out) {
    size_t i = 0;
    if (k >= 32) {
        const size_t seg = k / 4;
        uint32_t sum[3] = {0, 0, 0};
        for (size_t j = 0; j < 3; ++j) {
            for (size_t m = 0; m < seg; ++m) {
                sum[j] += x[j * seg + m];
            }
        }
        uint32_t a0 = cur;
        uint32_t a1 = a0 + sum[0];
        uint32_t a2 = a1 + sum[1];
        uint32_t a3 = a2 + sum[2];
        for (size_t m = 0; m < seg; ++m) {
            a0 += x[m];
            a1 += x[seg + m];
            a2 += x[2 * seg + m];
            a3 += x[3 * seg + m];
            out[m] = a0;
            out[seg + m] = a1;
            out[2 * seg + m] = a2;
            out[3 * seg + m] = a3;
        }
        cur = a3;
        i = 4 * seg;
    }
    for (; i < k; ++i) {
        cur += x[i];
        out[i] = cur;
    }
    return cur;
}

template <class Transform, size_t kBlock, bool kVertical>
size_t decodeImpl(const uint8_t* in, const uint8_t* end, size_t n, uint32_t* out) {
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
        for (size_t j = 0; j < k; ++j) {
            t[j] = Transform::toDelta(t[j]);
        }
        cur = prefixSum(cur, t, k, out + i);
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
