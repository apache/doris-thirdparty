//
// Created by 姜凯 on 2022/9/20.
//

#ifndef _lucene_util__stringutil_H
#define _lucene_util__stringutil_H

#ifdef __SSE2__
#include <emmintrin.h>
#elif __aarch64__
#include <sse2neon.h>
#endif

#include <cstring>
#include <assert.h>
#include "SSEUtil.h"

template <typename T>
const T* LUCENE_BLANK_SSTRING();

template<typename T>
void strnCopy(T *dst, const T *src, size_t size);

template<typename T>
void strCopy(T *dst, const T *src);

template<typename T>
int strCompare(const T *leftStr, const T *rightStr);

template<typename T>
T *strDuplicate(const T *str);

template<typename T>
size_t lenOfString(const T *str);

template <char not_case_lower_bound, char not_case_upper_bound>
class LowerUpperImpl {
public:
    static void transfer(const uint8_t* src, const uint8_t* src_end, uint8_t* dst) {
        const auto flip_case_mask = 'A' ^ 'a';

#if defined(__SSE2__) || defined(__aarch64__)
        const auto bytes_sse = sizeof(__m128i);
        const auto src_end_sse = src_end - size_t(src_end - src) % bytes_sse;

        const auto v_not_case_lower_bound = _mm_set1_epi8(not_case_lower_bound - 1);
        const auto v_not_case_upper_bound = _mm_set1_epi8(not_case_upper_bound + 1);
        const auto v_flip_case_mask = _mm_set1_epi8(flip_case_mask);

        for (; src < src_end_sse; src += bytes_sse, dst += bytes_sse) {
            const auto chars = _mm_loadu_si128(reinterpret_cast<const __m128i*>(src));
            const auto is_not_case = _mm_and_si128(_mm_cmpgt_epi8(chars, v_not_case_lower_bound),
                                                   _mm_cmplt_epi8(chars, v_not_case_upper_bound));
            const auto xor_mask = _mm_and_si128(v_flip_case_mask, is_not_case);
            const auto cased_chars = _mm_xor_si128(chars, xor_mask);
            _mm_storeu_si128(reinterpret_cast<__m128i*>(dst), cased_chars);
        }
#endif

        for (; src < src_end; ++src, ++dst)
            if (*src >= not_case_lower_bound && *src <= not_case_upper_bound)
                *dst = *src ^ flip_case_mask;
            else
                *dst = *src;
    }
};

[[maybe_unused]] static void to_lower(const uint8_t* src, int64_t len, uint8_t* dst) {
    if (len <= 0) {
        return;
    }
    LowerUpperImpl<'A', 'Z'>::transfer(src, src + len, dst);
}

[[maybe_unused]] static void to_upper(const uint8_t* src, int64_t len, uint8_t* dst) {
    if (len <= 0) {
        return;
    }
    LowerUpperImpl<'a', 'z'>::transfer(src, src + len, dst);
}

[[maybe_unused]] static inline bool is_alnum(uint8_t c) {
    static constexpr uint8_t LUT[256] = {
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
            0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
            1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
            1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    };
    return (bool)LUT[c];
}

[[maybe_unused]] static inline char to_lower(uint8_t c) {
    static constexpr uint8_t LUT[256] = {
            0,   1,   2,   3,   4,   5,   6,   7,   8,   9,   10,  11,  12,  13,  14,  15,
            16,  17,  18,  19,  20,  21,  22,  23,  24,  25,  26,  27,  28,  29,  30,  31,
            32,  33,  34,  35,  36,  37,  38,  39,  40,  41,  42,  43,  44,  45,  46,  47,
            48,  49,  50,  51,  52,  53,  54,  55,  56,  57,  58,  59,  60,  61,  62,  63,
            64,  97,  98,  99,  100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111,
            112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 91,  92,  93,  94,  95,
            96,  97,  98,  99,  100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111,
            112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127,
            128, 129, 130, 131, 132, 133, 134, 135, 136, 137, 138, 139, 140, 141, 142, 143,
            144, 145, 146, 147, 148, 149, 150, 151, 152, 153, 154, 155, 156, 157, 158, 159,
            160, 161, 162, 163, 164, 165, 166, 167, 168, 169, 170, 171, 172, 173, 174, 175,
            176, 177, 178, 179, 180, 181, 182, 183, 184, 185, 186, 187, 188, 189, 190, 191,
            192, 193, 194, 195, 196, 197, 198, 199, 200, 201, 202, 203, 204, 205, 206, 207,
            208, 209, 210, 211, 212, 213, 214, 215, 216, 217, 218, 219, 220, 221, 222, 223,
            224, 225, 226, 227, 228, 229, 230, 231, 232, 233, 234, 235, 236, 237, 238, 239,
            240, 241, 242, 243, 244, 245, 246, 247, 248, 249, 250, 251, 252, 253, 254, 255};
    return (char)LUT[c];
}

class StringUtil {
public:
    template <typename T>
    static inline T unaligned_load(const void* address) {
        T res {};
        memcpy(&res, address, sizeof(res));
        return res;
    }

#if defined(__SSE2__)

    static inline bool compareSSE2(const char* p1, const char* p2) {
        return 0xFFFF == _mm_movemask_epi8(_mm_cmpeq_epi8(
                                 _mm_loadu_si128(reinterpret_cast<const __m128i*>(p1)),
                                 _mm_loadu_si128(reinterpret_cast<const __m128i*>(p2))));
    }

    static inline bool compareSSE2x4(const char* p1, const char* p2) {
        return 0xFFFF ==
               _mm_movemask_epi8(_mm_and_si128(
                       _mm_and_si128(
                               _mm_cmpeq_epi8(
                                       _mm_loadu_si128(reinterpret_cast<const __m128i*>(p1)),
                                       _mm_loadu_si128(reinterpret_cast<const __m128i*>(p2))),
                               _mm_cmpeq_epi8(
                                       _mm_loadu_si128(reinterpret_cast<const __m128i*>(p1) + 1),
                                       _mm_loadu_si128(reinterpret_cast<const __m128i*>(p2) + 1))),
                       _mm_and_si128(
                               _mm_cmpeq_epi8(
                                       _mm_loadu_si128(reinterpret_cast<const __m128i*>(p1) + 2),
                                       _mm_loadu_si128(reinterpret_cast<const __m128i*>(p2) + 2)),
                               _mm_cmpeq_epi8(
                                       _mm_loadu_si128(reinterpret_cast<const __m128i*>(p1) + 3),
                                       _mm_loadu_si128(reinterpret_cast<const __m128i*>(p2) +
                                                       3)))));
    }

    static inline bool memequalSSE2Wide(const char* p1, const char* p2, size_t size) {
        if (size <= 16) {
            if (size >= 8) {
                /// Chunks of [8,16] bytes.
                return unaligned_load<uint64_t>(p1) == unaligned_load<uint64_t>(p2) &&
                       unaligned_load<uint64_t>(p1 + size - 8) ==
                               unaligned_load<uint64_t>(p2 + size - 8);
            } else if (size >= 4) {
                /// Chunks of [4,7] bytes.
                return unaligned_load<uint32_t>(p1) == unaligned_load<uint32_t>(p2) &&
                       unaligned_load<uint32_t>(p1 + size - 4) ==
                               unaligned_load<uint32_t>(p2 + size - 4);
            } else if (size >= 2) {
                /// Chunks of [2,3] bytes.
                return unaligned_load<uint16_t>(p1) == unaligned_load<uint16_t>(p2) &&
                       unaligned_load<uint16_t>(p1 + size - 2) ==
                               unaligned_load<uint16_t>(p2 + size - 2);
            } else if (size >= 1) {
                /// A single byte.
                return *p1 == *p2;
            }
            return true;
        }

        while (size >= 64) {
            if (compareSSE2x4(p1, p2)) {
                p1 += 64;
                p2 += 64;
                size -= 64;
            } else {
                return false;
            }
        }

        switch (size / 16) {
        case 3:
            if (!compareSSE2(p1 + 32, p2 + 32)) {
                return false;
            }
            [[fallthrough]];
        case 2:
            if (!compareSSE2(p1 + 16, p2 + 16)) {
                return false;
            }
            [[fallthrough]];
        case 1:
            if (!compareSSE2(p1, p2)) {
                return false;
            }
        }

        return compareSSE2(p1 + size - 16, p2 + size - 16);
    }

#endif

    // Compare two strings using sse4.2 intrinsics if they are available. This code assumes
    // that the trivial cases are already handled (i.e. one string is empty).
    // Returns:
    //   < 0 if s1 < s2
    //   0 if s1 == s2
    //   > 0 if s1 > s2
    // The SSE code path is just under 2x faster than the non-sse code path.
    //   - s1/n1: ptr/len for the first string
    //   - s2/n2: ptr/len for the second string
    //   - len: min(n1, n2) - this can be more cheaply passed in by the caller
    static inline int string_compare(const char* s1, int64_t n1, const char* s2, int64_t n2, int64_t len) {
        assert(len == std::min(n1, n2));
    #if defined(__SSE4_2__) || defined(__aarch64__)
        while (len >= sse_util::CHARS_PER_128_BIT_REGISTER) {
            __m128i xmm0 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(s1));
            __m128i xmm1 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(s2));
            int chars_match = _mm_cmpestri(xmm0, sse_util::CHARS_PER_128_BIT_REGISTER, xmm1,
                                           sse_util::CHARS_PER_128_BIT_REGISTER, sse_util::STRCMP_MODE);
            if (chars_match != sse_util::CHARS_PER_128_BIT_REGISTER) {
                return (unsigned char)s1[chars_match] - (unsigned char)s2[chars_match];
            }
            len -= sse_util::CHARS_PER_128_BIT_REGISTER;
            s1 += sse_util::CHARS_PER_128_BIT_REGISTER;
            s2 += sse_util::CHARS_PER_128_BIT_REGISTER;
        }
    #endif
        unsigned char u1, u2;
        while (len-- > 0) {
            u1 = (unsigned char)*s1++;
            u2 = (unsigned char)*s2++;
            if (u1 != u2) {
                return u1 - u2;
            }
        }

        return int(n1 - n2);
    }

    static inline int32_t utf8_byte_count(uint8_t c) {
        static constexpr int32_t LUT[256] = {
            1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,
            1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,
            1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,
            1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,
            1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,
            1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,
            1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,
            1,  1,  1,  1,  1,  1,  1,  1,  1,  -1, -1, -1, -1, -1, -1, -1, -1,
            -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
            -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
            -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
            -1, -1, -1, -1, -1, 2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,
            2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,
            2,  2,  2,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,
            3,  3,  4,  4,  4,  4,  4,  4,  4,  4,  -1, -1, -1, -1, -1, -1, -1};
        return LUT[c];
    }

    static inline bool is_valid_codepoint(uint32_t code_point) {
        return code_point < 0xD800u ||
               (code_point >= 0xE000u && code_point <= 0x10FFFFu);
    }

    static inline int32_t validate_utf8(const std::string_view& str) {
        int32_t bytes_in_char = 0;
        int32_t surplus_bytes = 0;
        uint32_t codepoint = 0;
        for (auto cc : str) {
            char c = (char)cc;
            if (bytes_in_char == 0) {
                if ((c & 0x80) == 0) {
                    codepoint = (uint32_t)c;
                    continue;
                } else if ((c & 0xE0) == 0xC0) {
                    codepoint = c & 0x1F;
                    bytes_in_char = 1;
                } else if ((c & 0xF0) == 0xE0) {
                    codepoint = c & 0x0F;
                    bytes_in_char = 2;
                } else if ((c & 0xF8) == 0xF0) {
                    codepoint = c & 0x07;
                    bytes_in_char = 3;
                } else {
                    return -1;
                }
                surplus_bytes = 1;
            } else {
                if ((c & 0xC0) != 0x80) return -1;
                codepoint = (codepoint << 6) | (c & 0x3F);
                bytes_in_char--;
                if (bytes_in_char == 0 && !is_valid_codepoint(codepoint)) {
                    return -1;
                }
                surplus_bytes++;
            }
        }
        return bytes_in_char == 0 ? 0 : surplus_bytes;
    }

    // utf8: 1-4 char = 1 wchar_t, invalid utf8: 1 char = 1 wchar_t
    static inline std::wstring string_to_wstring(const std::string_view& utf8_str) {
        std::wstring wstr;
        wstr.reserve(utf8_str.size());
        size_t i = 0;
        while (i < utf8_str.size()) {
            wchar_t wc = utf8_str[i];
            int32_t n = utf8_byte_count((uint8_t)utf8_str[i]);
            if ((n >= 1 && n <= 4) &&
                (i + (size_t)n <= utf8_str.size()) &&
                validate_utf8(std::string_view(utf8_str.data() + i, (size_t)n)) == 0) {
                if (n == 2) {
                    wc = ((utf8_str[i] & 0x1F) << 6) | (utf8_str[i + 1] & 0x3F);
                } else if (n == 3) {
                    wc = ((utf8_str[i] & 0x0F) << 12) | ((utf8_str[i + 1] & 0x3F) << 6) | (utf8_str[i + 2] & 0x3F);
                } else if (n == 4) {
                    wc = ((utf8_str[i] & 0x07) << 18) | ((utf8_str[i + 1] & 0x3F) << 12) | ((utf8_str[i + 2] & 0x3F) << 6) | (utf8_str[i + 3] & 0x3F);
                }
                i += (size_t)n;
            } else {
                i += 1;
            }
            wstr.push_back(wc);
        }
        return wstr;
    }
};

#endif//_lucene_util__stringutil_H
