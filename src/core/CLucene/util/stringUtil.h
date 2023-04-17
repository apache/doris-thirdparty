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
        const auto src_end_sse = src_end - (src_end - src) % bytes_sse;

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

static void to_lower(const uint8_t* src, int64_t len, uint8_t* dst) {
    if (len <= 0) {
        return;
    }
    LowerUpperImpl<'A', 'Z'>::transfer(src, src + len, dst);
}

static void to_upper(const uint8_t* src, int64_t len, uint8_t* dst) {
    if (len <= 0) {
        return;
    }
    LowerUpperImpl<'a', 'z'> lowerUpper;
    LowerUpperImpl<'a', 'z'>::transfer(src, src + len, dst);
}
#endif//_lucene_util__stringutil_H
