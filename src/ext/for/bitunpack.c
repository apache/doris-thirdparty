/**
    Copyright (C) powturbo 2013-2019
    GPL v2 License

    This program is free software; you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation; either version 2 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License along
    with this program; if not, write to the Free Software Foundation, Inc.,
    51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

    - homepage : https://sites.google.com/site/powturbo/
    - github   : https://github.com/powturbo
    - twitter  : https://twitter.com/powturbo
    - email    : powturbo [_AT_] gmail [_DOT_] com
**/
//   "Integer Compression" Bit Packing
#define BITUTIL_IN
#define VINT_IN
#include "conf.h"
#include "bitutil.h"
#include "bitpack.h"
#include "vint.h"
#include <string.h> 

#define PAD8(_x_) (((_x_)+7)/8)

#pragma warning( disable : 4005)
#pragma warning( disable : 4090)
#pragma warning( disable : 4068)

#pragma GCC push_options
#pragma GCC optimize ("align-functions=16")
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunsequenced"

#if !defined(SSE2_ON) && !defined(AVX2_ON) //----------------------------------- Plain -------------------------------------------------------------------------------------------
typedef unsigned char *(*BITUNPACK_F8)( const unsigned char *__restrict in, unsigned n, uint8_t  *__restrict out);
typedef unsigned char *(*BITUNPACK_D8)( const unsigned char *__restrict in, unsigned n, uint8_t  *__restrict out, uint8_t start);
typedef unsigned char *(*BITUNPACK_F16)(const unsigned char *__restrict in, unsigned n, uint16_t *__restrict out);
typedef unsigned char *(*BITUNPACK_D16)(const unsigned char *__restrict in, unsigned n, uint16_t *__restrict out, uint16_t start);
typedef unsigned char *(*BITUNPACK_F32)(const unsigned char *__restrict in, unsigned n, uint32_t *__restrict out);
typedef unsigned char *(*BITUNPACK_D32)(const unsigned char *__restrict in, unsigned n, uint32_t *__restrict out, uint32_t start);
typedef unsigned char *(*BITUNPACK_F64)(const unsigned char *__restrict in, unsigned n, uint64_t *__restrict out);
typedef unsigned char *(*BITUNPACK_D64)(const unsigned char *__restrict in, unsigned n, uint64_t *__restrict out, uint64_t start);

  #if 0   //????
#define OP(_op_, _x_) *_op_++
#define OPX(_op_)
  #else
#define OP(_op_, _x_) _op_[_x_]
#define OPX(_op_)     _op_ += 32
  #endif

#define OPI(_op_,_nb_,_parm_) OPX(_op_)
#define OUT( _op_, _x_, _w_, _nb_,_parm_) OP(_op_,_x_) = _w_
#define _BITUNPACK_ bitunpack
#include "bitunpack_.h"

#define DELTA

#define OUT( _op_, _x_, _w_, _nb_,_parm_) OP(_op_,_x_) = (_parm_ += (_w_))
#define _BITUNPACK_ bitdunpack  // delta + 0
#include "bitunpack_.h"

#define OUT( _op_, _x_, _w_, _nb_,_parm_) OP(_op_,_x_) = (_parm_ += TEMPLATE2(zigzagdec, USIZE)(_w_))
#define _BITUNPACK_ bitzunpack  // zigzag
#include "bitunpack_.h"

#define OUT( _op_, _x_, _w_, _nb_,_parm_) OP(_op_,_x_) = (_parm_ + (_w_))
#define _BITUNPACK_ bitfunpack  // for
#include "bitunpack_.h"

#define OPI(_op_,_nb_,_parm_) OPX(_op_); _parm_ += 32
#define OUT( _op_, _x_, _w_, _nb_,_parm_) OP(_op_,_x_) = (_parm_ += (_w_)) + (_x_+1)
#define _BITUNPACK_ bitd1unpack  // delta + 1
#include "bitunpack_.h"

#define OUT( _op_, _x_, _w_, _nb_,_parm_) OP(_op_,_x_) = _parm_ + (_w_)+(_x_+1)
#define _BITUNPACK_ bitf1unpack  // for + 1
#include "bitunpack_.h"
#undef OPI

#define BITNUNPACK(in, n, out, _csize_, _usize_) {\
  unsigned char *ip = in;\
  for(op = out,out+=n; op < out;) { unsigned oplen = out - op,b; if(oplen > _csize_) oplen = _csize_;       PREFETCH(ip+512,0);\
    b = *ip++; ip = TEMPLATE2(bitunpacka, _usize_)[b](ip, oplen, op);\
    op += oplen;\
  } \
  return ip - in;\
}

#define BITNDUNPACK(in, n, out, _csize_, _usize_, _bitunpacka_) { if(!n) return 0;\
  unsigned char *ip = in;\
  TEMPLATE2(vbxget, _usize_)(ip, start);\
  for(*out++ = start,--n,op = out; op != out+(n&~(_csize_-1)); ) {                              PREFETCH(ip+512,0);\
                         unsigned b = *ip++; ip = TEMPLATE2(_bitunpacka_, _usize_)[b](ip, _csize_, op, start); op += _csize_; start = op[-1];\
  } if(n&=(_csize_-1)) { unsigned b = *ip++; ip = TEMPLATE2(_bitunpacka_, _usize_)[b](ip, n,       op, start); }\
  return ip - in;\
}

size_t bitnunpack8(   unsigned char *__restrict in, size_t n, uint8_t  *__restrict out) { uint8_t  *op; BITNUNPACK(in, n, out, 128,  8); }
size_t bitnunpack16(  unsigned char *__restrict in, size_t n, uint16_t *__restrict out) { uint16_t *op; BITNUNPACK(in, n, out, 128, 16); }
size_t bitnunpack32(  unsigned char *__restrict in, size_t n, uint32_t *__restrict out) { uint32_t *op; BITNUNPACK(in, n, out, 128, 32); }
size_t bitnunpack64(  unsigned char *__restrict in, size_t n, uint64_t *__restrict out) { uint64_t *op; BITNUNPACK(in, n, out, 128, 64); }

size_t bitndunpack8(  unsigned char *__restrict in, size_t n, uint8_t  *__restrict out) { uint8_t  *op,start; BITNDUNPACK(in, n, out, 128,  8, bitdunpacka); }
size_t bitndunpack16( unsigned char *__restrict in, size_t n, uint16_t *__restrict out) { uint16_t *op,start; BITNDUNPACK(in, n, out, 128, 16, bitdunpacka); }
size_t bitndunpack32( unsigned char *__restrict in, size_t n, uint32_t *__restrict out) { uint32_t *op,start; BITNDUNPACK(in, n, out, 128, 32, bitdunpacka); }
size_t bitndunpack64( unsigned char *__restrict in, size_t n, uint64_t *__restrict out) { uint64_t *op,start; BITNDUNPACK(in, n, out, 128, 64, bitdunpacka); }

size_t bitnd1unpack8( unsigned char *__restrict in, size_t n, uint8_t  *__restrict out) { uint8_t  *op,start; BITNDUNPACK(in, n, out, 128,  8, bitd1unpacka); }
size_t bitnd1unpack16(unsigned char *__restrict in, size_t n, uint16_t *__restrict out) { uint16_t *op,start; BITNDUNPACK(in, n, out, 128, 16, bitd1unpacka); }
size_t bitnd1unpack32(unsigned char *__restrict in, size_t n, uint32_t *__restrict out) { uint32_t *op,start; BITNDUNPACK(in, n, out, 128, 32, bitd1unpacka); }
size_t bitnd1unpack64(unsigned char *__restrict in, size_t n, uint64_t *__restrict out) { uint64_t *op,start; BITNDUNPACK(in, n, out, 128, 64, bitd1unpacka); }

size_t bitnzunpack8(  unsigned char *__restrict in, size_t n, uint8_t  *__restrict out) { uint8_t  *op,start; BITNDUNPACK(in, n, out, 128,  8, bitzunpacka); }
size_t bitnzunpack16( unsigned char *__restrict in, size_t n, uint16_t *__restrict out) { uint16_t *op,start; BITNDUNPACK(in, n, out, 128, 16, bitzunpacka); }
size_t bitnzunpack32( unsigned char *__restrict in, size_t n, uint32_t *__restrict out) { uint32_t *op,start; BITNDUNPACK(in, n, out, 128, 32, bitzunpacka); }
size_t bitnzunpack64( unsigned char *__restrict in, size_t n, uint64_t *__restrict out) { uint64_t *op,start; BITNDUNPACK(in, n, out, 128, 64, bitzunpacka); }

size_t bitnfunpack8(  unsigned char *__restrict in, size_t n, uint8_t  *__restrict out) { uint8_t  *op,start; BITNDUNPACK(in, n, out, 128,  8, bitfunpacka); }
size_t bitnfunpack16( unsigned char *__restrict in, size_t n, uint16_t *__restrict out) { uint16_t *op,start; BITNDUNPACK(in, n, out, 128, 16, bitfunpacka); }
size_t bitnfunpack32( unsigned char *__restrict in, size_t n, uint32_t *__restrict out) { uint32_t *op,start; BITNDUNPACK(in, n, out, 128, 32, bitfunpacka); }
size_t bitnfunpack64( unsigned char *__restrict in, size_t n, uint64_t *__restrict out) { uint64_t *op,start; BITNDUNPACK(in, n, out, 128, 64, bitfunpacka); }

#else //-------------------------------------------- SSE/AVX2 ---------------------------------------------------------------------------------------

#define _BITNUNPACKV(in, n, out, _csize_, _usize_, _bitunpackv_) {\
  unsigned char *ip = in;\
  for(op = out; op != out+(n&~(_csize_-1)); op += _csize_) {                                                    PREFETCH(in+512,0);\
                         unsigned b = *ip++; ip = TEMPLATE2(_bitunpackv_, _usize_)(ip, _csize_, op,b);\
  } if(n&=(_csize_-1)) { unsigned b = *ip++; ip = TEMPLATE2(bitunpack,    _usize_)(ip, n,       op,b); }\
  return ip - in;\
}

#define _BITNDUNPACKV(in, n, out, _csize_, _usize_, _bitunpackv_, _bitunpack_) { if(!n) return 0;\
  unsigned char *ip = in;\
  TEMPLATE2(vbxget, _usize_)(ip, start); \
  *out++ = start;\
  for(--n,op = out; op != out+(n&~(_csize_-1)); ) {                                 PREFETCH(ip+512,0);\
                         unsigned b = *ip++; ip = TEMPLATE2(_bitunpackv_, _usize_)(ip, _csize_, op, start,b); op += _csize_; start = op[-1];\
  } if(n&=(_csize_-1)) { unsigned b = *ip++; ip = TEMPLATE2(_bitunpack_,  _usize_)(ip, n,     op, start,b); }\
  return ip - in;\
}
  #ifdef __AVX2__ //-------------------------------- AVX2 ----------------------------------------------------------------------------
#include <immintrin.h>

    #ifdef __AVX512F__
#define mm256_maskz_expand_epi32(_m_,_v_) _mm256_maskz_expand_epi32(_m_,_v_)
#define mm256_maskz_loadu_epi32( _m_,_v_) _mm256_maskz_loadu_epi32( _m_,_v_)
    #else
#if !(defined(_M_X64) || defined(__amd64__)) && (defined(__i386__) || defined(_M_IX86)) && !defined(__clang__)
static inline __m128i _mm_cvtsi64_si128(__int64 a) {  return _mm_loadl_epi64((__m128i*)&a); }
    #endif
static ALIGNED(unsigned char, permv[256][8], 32) = {
0,0,0,0,0,0,0,0,
0,1,1,1,1,1,1,1,
1,0,1,1,1,1,1,1,
0,1,2,2,2,2,2,2,
1,1,0,1,1,1,1,1,
0,2,1,2,2,2,2,2,
2,0,1,2,2,2,2,2,
0,1,2,3,3,3,3,3,
1,1,1,0,1,1,1,1,
0,2,2,1,2,2,2,2,
2,0,2,1,2,2,2,2,
0,1,3,2,3,3,3,3,
2,2,0,1,2,2,2,2,
0,3,1,2,3,3,3,3,
3,0,1,2,3,3,3,3,
0,1,2,3,4,4,4,4,
1,1,1,1,0,1,1,1,
0,2,2,2,1,2,2,2,
2,0,2,2,1,2,2,2,
0,1,3,3,2,3,3,3,
2,2,0,2,1,2,2,2,
0,3,1,3,2,3,3,3,
3,0,1,3,2,3,3,3,
0,1,2,4,3,4,4,4,
2,2,2,0,1,2,2,2,
0,3,3,1,2,3,3,3,
3,0,3,1,2,3,3,3,
0,1,4,2,3,4,4,4,
3,3,0,1,2,3,3,3,
0,4,1,2,3,4,4,4,
4,0,1,2,3,4,4,4,
0,1,2,3,4,5,5,5,
1,1,1,1,1,0,1,1,
0,2,2,2,2,1,2,2,
2,0,2,2,2,1,2,2,
0,1,3,3,3,2,3,3,
2,2,0,2,2,1,2,2,
0,3,1,3,3,2,3,3,
3,0,1,3,3,2,3,3,
0,1,2,4,4,3,4,4,
2,2,2,0,2,1,2,2,
0,3,3,1,3,2,3,3,
3,0,3,1,3,2,3,3,
0,1,4,2,4,3,4,4,
3,3,0,1,3,2,3,3,
0,4,1,2,4,3,4,4,
4,0,1,2,4,3,4,4,
0,1,2,3,5,4,5,5,
2,2,2,2,0,1,2,2,
0,3,3,3,1,2,3,3,
3,0,3,3,1,2,3,3,
0,1,4,4,2,3,4,4,
3,3,0,3,1,2,3,3,
0,4,1,4,2,3,4,4,
4,0,1,4,2,3,4,4,
0,1,2,5,3,4,5,5,
3,3,3,0,1,2,3,3,
0,4,4,1,2,3,4,4,
4,0,4,1,2,3,4,4,
0,1,5,2,3,4,5,5,
4,4,0,1,2,3,4,4,
0,5,1,2,3,4,5,5,
5,0,1,2,3,4,5,5,
0,1,2,3,4,5,6,6,
1,1,1,1,1,1,0,1,
0,2,2,2,2,2,1,2,
2,0,2,2,2,2,1,2,
0,1,3,3,3,3,2,3,
2,2,0,2,2,2,1,2,
0,3,1,3,3,3,2,3,
3,0,1,3,3,3,2,3,
0,1,2,4,4,4,3,4,
2,2,2,0,2,2,1,2,
0,3,3,1,3,3,2,3,
3,0,3,1,3,3,2,3,
0,1,4,2,4,4,3,4,
3,3,0,1,3,3,2,3,
0,4,1,2,4,4,3,4,
4,0,1,2,4,4,3,4,
0,1,2,3,5,5,4,5,
2,2,2,2,0,2,1,2,
0,3,3,3,1,3,2,3,
3,0,3,3,1,3,2,3,
0,1,4,4,2,4,3,4,
3,3,0,3,1,3,2,3,
0,4,1,4,2,4,3,4,
4,0,1,4,2,4,3,4,
0,1,2,5,3,5,4,5,
3,3,3,0,1,3,2,3,
0,4,4,1,2,4,3,4,
4,0,4,1,2,4,3,4,
0,1,5,2,3,5,4,5,
4,4,0,1,2,4,3,4,
0,5,1,2,3,5,4,5,
5,0,1,2,3,5,4,5,
0,1,2,3,4,6,5,6,
2,2,2,2,2,0,1,2,
0,3,3,3,3,1,2,3,
3,0,3,3,3,1,2,3,
0,1,4,4,4,2,3,4,
3,3,0,3,3,1,2,3,
0,4,1,4,4,2,3,4,
4,0,1,4,4,2,3,4,
0,1,2,5,5,3,4,5,
3,3,3,0,3,1,2,3,
0,4,4,1,4,2,3,4,
4,0,4,1,4,2,3,4,
0,1,5,2,5,3,4,5,
4,4,0,1,4,2,3,4,
0,5,1,2,5,3,4,5,
5,0,1,2,5,3,4,5,
0,1,2,3,6,4,5,6,
3,3,3,3,0,1,2,3,
0,4,4,4,1,2,3,4,
4,0,4,4,1,2,3,4,
0,1,5,5,2,3,4,5,
4,4,0,4,1,2,3,4,
0,5,1,5,2,3,4,5,
5,0,1,5,2,3,4,5,
0,1,2,6,3,4,5,6,
4,4,4,0,1,2,3,4,
0,5,5,1,2,3,4,5,
5,0,5,1,2,3,4,5,
0,1,6,2,3,4,5,6,
5,5,0,1,2,3,4,5,
0,6,1,2,3,4,5,6,
6,0,1,2,3,4,5,6,
0,1,2,3,4,5,6,7,
1,1,1,1,1,1,1,0,
0,2,2,2,2,2,2,1,
2,0,2,2,2,2,2,1,
0,1,3,3,3,3,3,2,
2,2,0,2,2,2,2,1,
0,3,1,3,3,3,3,2,
3,0,1,3,3,3,3,2,
0,1,2,4,4,4,4,3,
2,2,2,0,2,2,2,1,
0,3,3,1,3,3,3,2,
3,0,3,1,3,3,3,2,
0,1,4,2,4,4,4,3,
3,3,0,1,3,3,3,2,
0,4,1,2,4,4,4,3,
4,0,1,2,4,4,4,3,
0,1,2,3,5,5,5,4,
2,2,2,2,0,2,2,1,
0,3,3,3,1,3,3,2,
3,0,3,3,1,3,3,2,
0,1,4,4,2,4,4,3,
3,3,0,3,1,3,3,2,
0,4,1,4,2,4,4,3,
4,0,1,4,2,4,4,3,
0,1,2,5,3,5,5,4,
3,3,3,0,1,3,3,2,
0,4,4,1,2,4,4,3,
4,0,4,1,2,4,4,3,
0,1,5,2,3,5,5,4,
4,4,0,1,2,4,4,3,
0,5,1,2,3,5,5,4,
5,0,1,2,3,5,5,4,
0,1,2,3,4,6,6,5,
2,2,2,2,2,0,2,1,
0,3,3,3,3,1,3,2,
3,0,3,3,3,1,3,2,
0,1,4,4,4,2,4,3,
3,3,0,3,3,1,3,2,
0,4,1,4,4,2,4,3,
4,0,1,4,4,2,4,3,
0,1,2,5,5,3,5,4,
3,3,3,0,3,1,3,2,
0,4,4,1,4,2,4,3,
4,0,4,1,4,2,4,3,
0,1,5,2,5,3,5,4,
4,4,0,1,4,2,4,3,
0,5,1,2,5,3,5,4,
5,0,1,2,5,3,5,4,
0,1,2,3,6,4,6,5,
3,3,3,3,0,1,3,2,
0,4,4,4,1,2,4,3,
4,0,4,4,1,2,4,3,
0,1,5,5,2,3,5,4,
4,4,0,4,1,2,4,3,
0,5,1,5,2,3,5,4,
5,0,1,5,2,3,5,4,
0,1,2,6,3,4,6,5,
4,4,4,0,1,2,4,3,
0,5,5,1,2,3,5,4,
5,0,5,1,2,3,5,4,
0,1,6,2,3,4,6,5,
5,5,0,1,2,3,5,4,
0,6,1,2,3,4,6,5,
6,0,1,2,3,4,6,5,
0,1,2,3,4,5,7,6,
2,2,2,2,2,2,0,1,
0,3,3,3,3,3,1,2,
3,0,3,3,3,3,1,2,
0,1,4,4,4,4,2,3,
3,3,0,3,3,3,1,2,
0,4,1,4,4,4,2,3,
4,0,1,4,4,4,2,3,
0,1,2,5,5,5,3,4,
3,3,3,0,3,3,1,2,
0,4,4,1,4,4,2,3,
4,0,4,1,4,4,2,3,
0,1,5,2,5,5,3,4,
4,4,0,1,4,4,2,3,
0,5,1,2,5,5,3,4,
5,0,1,2,5,5,3,4,
0,1,2,3,6,6,4,5,
3,3,3,3,0,3,1,2,
0,4,4,4,1,4,2,3,
4,0,4,4,1,4,2,3,
0,1,5,5,2,5,3,4,
4,4,0,4,1,4,2,3,
0,5,1,5,2,5,3,4,
5,0,1,5,2,5,3,4,
0,1,2,6,3,6,4,5,
4,4,4,0,1,4,2,3,
0,5,5,1,2,5,3,4,
5,0,5,1,2,5,3,4,
0,1,6,2,3,6,4,5,
5,5,0,1,2,5,3,4,
0,6,1,2,3,6,4,5,
6,0,1,2,3,6,4,5,
0,1,2,3,4,7,5,6,
3,3,3,3,3,0,1,2,
0,4,4,4,4,1,2,3,
4,0,4,4,4,1,2,3,
0,1,5,5,5,2,3,4,
4,4,0,4,4,1,2,3,
0,5,1,5,5,2,3,4,
5,0,1,5,5,2,3,4,
0,1,2,6,6,3,4,5,
4,4,4,0,4,1,2,3,
0,5,5,1,5,2,3,4,
5,0,5,1,5,2,3,4,
0,1,6,2,6,3,4,5,
5,5,0,1,5,2,3,4,
0,6,1,2,6,3,4,5,
6,0,1,2,6,3,4,5,
0,1,2,3,7,4,5,6,
4,4,4,4,0,1,2,3,
0,5,5,5,1,2,3,4,
5,0,5,5,1,2,3,4,
0,1,6,6,2,3,4,5,
5,5,0,5,1,2,3,4,
0,6,1,6,2,3,4,5,
6,0,1,6,2,3,4,5,
0,1,2,7,3,4,5,6,
5,5,5,0,1,2,3,4,
0,6,6,1,2,3,4,5,
6,0,6,1,2,3,4,5,
0,1,7,2,3,4,5,6,
6,6,0,1,2,3,4,5,
0,7,1,2,3,4,5,6,
7,0,1,2,3,4,5,6,
0,1,2,3,4,5,6,7
};
#define u2vmask(_m_,_tv_)                  _mm256_sllv_epi32(_mm256_set1_epi8(_m_), _tv_)
#define mm256_maskz_expand_epi32(_m_, _v_) _mm256_permutevar8x32_epi32(_v_,  _mm256_cvtepu8_epi32(_mm_cvtsi64_si128(ctou64(permv[_m_]))) )
#define mm256_maskz_loadu_epi32(_m_,_v_)   _mm256_blendv_epi8(zv, mm256_maskz_expand_epi32(xm, _mm256_loadu_si256((__m256i*)pex)), u2vmask(xm,tv)) // emulate AVX512 _mm256_maskz_loadu_epi32 on AVX2 
  #endif

//-----------------------------------------------------------------------------
#define VO32( _op_, _i_, ov, _nb_,_parm_) _mm256_storeu_si256(_op_++, ov)
#define VOZ32(_op_, _i_, ov, _nb_,_parm_) _mm256_storeu_si256(_op_++, _parm_)
#include "bitunpack_.h"

#define BITUNBLK256V32_0(ip, _i_, _op_, _nb_,_parm_) {__m256i ov;\
  VOZ32(_op_,  0, ov, _nb_,_parm_);\
  VOZ32(_op_,  1, ov, _nb_,_parm_);\
  VOZ32(_op_,  2, ov, _nb_,_parm_);\
  VOZ32(_op_,  3, ov, _nb_,_parm_);\
  VOZ32(_op_,  4, ov, _nb_,_parm_);\
  VOZ32(_op_,  5, ov, _nb_,_parm_);\
  VOZ32(_op_,  6, ov, _nb_,_parm_);\
  VOZ32(_op_,  7, ov, _nb_,_parm_);\
  VOZ32(_op_,  8, ov, _nb_,_parm_);\
  VOZ32(_op_,  9, ov, _nb_,_parm_);\
  VOZ32(_op_, 10, ov, _nb_,_parm_);\
  VOZ32(_op_, 11, ov, _nb_,_parm_);\
  VOZ32(_op_, 12, ov, _nb_,_parm_);\
  VOZ32(_op_, 13, ov, _nb_,_parm_);\
  VOZ32(_op_, 14, ov, _nb_,_parm_);\
  VOZ32(_op_, 15, ov, _nb_,_parm_);\
  VOZ32(_op_, 16, ov, _nb_,_parm_);\
  VOZ32(_op_, 17, ov, _nb_,_parm_);\
  VOZ32(_op_, 18, ov, _nb_,_parm_);\
  VOZ32(_op_, 19, ov, _nb_,_parm_);\
  VOZ32(_op_, 20, ov, _nb_,_parm_);\
  VOZ32(_op_, 21, ov, _nb_,_parm_);\
  VOZ32(_op_, 22, ov, _nb_,_parm_);\
  VOZ32(_op_, 23, ov, _nb_,_parm_);\
  VOZ32(_op_, 24, ov, _nb_,_parm_);\
  VOZ32(_op_, 25, ov, _nb_,_parm_);\
  VOZ32(_op_, 26, ov, _nb_,_parm_);\
  VOZ32(_op_, 27, ov, _nb_,_parm_);\
  VOZ32(_op_, 28, ov, _nb_,_parm_);\
  VOZ32(_op_, 29, ov, _nb_,_parm_);\
  VOZ32(_op_, 30, ov, _nb_,_parm_);\
  VOZ32(_op_, 31, ov, _nb_,_parm_);\
}
#define BITUNPACK0(_parm_) _parm_ = _mm256_setzero_si256()

unsigned char *bitunpack256v32( const unsigned char *__restrict in, unsigned n, unsigned *__restrict out, unsigned b) {
  const unsigned char *ip = in+PAD8(256*b);
  __m256i sv;
  BITUNPACK256V32(in, b, out, sv);
  return (unsigned char *)ip;
}

//--------------------------------------- zeromask unpack for TurboPFor vp4d.c --------------------------------------
#define VO32(_op_, _i_, _ov_, _nb_,_parm_)  xm = *bb++; _mm256_storeu_si256(_op_++, _mm256_add_epi32(_ov_, _mm256_slli_epi32(mm256_maskz_loadu_epi32(xm,(__m256i*)pex), _nb_) )); pex += popcnt32(xm)
#define VOZ32(_op_, _i_, _ov_, _nb_,_parm_) xm = *bb++; _mm256_storeu_si256(_op_++,                                          mm256_maskz_loadu_epi32(xm,(__m256i*)pex) );         pex += popcnt32(xm)
#define BITUNPACK0(_parm_)
#include "bitunpack_.h"
unsigned char *_bitunpack256v32( const unsigned char *__restrict in, unsigned n, unsigned *__restrict out, unsigned b, unsigned *__restrict pex, unsigned char *bb) {
  const unsigned char *ip = in+PAD8(256*b); unsigned xm; __m256i sv, zv = _mm256_setzero_si256(), tv = _mm256_set_epi32(0,1,2,3,4,5,6,7);
  BITUNPACK256V32(in, b, out, sv);
  return (unsigned char *)ip;
}

#define VOZ32(_op_, _i_, ov, _nb_,_parm_) _mm256_storeu_si256(_op_++, _parm_)
#define VO32(_op_, i, _ov_, _nb_,_sv_) _ov_ = mm256_zzagd_epi32(_ov_); _sv_ = mm256_scan_epi32(_ov_,_sv_); _mm256_storeu_si256(_op_++, _sv_)
#include "bitunpack_.h"
#define BITUNPACK0(_parm_)
unsigned char *bitzunpack256v32( const unsigned char *__restrict in, unsigned n, unsigned *__restrict out, unsigned start, unsigned b) {
  const unsigned char *ip = in+PAD8(256*b);
  __m256i sv = _mm256_set1_epi32(start);//, zv = _mm256_setzero_si256();
  BITUNPACK256V32(in, b, out, sv);
  return (unsigned char *)ip;
}


#define VO32(_op_, i, _ov_, _nb_,_sv_) _sv_ = mm256_scan_epi32(_ov_,_sv_); _mm256_storeu_si256(_op_++, _sv_)
#include "bitunpack_.h"
#define BITUNPACK0(_parm_)
unsigned char *bitdunpack256v32( const unsigned char *__restrict in, unsigned n, unsigned *__restrict out, unsigned start, unsigned b) {
  const unsigned char *ip = in+PAD8(256*b);
  __m256i sv = _mm256_set1_epi32(start);// zv = _mm256_setzero_si256();
  BITUNPACK256V32(in, b, out, sv);
  return (unsigned char *)ip;
}

#define VO32( _op_, _i_, _ov_, _nb_,_parm_) _mm256_storeu_si256(_op_++, _mm256_add_epi32(_ov_, sv))
#include "bitunpack_.h"
#define BITUNPACK0(_parm_)
unsigned char *bitfunpack256v32( const unsigned char *__restrict in, unsigned n, unsigned *__restrict out, unsigned start, unsigned b) {
  const unsigned char *ip = in+PAD8(256*b);
  __m256i sv = _mm256_set1_epi32(start);
  BITUNPACK256V32(in, b, out, sv);
  return (unsigned char *)ip;
}
//-----------------------------------------------------------------------------
#define VX32(_i_,  _nb_,_ov_) xm = *bb++; _ov_ = _mm256_add_epi32(_ov_, _mm256_slli_epi32(mm256_maskz_loadu_epi32(xm,(__m256i*)pex), _nb_) ); pex += popcnt32(xm)
#define VXZ32(_i_, _nb_,_ov_) xm = *bb++; _ov_ =                                          mm256_maskz_loadu_epi32(xm,(__m256i*)pex);       pex += popcnt32(xm)

#define VO32( _op_, _i_, _ov_, _nb_,_sv_) VX32( _i_, _nb_,_ov_); _sv_ = mm256_scan_epi32(_ov_,_sv_); _mm256_storeu_si256(_op_++, _sv_);
#define VOZ32(_op_, _i_, _ov_, _nb_,_sv_) VXZ32(_i_, _nb_,_ov_); _sv_ = mm256_scan_epi32(_ov_,_sv_); _mm256_storeu_si256(_op_++, _sv_);
#include "bitunpack_.h"
#define BITUNPACK0(_parm_)
unsigned char *_bitdunpack256v32( const unsigned char *__restrict in, unsigned n, unsigned *__restrict out, unsigned start, unsigned b, unsigned *__restrict pex, unsigned char *bb) {
  const unsigned char *ip = in+PAD8(256*b); unsigned xm;
  __m256i sv = _mm256_set1_epi32(start), tv = _mm256_set_epi32(0,1,2,3,4,5,6,7),zv = _mm256_setzero_si256();
  BITUNPACK256V32(in, b, out, sv);
  return (unsigned char *)ip;
}

#define VX32(_i_, _nb_,_ov_)  xm = *bb++; _ov_ = _mm256_add_epi32(_ov_, _mm256_slli_epi32(mm256_maskz_loadu_epi32(xm,(__m256i*)pex), _nb_) ); pex += popcnt32(xm)
#define VXZ32(_i_, _nb_,_ov_) xm = *bb++; _ov_ =                                          mm256_maskz_loadu_epi32(xm,(__m256i*)pex);          pex += popcnt32(xm)

  #ifdef _MSC_VER
#define SCAN32x8( _v_, _sv_) {\
  _v_  = _mm256_add_epi32(_v_, _mm256_slli_si256(_v_, 4));\
  _v_  = _mm256_add_epi32(_v_, _mm256_slli_si256(_v_, 8));\
  _sv_ = _mm256_add_epi32(     _mm256_permute2x128_si256(   _mm256_shuffle_epi32(_sv_,_MM_SHUFFLE(3, 3, 3, 3)), _sv_, 0x11), \
         _mm256_add_epi32(_v_, _mm256_permute2x128_si256(zv,_mm256_shuffle_epi32(_v_, _MM_SHUFFLE(3, 3, 3, 3)),       0x20)));\
}
#define SCANI32x8(_v_, _sv_, _vi_) SCAN32x8(_v_, _sv_); _sv_ = _mm256_add_epi32(_sv_, _vi_)
#define   ZIGZAG32x8(_v_) _mm256_xor_si256(_mm256_slli_epi32(_v_,1), _mm256_srai_epi32(_v_,31))
#define UNZIGZAG32x8(_v_) _mm256_xor_si256(_mm256_srli_epi32(_v_,1), _mm256_srai_epi32(_mm256_slli_epi32(_v_,31),31) )

#define VO32( _op_, _i_, _ov_, _nb_,_sv_) VX32( _i_, _nb_,_ov_); _ov_ = UNZIGZAG32x8(_ov_); SCAN32x8(_ov_,_sv_); _mm256_storeu_si256(_op_++, _sv_);
#define VOZ32(_op_, _i_, _ov_, _nb_,_sv_) VXZ32(_i_, _nb_,_ov_); _ov_ = UNZIGZAG32x8(_ov_); SCAN32x8(_ov_,_sv_); _mm256_storeu_si256(_op_++, _sv_);
  #else
#define VO32( _op_, _i_, _ov_, _nb_,_sv_) VX32( _i_, _nb_,_ov_); _ov_ = mm256_zzagd_epi32(_ov_); _sv_ = mm256_scan_epi32(_ov_,_sv_); _mm256_storeu_si256(_op_++, _sv_);
#define VOZ32(_op_, _i_, _ov_, _nb_,_sv_) VXZ32(_i_, _nb_,_ov_); _ov_ = mm256_zzagd_epi32(_ov_); _sv_ = mm256_scan_epi32(_ov_,_sv_); _mm256_storeu_si256(_op_++, _sv_);
  #endif

#include "bitunpack_.h"
#define BITUNPACK0(_parm_)
unsigned char *_bitzunpack256v32( const unsigned char *__restrict in, unsigned n, unsigned *__restrict out, unsigned start, unsigned b, unsigned *__restrict pex, unsigned char *bb) {
  const unsigned char *ip = in+PAD8(256*b); 
        unsigned xm; 
  const __m256i zv = _mm256_setzero_si256(), tv = _mm256_set_epi32(0,1,2,3,4,5,6,7); 
        __m256i sv = _mm256_set1_epi32(start);
  BITUNPACK256V32(in, b, out, sv); 
  return (unsigned char *)ip;
}

#define VO32(_op_, i, _ov_, _nb_,_sv_)    _sv_ = mm256_scani_epi32(_ov_,_sv_,cv); _mm256_storeu_si256(_op_++, _sv_);
#define VOZ32(_op_, _i_, ov, _nb_,_parm_) _mm256_storeu_si256(_op_++, _parm_); _parm_ = _mm256_add_epi32(_parm_, cv)
#include "bitunpack_.h"
#define BITUNPACK0(_parm_) _parm_ = _mm256_add_epi32(_parm_, cv); cv = _mm256_set1_epi32(8)
unsigned char *bitd1unpack256v32( const unsigned char *__restrict in, unsigned n, unsigned *__restrict out, unsigned start, unsigned b) {
  const unsigned char *ip = in+PAD8(256*b);
  const __m256i zv = _mm256_setzero_si256();
        __m256i sv = _mm256_set1_epi32(start), cv = _mm256_set_epi32(8,7,6,5,4,3,2,1);
  BITUNPACK256V32(in, b, out, sv);
  return (unsigned char *)ip;
}

#define VO32( _op_, _i_, _ov_, _nb_,_sv_) _mm256_storeu_si256(_op_++, _mm256_add_epi32(_ov_, _sv_)); _sv_ = _mm256_add_epi32(_sv_, cv)
#define VOZ32(_op_, _i_, ov, _nb_,_sv_)   _mm256_storeu_si256(_op_++, _sv_);                         _sv_ = _mm256_add_epi32(_sv_, cv);
#include "bitunpack_.h"
#define BITUNPACK0(_parm_)
unsigned char *bitf1unpack256v32( const unsigned char *__restrict in, unsigned n, unsigned *__restrict out, unsigned start, unsigned b) {
  const unsigned char *ip = in+PAD8(256*b);
  const __m256i cv = _mm256_set1_epi32(8);
        __m256i sv = _mm256_set_epi32(start+8,start+7,start+6,start+5,start+4,start+3,start+2,start+1);
  BITUNPACK256V32(in, b, out, sv);
  return (unsigned char *)ip;
}

#define VO32( _op_, _i_, _ov_, _nb_,_sv_)   VX32( _i_, _nb_,_ov_); _sv_ = mm256_scani_epi32(_ov_,_sv_,cv); _mm256_storeu_si256(_op_++, _sv_);
#define VOZ32(_op_, _i_, _ov_, _nb_,_sv_)   VXZ32(_i_, _nb_,_ov_); _sv_ = mm256_scani_epi32(_ov_,_sv_,cv); _mm256_storeu_si256(_op_++, _sv_);
#include "bitunpack_.h"
#define BITUNPACK0(_parm_) mv = _mm256_set1_epi32(0) //_parm_ = _mm_setzero_si128()
unsigned char *_bitd1unpack256v32( const unsigned char *__restrict in, unsigned n, unsigned *__restrict out, unsigned start, unsigned b, unsigned *__restrict pex, unsigned char *bb) {
  const unsigned char *ip = in+PAD8(256*b); unsigned xm;
  const __m256i cv = _mm256_set_epi32(8,7,6,5,4,3,2,1), zv = _mm256_setzero_si256(), tv = _mm256_set_epi32(0,1,2,3,4,5,6,7);
        __m256i sv = _mm256_set1_epi32(start);
  BITUNPACK256V32(in, b, out, sv);
  return (unsigned char *)ip;
}

size_t bitnunpack256v32(  unsigned char *__restrict in, size_t n, uint32_t *__restrict out) { uint32_t *op;       _BITNUNPACKV( in, n, out, 256, 32, bitunpack256v); }
size_t bitndunpack256v32( unsigned char *__restrict in, size_t n, uint32_t *__restrict out) { uint32_t *op,start; _BITNDUNPACKV(in, n, out, 256, 32, bitdunpack256v,  bitdunpack); }
size_t bitnd1unpack256v32(unsigned char *__restrict in, size_t n, uint32_t *__restrict out) { uint32_t *op,start; _BITNDUNPACKV(in, n, out, 256, 32, bitd1unpack256v, bitd1unpack); }
//size_t bitns1unpack256v32(unsigned char *__restrict in, size_t n, uint32_t *__restrict out) { uint32_t *op,start; _BITNDUNPACKV(in, n, out, 256, 32, bits1unpack256v, bitd1unpack); }
size_t bitnzunpack256v32( unsigned char *__restrict in, size_t n, uint32_t *__restrict out) { uint32_t *op,start; _BITNDUNPACKV(in, n, out, 256, 32, bitzunpack256v,  bitzunpack); }
size_t bitnfunpack256v32( unsigned char *__restrict in, size_t n, uint32_t *__restrict out) { uint32_t *op,start; _BITNDUNPACKV(in, n, out, 256, 32, bitfunpack256v,  bitfunpack); }
  #elif defined(__SSE2__) || defined(__ARM_NEON) //------------------------------ SSE2/SSSE3 ---------------------------------------------------------
#define BITMAX16 16
#define BITMAX32 32

#define VO16( _op_, _i_, ov, _nb_,_parm_) _mm_storeu_si128(_op_++, ov)
#define VO32( _op_, _i_, ov, _nb_,_parm_) _mm_storeu_si128(_op_++, ov)
#include "bitunpack_.h"

#define VOZ16(_op_, _i_, ov, _nb_,_parm_) _mm_storeu_si128(_op_++, _parm_)
#define VOZ32(_op_, _i_, ov, _nb_,_parm_) _mm_storeu_si128(_op_++, _parm_)
#define BITUNBLK128V16_0(ip, _i_, _op_, _nb_,_parm_) {__m128i ov;\
  VOZ16(_op_,  0, ov, _nb_,_parm_);\
  VOZ16(_op_,  1, ov, _nb_,_parm_);\
  VOZ16(_op_,  2, ov, _nb_,_parm_);\
  VOZ16(_op_,  3, ov, _nb_,_parm_);\
  VOZ16(_op_,  4, ov, _nb_,_parm_);\
  VOZ16(_op_,  5, ov, _nb_,_parm_);\
  VOZ16(_op_,  6, ov, _nb_,_parm_);\
  VOZ16(_op_,  7, ov, _nb_,_parm_);\
  VOZ16(_op_,  8, ov, _nb_,_parm_);\
  VOZ16(_op_,  9, ov, _nb_,_parm_);\
  VOZ16(_op_, 10, ov, _nb_,_parm_);\
  VOZ16(_op_, 11, ov, _nb_,_parm_);\
  VOZ16(_op_, 12, ov, _nb_,_parm_);\
  VOZ16(_op_, 13, ov, _nb_,_parm_);\
  VOZ16(_op_, 14, ov, _nb_,_parm_);\
  VOZ16(_op_, 15, ov, _nb_,_parm_);\
  /*VOZ16(_op_, 16, ov, _nb_,_parm_);\
  VOZ16(_op_, 17, ov, _nb_,_parm_);\
  VOZ16(_op_, 18, ov, _nb_,_parm_);\
  VOZ16(_op_, 19, ov, _nb_,_parm_);\
  VOZ16(_op_, 20, ov, _nb_,_parm_);\
  VOZ16(_op_, 21, ov, _nb_,_parm_);\
  VOZ16(_op_, 22, ov, _nb_,_parm_);\
  VOZ16(_op_, 23, ov, _nb_,_parm_);\
  VOZ16(_op_, 24, ov, _nb_,_parm_);\
  VOZ16(_op_, 25, ov, _nb_,_parm_);\
  VOZ16(_op_, 26, ov, _nb_,_parm_);\
  VOZ16(_op_, 27, ov, _nb_,_parm_);\
  VOZ16(_op_, 28, ov, _nb_,_parm_);\
  VOZ16(_op_, 29, ov, _nb_,_parm_);\
  VOZ16(_op_, 30, ov, _nb_,_parm_);\
  VOZ16(_op_, 31, ov, _nb_,_parm_);*/\
}

#define BITUNBLK128V32_0(ip, _i_, _op_, _nb_,_parm_) {__m128i ov;\
  VOZ32(_op_,  0, ov, _nb_,_parm_);\
  VOZ32(_op_,  1, ov, _nb_,_parm_);\
  VOZ32(_op_,  2, ov, _nb_,_parm_);\
  VOZ32(_op_,  3, ov, _nb_,_parm_);\
  VOZ32(_op_,  4, ov, _nb_,_parm_);\
  VOZ32(_op_,  5, ov, _nb_,_parm_);\
  VOZ32(_op_,  6, ov, _nb_,_parm_);\
  VOZ32(_op_,  7, ov, _nb_,_parm_);\
  VOZ32(_op_,  8, ov, _nb_,_parm_);\
  VOZ32(_op_,  9, ov, _nb_,_parm_);\
  VOZ32(_op_, 10, ov, _nb_,_parm_);\
  VOZ32(_op_, 11, ov, _nb_,_parm_);\
  VOZ32(_op_, 12, ov, _nb_,_parm_);\
  VOZ32(_op_, 13, ov, _nb_,_parm_);\
  VOZ32(_op_, 14, ov, _nb_,_parm_);\
  VOZ32(_op_, 15, ov, _nb_,_parm_);\
  VOZ32(_op_, 16, ov, _nb_,_parm_);\
  VOZ32(_op_, 17, ov, _nb_,_parm_);\
  VOZ32(_op_, 18, ov, _nb_,_parm_);\
  VOZ32(_op_, 19, ov, _nb_,_parm_);\
  VOZ32(_op_, 20, ov, _nb_,_parm_);\
  VOZ32(_op_, 21, ov, _nb_,_parm_);\
  VOZ32(_op_, 22, ov, _nb_,_parm_);\
  VOZ32(_op_, 23, ov, _nb_,_parm_);\
  VOZ32(_op_, 24, ov, _nb_,_parm_);\
  VOZ32(_op_, 25, ov, _nb_,_parm_);\
  VOZ32(_op_, 26, ov, _nb_,_parm_);\
  VOZ32(_op_, 27, ov, _nb_,_parm_);\
  VOZ32(_op_, 28, ov, _nb_,_parm_);\
  VOZ32(_op_, 29, ov, _nb_,_parm_);\
  VOZ32(_op_, 30, ov, _nb_,_parm_);\
  VOZ32(_op_, 31, ov, _nb_,_parm_);\
}
#define BITUNPACK0(_parm_) _parm_ = _mm_setzero_si128()

unsigned char *bitunpack128v16( const unsigned char *__restrict in, unsigned n, unsigned short *__restrict out, unsigned b) { const unsigned char *ip = in+PAD8(128*b); __m128i sv; BITUNPACK128V16(in, b, out, sv); return (unsigned char *)ip; }
unsigned char *bitunpack128v32( const unsigned char *__restrict in, unsigned n, unsigned       *__restrict out, unsigned b) { const unsigned char *ip = in+PAD8(128*b); __m128i sv; BITUNPACK128V32(in, b, out, sv); return (unsigned char *)ip; }
unsigned char *bitunpack256w32( const unsigned char *__restrict in, unsigned n, unsigned       *__restrict out, unsigned b) {
  const unsigned char *_in=in; unsigned *_out=out; __m128i sv;
  BITUNPACK128V32(in, b, out, sv); out = _out+128; in=_in+PAD8(128*b);
  BITUNPACK128V32(in, b, out, sv);
  return (unsigned char *)_in+PAD8(256*b);
}
static void applyException_8bits(uint8_t xm8, uint32_t **pPEX, int nb, uint32_t ov[8])
{
    uint32_t *ex = *pPEX;
    for(int j=0; j<8; j++){
        if((xm8>>j) & 1) {
            ov[j] += (ex[0]<< nb);
            ex++;
        }
    }
    *pPEX = ex;
}

static void bitunblk256v32_scalar_template(
    uint32_t **pIn,
    uint32_t **pOut,
    int expansions_count,
    const uint8_t *SHIFT_HI,
    const uint8_t *SHIFT_LO,
    const uint8_t *READ_FLAG,
    uint32_t mask,
    int nb
)
{
    const uint32_t *oldp = NULL; // pointer to current block data
    uint32_t ov[8], tmp[8];

    for (int k = 0; k < expansions_count; k++) {
        if (k == 0) {
            // Step 0: Load input block and directly take the lower nb bits
            oldp = *pIn;
            *pIn += 8;
            for (int j = 0; j < 8; j++) {
                ov[j] = oldp[j] & mask;
            }
        } else {
            // First right shift the current block data by SHIFT_HI[k]
            for (int j = 0; j < 8; j++) {
                ov[j] = oldp[j] >> SHIFT_HI[k];
            }
            if (READ_FLAG[k]) {
                // Need to load a new block: left shift the new block data by SHIFT_LO[k], then merge with ov
                const uint32_t *newp = *pIn;
                *pIn += 8;
                for (int j = 0; j < 8; j++) {
                    uint32_t part_lo = (newp[j] << SHIFT_LO[k]) & mask;
                    ov[j] |= part_lo;
                }
                // Update current block pointer
                oldp = newp;
            } else {
                // No need to load a new block, ensure the result is within mask range
                for (int j = 0; j < 8; j++) {
                    ov[j] &= mask;
                }
            }
        }
        // Write out the current 8 results
        uint32_t *outp = *pOut;
        for (int j = 0; j < 8; j++) {
            outp[j] = ov[j];
        }
        *pOut += 8;
    }
}
/**
 * Generic template: supports "some expansions don't need to read new blocks".
 *
 * Parameters:
 *  - expansions_count: total number of expansions (for 29-bit, it might be 32 times)
 *  - SHIFT_HI[k], SHIFT_LO[k]: right shift for leftover, left shift for new block in k-th expansion
 *  - READ_FLAG[k]: whether k-th expansion needs to read a new block (1 means yes, 0 means no)
 *  - mask: for 29-bit = (1u << 29) - 1
 *  - nb: base bits (29)
 */
static void bitunblk256v32_scalarBlock_ex_template(
    uint32_t **pIn,
    uint32_t **pOut,
    uint32_t **pPEX,
    unsigned char **pBB,
    int expansions_count,
    const uint8_t *SHIFT_HI,
    const uint8_t *SHIFT_LO,
    const uint8_t *READ_FLAG,
    uint32_t mask,
    int nb
)
{
    const uint32_t *oldp = NULL; // leftover block (previous batch)
    
    for (int k = 0; k < expansions_count; k++) {
        uint32_t ov[8];

        if (k == 0) {
            // First time: directly read 8×32-bit and apply mask
            oldp = *pIn;
            *pIn += 8;
            for (int j = 0; j < 8; j++) {
                ov[j] = oldp[j] & mask;
            }
        } 
        else {
            // Subsequent expansions
            uint8_t hi = SHIFT_HI[k];
            uint8_t lo = SHIFT_LO[k];

            // First shift leftover >> hi
            for (int j = 0; j < 8; j++) {
                ov[j] = (oldp[j] >> hi);
            }

            // If this expansion needs to read a new block, append newp << lo
            if (READ_FLAG[k]) {
                const uint32_t *newp = *pIn;
                *pIn += 8;
                for (int j = 0; j < 8; j++) {
                    uint32_t part_lo = (newp[j] << lo) & mask;
                    ov[j] |= part_lo;
                }
                // After reading, newp becomes the leftover for next time
                oldp = newp;
            } else {
                // No need to read new block => just apply mask to leftover >> hi
                for (int j = 0; j < 8; j++) {
                    ov[j] &= mask;
                }
                // leftover remains unchanged, continue using oldp
            }
        }

        // Apply exceptions
        uint8_t xm8 = **pBB;
        (*pBB)++;
        applyException_8bits(xm8, pPEX, nb, ov);

        // Write out this batch of 8 results
        uint32_t *outp = *pOut;
        for (int j = 0; j < 8; j++) {
            outp[j] = ov[j];
        }
        *pOut += 8;
    }
}
static void bitunpack256v32_0_scalar(uint32_t **pIn,
                                     uint32_t **pOut,
                                     uint32_t **pPEX,
                                     unsigned char **pBB)
{
    uint32_t *op = *pOut;
    for (int i = 0; i < 32; i++) {
        // Read bitmap if exists, otherwise default to 0
        uint8_t xm8 = (pBB != NULL) ? **pBB : 0;
        if (pBB != NULL) {
            (*pBB)++;
        }
        // Initialize output array (all zeros by default)
        uint32_t ov[8] = {0};
        if (xm8 != 0 && pPEX != NULL) {
            applyException_8bits(xm8, pPEX, 0, ov);
        }
        
        // Directly write 8 values using a loop to avoid repeated memory copy calls
        for (int j = 0; j < 8; j++) {
            op[j] = ov[j];
        }
        op += 8;
    }
    *pOut = op;
}

static void bitunpack256v32_1_scalar(
    uint32_t **pIn,
    uint32_t **pOut,
    uint32_t **pPEX,
    unsigned char **pBB
)
{
    const int nb1 = 1;
    const uint32_t mask1 = 1;  // 0x1
    const int expansions_count_1 = 32;
    static const uint8_t SHIFT_HI_1[32] = {
         0,  1,  2,  3,  4,  5,  6,  7,
         8,  9, 10, 11, 12, 13, 14, 15,
        16, 17, 18, 19, 20, 21, 22, 23,
        24, 25, 26, 27, 28, 29, 30, 31
    };
    static const uint8_t SHIFT_LO_1[32] = {
         0, 0, 0, 0, 0, 0, 0, 0,
         0, 0, 0, 0, 0, 0, 0, 0,
         0, 0, 0, 0, 0, 0, 0, 0,
         0, 0, 0, 0, 0, 0, 0, 0
    };
    static const uint8_t READ_FLAG_1[32] = {
         1, 0, 0, 0, 0, 0, 0, 0,
         0, 0, 0, 0, 0, 0, 0, 0,
         0, 0, 0, 0, 0, 0, 0, 0,
         0, 0, 0, 0, 0, 0, 0, 0
    };
    if (pPEX != NULL && pBB != NULL) {
      bitunblk256v32_scalarBlock_ex_template(
         pIn, pOut, pPEX, pBB,
         expansions_count_1,
         SHIFT_HI_1,
         SHIFT_LO_1,
         READ_FLAG_1,
         mask1,
         nb1
      );
    } else {
      bitunblk256v32_scalar_template(
         pIn, pOut, expansions_count_1,
         SHIFT_HI_1, SHIFT_LO_1, READ_FLAG_1,
         mask1, nb1
      );
    }
}

static void bitunpack256v32_2_scalar(
    uint32_t **pIn,
    uint32_t **pOut,
    uint32_t **pPEX,
    unsigned char **pBB
)
{
    const int nb2 = 2;
    const uint32_t mask2 = (1u << nb2) - 1;  // 0x3
    const int expansions_count_2 = 16;
    static const uint8_t SHIFT_HI_2[16] = {
         0,  2,  4,  6,  8, 10, 12, 14,
        16, 18, 20, 22, 24, 26, 28, 30
    };
    static const uint8_t SHIFT_LO_2[16] = {
         0, 0, 0, 0, 0, 0, 0, 0,
         0, 0, 0, 0, 0, 0, 0, 0
    };
    static const uint8_t READ_FLAG_2[16] = {
         1, 0, 0, 0, 0, 0, 0, 0,
         0, 0, 0, 0, 0, 0, 0, 0
    };
    if(pPEX != NULL && pBB != NULL) {
      for (int i = 0; i < 2; i++) {
        bitunblk256v32_scalarBlock_ex_template(
             pIn, pOut, pPEX, pBB,
             expansions_count_2,
             SHIFT_HI_2,
             SHIFT_LO_2,
             READ_FLAG_2,
             mask2,
             nb2
        );
      }
    } else {
      for (int i = 0; i < 2; i++) {
        bitunblk256v32_scalar_template(
            pIn, pOut, expansions_count_2,
            SHIFT_HI_2, SHIFT_LO_2, READ_FLAG_2,
            mask2, nb2
        );
      }
    }
}

static void bitunpack256v32_3_scalar(
    uint32_t **pIn,
    uint32_t **pOut,
    uint32_t **pPEX,
    unsigned char **pBB
)
{
    const int nb3 = 3;
    const uint32_t mask3 = (1u << nb3) - 1;  // 0x7
    const int expansions_count_3 = 32;
    static const uint8_t SHIFT_HI_3[32] = {
         0,  3,  6,  9, 12, 15, 18, 21,
        24, 27, 30,  1,  4,  7, 10, 13,
        16, 19, 22, 25, 28, 31,  2,  5,
         8, 11, 14, 17, 20, 23, 26, 29
    };
    static const uint8_t SHIFT_LO_3[32] = {
         0, 0, 0, 0, 0, 0, 0, 0,
         0, 0, 2, 0, 0, 0, 0, 0,
         0, 0, 0, 0, 0, 1, 0, 0,
         0, 0, 0, 0, 0, 0, 0, 0
    };
    static const uint8_t READ_FLAG_3[32] = {
         1, 0, 0, 0, 0, 0, 0, 0,
         0, 0, 1, 0, 0, 0, 0, 0,
         0, 0, 0, 0, 0, 1, 0, 0,
         0, 0, 0, 0, 0, 0, 0, 0
    };
    if(pPEX != NULL && pBB != NULL) {
      bitunblk256v32_scalarBlock_ex_template(
         pIn, pOut, pPEX, pBB,
         expansions_count_3,
         SHIFT_HI_3,
         SHIFT_LO_3,
         READ_FLAG_3,
         mask3,
         nb3
      );
    } else {
      bitunblk256v32_scalar_template(
        pIn, pOut, expansions_count_3,
        SHIFT_HI_3, SHIFT_LO_3, READ_FLAG_3,
        mask3, nb3
      );
    }
}

static void bitunpack256v32_4_scalar(
    uint32_t **pIn,
    uint32_t **pOut,
    uint32_t **pPEX,
    unsigned char **pBB
)
{
    const uint32_t mask4 = (1u << 4) - 1;  // 0xF
    const int nb = 4; // base bits
    const int expansions_count = 8;
    static const uint8_t SHIFT_HI_4[8] = { 0, 4, 8, 12, 16, 20, 24, 28 };
    static const uint8_t SHIFT_LO_4[8] = { 0, 0, 0, 0, 0, 0, 0, 0 };
    static const uint8_t READ_FLAG_4[8] = { 1, 0, 0, 0, 0, 0, 0, 0 };

    if(pPEX != NULL && pBB != NULL) {
      for (int i = 0; i < 4; i++) {
         bitunblk256v32_scalarBlock_ex_template(
              pIn, pOut, pPEX, pBB,
              expansions_count,
              SHIFT_HI_4,
              SHIFT_LO_4,
              READ_FLAG_4,
              mask4,
              nb
         );
      }
    } else {
      for (int i = 0; i < 4; i++) {
        bitunblk256v32_scalar_template(
          pIn, pOut, expansions_count,
            SHIFT_HI_4, SHIFT_LO_4, READ_FLAG_4,
            mask4, nb
          );
      }
    }
}

static void bitunpack256v32_5_scalar(
    uint32_t **pIn,
    uint32_t **pOut,
    uint32_t **pPEX,
    unsigned char **pBB
)
{
    const int nb5 = 5;
    const uint32_t mask5 = (1u << nb5) - 1;  // 0x1F
    const int expansions_count_5 = 32;
    static const uint8_t SHIFT_HI_5[32] = {
         0,  5, 10, 15, 20, 25, 30,  3,
         8, 13, 18, 23, 28,  1,  6, 11,
        16, 21, 26, 31,  4,  9, 14, 19,
        24, 29,  2,  7, 12, 17, 22, 27
    };
    static const uint8_t SHIFT_LO_5[32] = {
         0, 0, 0, 0, 0, 0, 2, 0,
         0, 0, 0, 0, 4, 0, 0, 0,
         0, 0, 0, 1, 0, 0, 0, 0,
         0, 3, 0, 0, 0, 0, 0, 0
    };
    static const uint8_t READ_FLAG_5[32] = {
         1, 0, 0, 0, 0, 0, 1, 0,
         0, 0, 0, 0, 1, 0, 0, 0,
         0, 0, 0, 1, 0, 0, 0, 0,
         0, 1, 0, 0, 0, 0, 0, 0
    };
    if(pPEX != NULL && pBB != NULL) {
      bitunblk256v32_scalarBlock_ex_template(
        pIn, pOut, pPEX, pBB,
        expansions_count_5,
        SHIFT_HI_5,
        SHIFT_LO_5,
        READ_FLAG_5,
        mask5,
        nb5
      );
    } else {
      bitunblk256v32_scalar_template(
        pIn, pOut, expansions_count_5,
        SHIFT_HI_5, SHIFT_LO_5, READ_FLAG_5,
        mask5, nb5
      );
    }
}
static void bitunpack256v32_6_scalar(
    uint32_t **pIn,
    uint32_t **pOut,
    uint32_t **pPEX,
    unsigned char **pBB
)
{
    const int nb6 = 6;
    const uint32_t mask6 = (1u << nb6) - 1;  // 0x3F
    const int expansions_count_6 = 16;
    static const uint8_t SHIFT_HI_6[16] = { 
         0,  6, 12, 18, 24, 30,  4, 10,
        16, 22, 28,  2,  8, 14, 20, 26
    };
    static const uint8_t SHIFT_LO_6[16] = { 
         0, 0, 0, 0, 0, 2, 0, 0,
         0, 0, 4, 0, 0, 0, 0, 0
    };
    static const uint8_t READ_FLAG_6[16] = { 
         1, 0, 0, 0, 0, 1, 0, 0,
         0, 0, 1, 0, 0, 0, 0, 0
    };

    if(pPEX != NULL && pBB != NULL) {
          for (int i = 0; i < 2; i++) {
            bitunblk256v32_scalarBlock_ex_template(
              pIn, pOut, pPEX, pBB,
              expansions_count_6,
              SHIFT_HI_6,
              SHIFT_LO_6,
              READ_FLAG_6,
              mask6,
              nb6
            );
          }
    } else {
          for (int i = 0; i < 2; i++) {
            bitunblk256v32_scalar_template(
              pIn, pOut, expansions_count_6,
              SHIFT_HI_6, SHIFT_LO_6, READ_FLAG_6,
              mask6, nb6
            );
          }
    }
}
static void bitunpack256v32_7_scalar(
    uint32_t **pIn,
    uint32_t **pOut,
    uint32_t **pPEX,
    unsigned char **pBB
)
{
    const int nb7 = 7;
    const uint32_t mask7 = (1u << nb7) - 1;  // 0x7F
    const int expansions_count = 32;
    static const uint8_t SHIFT_HI_7[32] = {
         0,  7, 14, 21, 28,  3, 10, 17,
        24, 31,  6, 13, 20, 27,  2,  9,
        16, 23, 30,  5, 12, 19, 26,  1,
         8, 15, 22, 29,  4, 11, 18, 25
    };
    static const uint8_t SHIFT_LO_7[32] = {
         0, 0, 0, 0, 4, 0, 0, 0,
         0, 1, 0, 0, 0, 5, 0, 0,
         0, 0, 2, 0, 0, 0, 6, 0,
         0, 0, 0, 3, 0, 0, 0, 0
    };
    static const uint8_t READ_FLAG_7[32] = {
         1, 0, 0, 0, 1, 0, 0, 0,
         0, 1, 0, 0, 0, 1, 0, 0,
         0, 0, 1, 0, 0, 0, 1, 0,
         0, 0, 0, 1, 0, 0, 0, 0
    };
    if(pPEX != NULL && pBB != NULL) {
        bitunblk256v32_scalarBlock_ex_template(
            pIn, pOut, pPEX, pBB,
            expansions_count,
            SHIFT_HI_7,
            SHIFT_LO_7,
            READ_FLAG_7,
            mask7,
            nb7
        );
    } else {
        bitunblk256v32_scalar_template(
            pIn, pOut, expansions_count,
            SHIFT_HI_7, SHIFT_LO_7, READ_FLAG_7,
            mask7,
            nb7
        );
    }
}
static void bitunpack256v32_8_scalar(
    uint32_t **pIn,
    uint32_t **pOut,
    uint32_t **pPEX,
    unsigned char **pBB
)
{
    const int nb8 = 8;
    const uint32_t mask8 = (1u << nb8) - 1;  // 0xFF
    const int expansions_count_8 = 4;
    static const uint8_t SHIFT_HI_8[4] = { 0, 8, 16, 24 };
    static const uint8_t SHIFT_LO_8[4] = { 0, 0, 0, 0 };
    static const uint8_t READ_FLAG_8[4] = { 1, 0, 0, 0 };

    if (pPEX != NULL && pBB != NULL) {
        for(int i=0; i<8; i++) {
            bitunblk256v32_scalarBlock_ex_template(
                pIn, pOut, pPEX, pBB,
                expansions_count_8,
                SHIFT_HI_8,
                SHIFT_LO_8,
                READ_FLAG_8,
                mask8,
                nb8
            );
        }
    } else {
        for(int i=0; i<8; i++) {
            bitunblk256v32_scalar_template(
              pIn, pOut, expansions_count_8,
              SHIFT_HI_8, SHIFT_LO_8, READ_FLAG_8,
            mask8,
            nb8
        );
        }
    }
}

static void bitunpack256v32_9_scalar(
    uint32_t **pIn,
    uint32_t **pOut,
    uint32_t **pPEX,
    unsigned char **pBB
)
{
    const int nb9 = 9;
    const uint32_t mask9 = (1u << nb9) - 1;  // 0x1FF
    const int expansions_count_9 = 32;
    static const uint8_t SHIFT_HI_9[32] = {
         0,  9, 18, 27,  4, 13, 22, 31,
         8, 17, 26,  3, 12, 21, 30,  7,
        16, 25,  2, 11, 20, 29,  6, 15,
        24,  1, 10, 19, 28,  5, 14, 23
    };
    static const uint8_t SHIFT_LO_9[32] = {
         0, 0, 0, 5, 0, 0, 0, 1,
         0, 0, 6, 0, 0, 0, 2, 0,
         0, 7, 0, 0, 0, 3, 0, 0,
         8, 0, 0, 0, 4, 0, 0, 0
    };
    static const uint8_t READ_FLAG_9[32] = {
         1, 0, 0, 1, 0, 0, 0, 1,
         0, 0, 1, 0, 0, 0, 1, 0,
         0, 1, 0, 0, 0, 1, 0, 0,
         1, 0, 0, 0, 1, 0, 0, 0
    };

    if (pPEX != NULL && pBB != NULL) {
        bitunblk256v32_scalarBlock_ex_template(
            pIn, pOut, pPEX, pBB,
            expansions_count_9,
            SHIFT_HI_9,
            SHIFT_LO_9,
            READ_FLAG_9,
            mask9,
            nb9
          );
    } else {
        bitunblk256v32_scalar_template(
            pIn, pOut, expansions_count_9,
            SHIFT_HI_9, SHIFT_LO_9, READ_FLAG_9,
            mask9, nb9
        );
    }
}
static void bitunpack256v32_10_scalar(
    uint32_t **pIn,
    uint32_t **pOut,
    uint32_t **pPEX,
    unsigned char **pBB
)
{
    const int nb10 = 10;
    const uint32_t mask10 = (1u << nb10) - 1;  // 0x3FF
    const int expansions_count_10 = 16;
    static const uint8_t SHIFT_HI_10[16] = {
         0, 10, 20, 30, 8, 18, 28, 6,
        16, 26, 4, 14, 24, 2, 12, 22
    };
    static const uint8_t SHIFT_LO_10[16] = {
         0, 0, 0, 2, 0, 0, 4, 0,
         0, 6, 0, 0, 8, 0, 0, 0
    };
    static const uint8_t READ_FLAG_10[16] = {
         1, 0, 0, 1, 0, 0, 1, 0,
         0, 1, 0, 0, 1, 0, 0, 0
    };

    if (pPEX != NULL && pBB != NULL) {
      for(int i=0; i<2; i++) {
        bitunblk256v32_scalarBlock_ex_template(
         pIn, pOut, pPEX, pBB,
         expansions_count_10,
         SHIFT_HI_10,
         SHIFT_LO_10,
         READ_FLAG_10,
         mask10,
         nb10
        );
      }
    } else {
      for(int i=0; i<2; i++) {
        bitunblk256v32_scalar_template(
          pIn, pOut, expansions_count_10,
          SHIFT_HI_10, SHIFT_LO_10, READ_FLAG_10,
          mask10, nb10
        );
      }
    }
}

static void bitunpack256v32_11_scalar(
    uint32_t **pIn,
    uint32_t **pOut,
    uint32_t **pPEX,
    unsigned char **pBB
)
{
    const int nb11 = 11;
    const uint32_t mask11 = (1u << nb11) - 1;  // 0x7FF
    const int expansions_count_11 = 32;
    static const uint8_t SHIFT_HI_11[32] = {
         0, 11, 22,  1, 12, 23,  2, 13,
        24,  3, 14, 25,  4, 15, 26,  5,
        16, 27,  6, 17, 28,  7, 18, 29,
         8, 19, 30,  9, 20, 31, 10, 21
    };
    static const uint8_t SHIFT_LO_11[32] = {
         0,  0, 10,  0,  0,  9,  0,  0,
         8,  0,  0,  7,  0,  0,  6,  0,
         0,  5,  0,  0,  4,  0,  0,  3,
         0,  0,  2,  0,  0,  1,  0,  0
    };
    static const uint8_t READ_FLAG_11[32] = {
         1, 0, 1, 0, 0, 1, 0, 0,
         1, 0, 0, 1, 0, 0, 1, 0,
         0, 1, 0, 0, 1, 0, 0, 1,
         0, 0, 1, 0, 0, 1, 0, 0
    };

    if (pPEX != NULL && pBB != NULL) {
      bitunblk256v32_scalarBlock_ex_template(
        pIn, pOut, pPEX, pBB,
        expansions_count_11,
        SHIFT_HI_11,
        SHIFT_LO_11,
        READ_FLAG_11,
        mask11,
        nb11
      );
    } else {
      bitunblk256v32_scalar_template(
        pIn, pOut, expansions_count_11,
        SHIFT_HI_11, SHIFT_LO_11, READ_FLAG_11,
        mask11, nb11
      );
    }
}

static void bitunpack256v32_12_scalar(
    uint32_t **pIn,
    uint32_t **pOut,
    uint32_t **pPEX,
    unsigned char **pBB
)
{
    const int nb12 = 12;
    const uint32_t mask12 = (1u << nb12) - 1;  // 0xFFF
    const int expansions_count_12 = 8;
    static const uint8_t SHIFT_HI_12[8] = { 0, 12, 24, 4, 16, 28, 8, 20 };
    static const uint8_t SHIFT_LO_12[8] = { 0,  0,  8, 0,  0,  4,  0,  0 };
    static const uint8_t READ_FLAG_12[8] = { 1,  0,  1, 0,  0,  1,  0,  0 };

    if (pPEX != NULL && pBB != NULL) {
      for(int i=0; i<4; i++) {
        bitunblk256v32_scalarBlock_ex_template(
          pIn, pOut, pPEX, pBB,
          expansions_count_12,
          SHIFT_HI_12,
          SHIFT_LO_12,
          READ_FLAG_12,
          mask12,
          nb12
        );
      }
    } else {
      for(int i=0; i<4; i++) {
        bitunblk256v32_scalar_template(
          pIn, pOut, expansions_count_12,
          SHIFT_HI_12, SHIFT_LO_12, READ_FLAG_12,
          mask12, nb12
        );
      }
    }
}

static void bitunpack256v32_13_scalar(
    uint32_t **pIn,
    uint32_t **pOut,
    uint32_t **pPEX,
    unsigned char **pBB
)
{
    const int nb13 = 13;
    const uint32_t mask13 = (1u << nb13) - 1;  // 0x1FFF
    const int expansions_count_13 = 32;
    static const uint8_t SHIFT_HI_13[32] = {
         0, 13, 26,  7, 20,  1, 14, 27,
         8, 21,  2, 15, 28,  9, 22,  3,
        16, 29, 10, 23,  4, 17, 30, 11,
        24,  5, 18, 31, 12, 25,  6, 19
    };
    static const uint8_t SHIFT_LO_13[32] = {
         0,  0,  6,  0, 12,  0,  0,  5,
         0, 11,  0,  0,  4,  0, 10,  0,
         0,  3,  0,  9,  0,  0,  2,  0,
         8,  0,  0,  1,  0,  7,  0,  0
    };
    static const uint8_t READ_FLAG_13[32] = {
         1, 0, 1, 0, 1, 0, 0, 1,
         0, 1, 0, 0, 1, 0, 1, 0,
         0, 1, 0, 1, 0, 0, 1, 0,
         1, 0, 0, 1, 0, 1, 0, 0
    };
    if (pPEX != NULL && pBB != NULL) {
      bitunblk256v32_scalarBlock_ex_template(
        pIn, pOut, pPEX, pBB,
        expansions_count_13,
        SHIFT_HI_13,
        SHIFT_LO_13,
        READ_FLAG_13,
        mask13,
        nb13
      );
    } else {
        bitunblk256v32_scalar_template(
          pIn, pOut, expansions_count_13,
          SHIFT_HI_13, SHIFT_LO_13, READ_FLAG_13,
          mask13, nb13
        );
    }
}

static void bitunpack256v32_14_scalar(
    uint32_t **pIn,
    uint32_t **pOut,
    uint32_t **pPEX,
    unsigned char **pBB
)
{
    const int nb14 = 14;
    const uint32_t mask14 = (1u << nb14) - 1;  // 0x3FFF
    const int expansions_count_14 = 16;
    static const uint8_t SHIFT_HI_14[16] = {
         0, 14, 28, 10, 24, 6, 20, 2,
         16, 30, 12, 26, 8, 22, 4, 18
    };
    static const uint8_t SHIFT_LO_14[16] = {
         0, 0, 4, 0, 8, 0, 12, 0,
         0, 2, 0, 6, 0, 10, 0, 0
    };
    static const uint8_t READ_FLAG_14[16] = {
         1, 0, 1, 0, 1, 0, 1, 0,
         0, 1, 0, 1, 0, 1, 0, 0
    };
    if (pPEX != NULL && pBB != NULL) {
      for(int i=0; i<2; i++) {
        bitunblk256v32_scalarBlock_ex_template(
          pIn, pOut, pPEX, pBB,
          expansions_count_14,
          SHIFT_HI_14,
          SHIFT_LO_14,
          READ_FLAG_14,
          mask14,
          nb14
        );
      }
    } else {
      for(int i=0; i<2; i++) {
        bitunblk256v32_scalar_template(
          pIn, pOut, expansions_count_14,
          SHIFT_HI_14, SHIFT_LO_14, READ_FLAG_14,
          mask14, nb14
        );
      }
    }
}

static void bitunpack256v32_15_scalar(
    uint32_t **pIn,
    uint32_t **pOut,
    uint32_t **pPEX,
    unsigned char **pBB
)
{
    const int nb15 = 15;
    const uint32_t mask15 = (1u << 15) -1;  // 0x7FFF

    // expansions=32 => unpacks 256 values at once
    const int expansions_count_15 = 32;

    static const uint8_t SHIFT_HI_15[32] = {
        0,15,30,13, 28,11,26, 9,
        24, 7,22, 5, 20, 3,18, 1,
        16,31,14,29, 12,27,10,25,
         8,23, 6,21,  4,19, 2,17
    };

    static const uint8_t SHIFT_LO_15[32] = {
         0, 0, 2, 0,  4, 0, 6, 0,
         8, 0,10, 0, 12, 0,14, 0,
         0, 1, 0, 3,  0, 5, 0, 7,
         0, 9, 0,11,  0,13, 0, 0
    };

    static const uint8_t READ_FLAG_15[32] = {
        1,0,1,0, 1,0,1,0,
        1,0,1,0, 1,0,1,0,
        0,1,0,1, 0,1,0,1,
        0,1,0,1, 0,1,0,0
    };
    if (pPEX != NULL && pBB != NULL) {
      bitunblk256v32_scalarBlock_ex_template(
        pIn, pOut, pPEX, pBB,
        expansions_count_15,
        SHIFT_HI_15,
        SHIFT_LO_15,
        READ_FLAG_15,
        mask15,
        nb15
      );
    } else {
        bitunblk256v32_scalar_template(
          pIn, pOut, expansions_count_15,
          SHIFT_HI_15, SHIFT_LO_15, READ_FLAG_15,
          mask15, nb15
        );
    }
}

static void bitunpack256v32_16_scalar(
    uint32_t **pIn,
    uint32_t **pOut,
    uint32_t **pPEX,
    unsigned char **pBB
)
{
    const int nb16 = 16;
    const uint32_t mask16 = (1u << 16) - 1;  // 0xFFFF

    const int expansions_count = 2;
    // Iteration 0: directly read 8×32-bit; Iteration 1: only right shift 16 bits, no new data read
    static const uint8_t SHIFT_HI_16[2] = { 0, 16 };
    static const uint8_t SHIFT_LO_16[2] = { 0, 0 };
    static const uint8_t READ_FLAG_16[2] = { 1, 0 };

    if (pPEX != NULL && pBB != NULL) {
      for(int i=0; i<16; i++) {
        bitunblk256v32_scalarBlock_ex_template(
          pIn, pOut, pPEX, pBB,
          expansions_count,
          SHIFT_HI_16,
          SHIFT_LO_16,
          READ_FLAG_16,
          mask16,
          nb16
        );
      }
    } else {
      for(int i=0; i<16; i++) {
        bitunblk256v32_scalar_template(
          pIn, pOut, expansions_count,
          SHIFT_HI_16, SHIFT_LO_16, READ_FLAG_16,
          mask16, nb16
        );
      }
    }
}

static void bitunpack256v32_17_scalar(
    uint32_t **pIn,
    uint32_t **pOut,
    uint32_t **pPEX,
    unsigned char **pBB
)
{
    const int nb17 = 17;
    const uint32_t mask17 = (1u << 17) - 1; // 0x1FFFF

    // expansions=32 => unpacks 256 values
    const int expansions_count_17 = 32;

    static const uint8_t SHIFT_HI_17[32] = {
       0,17, 2,19, 4,21, 6,23,
       8,25,10,27,12,29,14,31,
      16, 1,18, 3,20, 5,22, 7,
      24, 9,26,11,28,13,30,15
    };

    static const uint8_t SHIFT_LO_17[32] = {
       0,15, 0,13, 0,11, 0, 9,
       0, 7, 0, 5, 0, 3, 0, 1,
      16, 0,14, 0,12, 0,10, 0,
       8, 0, 6, 0, 4, 0, 2, 0
    };

    static const uint8_t READ_FLAG_17[32] = {
        1,1,0,1, 0,1,0,1,
        0,1,0,1, 0,1,0,1,
        1,0,1,0, 1,0,1,0,
        1,0,1,0, 1,0,1,0
    };
    if (pPEX != NULL && pBB != NULL) {
      bitunblk256v32_scalarBlock_ex_template(
        pIn,
        pOut,
        pPEX,
        pBB,
        expansions_count_17,
        SHIFT_HI_17,
        SHIFT_LO_17,
        READ_FLAG_17,
        mask17,
        nb17
      );
    } else {
      bitunblk256v32_scalar_template(
        pIn, pOut, expansions_count_17,
        SHIFT_HI_17, SHIFT_LO_17, READ_FLAG_17,
        mask17, nb17
      );
    }
}

static void bitunpack256v32_18_scalar(
    uint32_t **pIn,
    uint32_t **pOut,
    uint32_t **pPEX,
    unsigned char **pBB
)
{
    // base bits & mask
    const int nb18 = 18;
    const uint32_t mask18 = (1u << 18) - 1; // 0x3FFFF

    // expansions=16 => 128 values
    const int expansions_count_18 = 16;

    static const uint8_t SHIFT_HI_18[16] = {
         0,18, 4,22,  8,26,12,30,
        16, 2,20, 6, 24,10,28,14
    };
    static const uint8_t SHIFT_LO_18[16] = {
         0,14, 0,10,  0, 6, 0, 2,
        16, 0,12, 0,  8, 0, 4, 0
    };
    static const uint8_t READ_FLAG_18[16] = {
        // #0 =>1, #1 =>1, #2=>0, #3=>1,
        // #4 =>0, #5 =>1, #6=>0, #7=>1,
        // #8 =>1, #9 =>0, #10=>1, #11=>0,
        // #12=>1, #13=>0, #14=>1, #15=>0
         1, 1, 0, 1, 0, 1, 0, 1,
         1, 0, 1, 0, 1, 0, 1, 0
    };

    if (pPEX != NULL && pBB != NULL) {
      for(int i=0; i<2; i++) {
        bitunblk256v32_scalarBlock_ex_template(
          pIn,
          pOut,
          pPEX,
          pBB,
          expansions_count_18,
          SHIFT_HI_18,
          SHIFT_LO_18,
          READ_FLAG_18,
          mask18,
          nb18
        );
      }
    } else {
      for(int i=0; i<2; i++) {
        bitunblk256v32_scalar_template(
          pIn, pOut, expansions_count_18,
          SHIFT_HI_18, SHIFT_LO_18, READ_FLAG_18,
          mask18, nb18
        );
      }
    }
}

static void bitunpack256v32_19_scalar(
    uint32_t **pIn,
    uint32_t **pOut,
    uint32_t **pPEX,
    unsigned char **pBB
)
{
    // base bits & mask
    const int nb19 = 19;
    const uint32_t mask19 = (1u << 19) - 1; // 0x7FFFF

    // expansions=32 => unpacks 256 values at once
    const int expansions_count_19 = 32;

    static const uint8_t SHIFT_HI_19[32] = {
        0,19, 6,25, 12,31,18, 5,
        24,11,30,17,  4,23,10,29,
        16, 3,22, 9, 28,15, 2,21,
         8,27,14, 1, 20, 7,26,13
    };
    static const uint8_t SHIFT_LO_19[32] = {
         0,13, 0, 7,  0, 1,14, 0,
         8, 0, 2,15,  0, 9, 0, 3,
        16, 0,10, 0,  4,17, 0,11,
         0, 5,18, 0, 12, 0, 6, 0
    };
    static const uint8_t READ_FLAG_19[32] = {
        1,1,0,1, 0,1,1,0,
        1,0,1,1, 0,1,0,1,
        1,0,1,0, 1,1,0,1,
        0,1,1,0, 1,0,1,0
    };
    if (pPEX != NULL && pBB != NULL) {
      bitunblk256v32_scalarBlock_ex_template(
        pIn,
        pOut,
        pPEX,
        pBB,
        expansions_count_19,
        SHIFT_HI_19,
        SHIFT_LO_19,
        READ_FLAG_19,
        mask19,
        nb19
      );
    } else {
      bitunblk256v32_scalar_template(
        pIn,
        pOut,
        expansions_count_19,
        SHIFT_HI_19,
        SHIFT_LO_19,
        READ_FLAG_19,
        mask19,
        nb19
      );
    }
}

static void bitunpack256v32_20_scalar(
    uint32_t **pIn,
    uint32_t **pOut,
    uint32_t **pPEX,
    unsigned char **pBB
)
{
    // base bits & mask
    const int nb20 = 20;
    const uint32_t mask20 = (1u << 20) - 1; // 0xFFFFF

    // expansions=8 => process 64 values at once
    const int expansions_count_20 = 8;

    // shift tables for k=0..7
    static const uint8_t SHIFT_HI_20[8] = { 0, 20, 8, 28, 16, 4, 24, 12 };
    static const uint8_t SHIFT_LO_20[8] = { 0, 12, 0,  4, 16, 0,  8,  0 };
    static const uint8_t READ_FLAG_20[8] = { 1, 1, 0,  1,  1, 0,  1,  0 };

    if (pPEX != NULL && pBB != NULL) {
      for(int i=0; i<4; i++) {
        bitunblk256v32_scalarBlock_ex_template(
          pIn,
          pOut,
          pPEX,
          pBB,
          expansions_count_20,
        SHIFT_HI_20,
          SHIFT_LO_20,
          READ_FLAG_20,
          mask20,
          nb20
        );
      }
    } else {
      for(int i=0; i<4; i++) {
        bitunblk256v32_scalar_template(
          pIn, pOut, expansions_count_20,
          SHIFT_HI_20, SHIFT_LO_20, READ_FLAG_20,
          mask20, nb20
        );
      }
    }
}

static void bitunpack256v32_21_scalar(
    uint32_t **pIn,
    uint32_t **pOut,
    uint32_t **pPEX,
    unsigned char **pBB
)
{
    // base bits & mask
    const uint32_t mask21 = (1u << 21) - 1; // 0x1FFFFF
    const int nb21 = 21;

    // expansions=32 => unpacks 256 values at once
    const int expansions_count_21 = 32;

    static const uint8_t SHIFT_HI_21[32] = {
        0,21,10,31, 20, 9,30,19,
        8,29,18, 7, 28,17, 6,27,
        16, 5,26,15, 4,25,14, 3,
        24,13, 2,23, 12, 1,22,11
    };
    static const uint8_t SHIFT_LO_21[32] = {
         0,11, 0, 1, 12, 0, 2,13,
         0, 3,14, 0,  4,15, 0, 5,
        16, 0, 6,17,  0, 7,18, 0,
         8,19, 0, 9, 20, 0,10, 0
    };
    static const uint8_t READ_FLAG_21[32] = {
        // Check original expansions #k if there's a "load #X" => 1 if yes, 0 if no
        1,1,0,1, 1,0,1,1,
        0,1,1,0, 1,1,0,1,
        1,0,1,1, 0,1,1,0,
        1,1,0,1, 1,0,1,0
    };

    if (pPEX != NULL && pBB != NULL) {
      bitunblk256v32_scalarBlock_ex_template(
        pIn, pOut, pPEX, pBB,
        expansions_count_21,
        SHIFT_HI_21,
        SHIFT_LO_21,
        READ_FLAG_21,
        mask21,
        nb21
      );
    } else {
      bitunblk256v32_scalar_template(
        pIn, pOut, expansions_count_21,
        SHIFT_HI_21, SHIFT_LO_21, READ_FLAG_21,
        mask21, nb21
      );
    }
}

static void bitunpack256v32_22_scalar(
    uint32_t **pIn,
    uint32_t **pOut,
    uint32_t **pPEX,
    unsigned char **pBB
)
{
    // base bits & mask
    const uint32_t mask22 = (1u << 22) - 1; // 0x3FFFFF
    const int nb22 = 22;

    // b=22 => one block function with expansions=16 => outputs 128 values
    // need to call it twice to get 256 values
    const int expansions_count_22 = 16;

    static const uint8_t SHIFT_HI_22[16] = {
        /* 0 */ 0,  /* 1 */22, /* 2 */12, /* 3 */ 2,
        /* 4 */24,  /* 5 */14, /* 6 */ 4, /* 7 */26,
        /* 8 */16,  /* 9 */ 6, /*10 */28, /*11 */18,
        /*12 */ 8,  /*13 */30, /*14 */20, /*15 */10
    };

    static const uint8_t SHIFT_LO_22[16] = {
        /* 0 */ 0,  /* 1 */10, /* 2 */20, /* 3 */ 0,
        /* 4 */ 8,  /* 5 */18, /* 6 */ 0,  /* 7 */ 6,
        /* 8 */16,  /* 9 */ 0, /*10 */ 4,  /*11 */14,
        /*12 */ 0,  /*13 */ 2, /*14 */12, /*15 */ 0
    };

    static const uint8_t READ_FLAG_22[16] = {
        // From original code: expansions #3, #6, #9, #12, #15 don't read, others do
        1,1,1,0, 1,1,0,1, 1,0,1,1, 0,1,1,0
    };

    if (pPEX != NULL && pBB != NULL) {
      for(int i=0; i<2; i++) {
        bitunblk256v32_scalarBlock_ex_template(
          pIn,
          pOut,
          pPEX,
          pBB,
          expansions_count_22,
          SHIFT_HI_22,
          SHIFT_LO_22,
          READ_FLAG_22,
          mask22,
          nb22
        );
      }
    } else {
      for(int i=0; i<2; i++) {
        bitunblk256v32_scalar_template(
          pIn, pOut, expansions_count_22,
          SHIFT_HI_22, SHIFT_LO_22, READ_FLAG_22,
          mask22, nb22
        );
      }
    }
}

static void bitunpack256v32_23_scalar(
    uint32_t **pIn,
    uint32_t **pOut,
    uint32_t **pPEX,
    unsigned char **pBB
)
{
    // base bits & mask
    const int nb23 = 23;
    const uint32_t mask23 = (1u << 23) - 1; // 0x7FFFFF

    // expansions_count=32
    const int expansions_count_23 = 32;

    // Predefined SHIFT_HI_23, SHIFT_LO_23, READ_FLAG_23
    static const uint8_t SHIFT_HI_23[32] = {
        0, 23,14, 5, 28,19,10, 1,
        24,15, 6,29, 20,11, 2,25,
        16, 7,30,21, 12, 3,26,17,
         8,31,22,13,  4,27,18, 9
    };

    static const uint8_t SHIFT_LO_23[32] = {
         0,  9,18, 0,  4,13,22, 0,
         8,17, 0, 3, 12,21, 0, 7,
        16,  0, 2,11, 20, 0, 6,15,
         0,  1,10,19,  0, 5,14, 0
    };

    static const uint8_t READ_FLAG_23[32] = {
        1,1,1,0, 1,1,1,0, 1,1,0,1, 1,1,0,1,
        1,0,1,1, 1,0,1,1, 0,1,1,1, 0,1,1,0
    };

    if (pPEX != NULL && pBB != NULL) {
        bitunblk256v32_scalarBlock_ex_template(
            pIn, pOut, pPEX, pBB,
            expansions_count_23,
            SHIFT_HI_23,
            SHIFT_LO_23,
            READ_FLAG_23,
            mask23,
            nb23
        );
    } else {
      bitunblk256v32_scalar_template(
        pIn, pOut, expansions_count_23,
        SHIFT_HI_23, SHIFT_LO_23, READ_FLAG_23,
        mask23, nb23
      );
    }
}
static void bitunpack256v32_24_scalar(
    uint32_t **pIn,
    uint32_t **pOut,
    uint32_t **pPEX,
    unsigned char **pBB
)
{
    // base bits & mask
    const int nb24 = 24;
    const uint32_t mask24 = (1u << 24) - 1;  // 0xFFFFFF

    // expansions_count=4 (corresponds to 4 expansions => outputs 32 values)
    const int expansions_count_24 = 4;

    // k=0 => leftover>>0, new<<0
    // k=1 => leftover>>24, new<<8
    // k=2 => leftover>>16, new<<16
    // k=3 => leftover>>8,  no new block read
    static const uint8_t SHIFT_HI_24[4] = {0, 24, 16, 8};
    static const uint8_t SHIFT_LO_24[4] = {0,  8, 16, 0};

    // Only read new blocks for steps 0,1,2, not for step 3
    static const uint8_t READ_FLAG_24[4] = {1, 1, 1, 0};

    if (pPEX != NULL && pBB != NULL) {
          for(int i=0; i<8; i++) {
            bitunblk256v32_scalarBlock_ex_template(
                pIn, pOut, pPEX, pBB,
                expansions_count_24,
                SHIFT_HI_24,
                SHIFT_LO_24,
                READ_FLAG_24,
            mask24,
            nb24
        );
      }
    } else {
      for(int i=0; i<8; i++) {
        bitunblk256v32_scalar_template(
          pIn, pOut, expansions_count_24, 
          SHIFT_HI_24, SHIFT_LO_24, READ_FLAG_24,
          mask24, nb24
        );
      }
    }
}

static void bitunpack256v32_25_scalar(uint32_t **pIn,
                                             uint32_t **pOut,
                                             uint32_t **pPEX,
                                             unsigned char **pBB)
{
    // mask & base bits
    const uint32_t mask25 = (1u<<25) -1;  // 0x1FFFFFF
    const int nb25 = 25;

    // 32 expansions total
    const int expansions_count_25 = 32;

    // Extract high and low shift amounts from original implementation
    static const uint8_t SHIFT_HI_25[32] = {
        /* #0  */  0, /* #1  */ 25, /* #2  */ 18, /* #3  */ 11, 
        /* #4  */  4, /* #5  */ 29, /* #6  */ 22, /* #7  */ 15, 
        /* #8  */  8, /* #9  */  1, /* #10 */ 26, /* #11 */ 19, 
        /* #12 */ 12, /* #13 */  5, /* #14 */ 30, /* #15 */ 23, 
        /* #16 */ 16, /* #17 */  9, /* #18 */  2, /* #19 */ 27, 
        /* #20 */ 20, /* #21 */ 13, /* #22 */  6, /* #23 */ 31, 
        /* #24 */ 24, /* #25 */ 17, /* #26 */ 10, /* #27 */  3, 
        /* #28 */ 28, /* #29 */ 21, /* #30 */ 14, /* #31 */  7
    };

    static const uint8_t SHIFT_LO_25[32] = {
        /* #0  */  0, /* #1  */  7, /* #2  */ 14, /* #3  */ 21, 
        /* #4  */  0, /* #5  */  3, /* #6  */ 10, /* #7  */ 17, 
        /* #8  */ 24, /* #9  */  0, /* #10 */  6, /* #11 */ 13, 
        /* #12 */ 20, /* #13 */  0, /* #14 */  2, /* #15 */  9, 
        /* #16 */ 16, /* #17 */ 23, /* #18 */  0, /* #19 */  5, 
        /* #20 */ 12, /* #21 */ 19, /* #22 */  0, /* #23 */  1, 
        /* #24 */  8, /* #25 */ 15, /* #26 */ 22, /* #27 */  0, 
        /* #28 */  4, /* #29 */ 11, /* #30 */ 18, /* #31 */  0
    };

    // Mark which steps don't need to read new data
    // Based on original code, expansions #4, #9, #13, #18, #22, #27, #31 don't need to read new data
    static const uint8_t READ_FLAG_25[32] = {
        /* #0  */ 1, /* #1  */ 1, /* #2  */ 1, /* #3  */ 1, 
        /* #4  */ 0, /* #5  */ 1, /* #6  */ 1, /* #7  */ 1, 
        /* #8  */ 1, /* #9  */ 0, /* #10 */ 1, /* #11 */ 1, 
        /* #12 */ 1, /* #13 */ 0, /* #14 */ 1, /* #15 */ 1, 
        /* #16 */ 1, /* #17 */ 1, /* #18 */ 0, /* #19 */ 1, 
        /* #20 */ 1, /* #21 */ 1, /* #22 */ 0, /* #23 */ 1, 
        /* #24 */ 1, /* #25 */ 1, /* #26 */ 1, /* #27 */ 0, 
        /* #28 */ 1, /* #29 */ 1, /* #30 */ 1, /* #31 */ 0
    };
    if (pPEX != NULL && pBB != NULL) {
      for (int i=0; i<2; i++) {
        bitunblk256v32_scalarBlock_ex_template(
            pIn, pOut, pPEX, pBB,
            expansions_count_25,
            SHIFT_HI_25,
            SHIFT_LO_25,
            READ_FLAG_25,
            mask25,
            nb25
        );
      }
    } else {
      bitunblk256v32_scalar_template(
        pIn, pOut, expansions_count_25,
        SHIFT_HI_25, SHIFT_LO_25, READ_FLAG_25,
        mask25, nb25
      );
    }
}

static void bitunpack256v32_26_scalar(uint32_t **pIn,
                                             uint32_t **pOut,
                                             uint32_t **pPEX,
                                             unsigned char **pBB)
{
    // mask & base bits
    const uint32_t mask26 = (1u<<26) -1;  // 0x3FFFFFF
    const int nb26 = 26;

    // 16 expansions total
    const int expansions_count_26 = 16;

    // Extract high and low shift amounts from original implementation
    static const uint8_t SHIFT_HI_26[16] = {
        /* #0  */  0, /* #1  */ 26, /* #2  */ 20, /* #3  */ 14, 
        /* #4  */  8, /* #5  */  2, /* #6  */ 28, /* #7  */ 22, 
        /* #8  */ 16, /* #9  */ 10, /* #10 */  4, /* #11 */ 30, 
        /* #12 */ 24, /* #13 */ 18, /* #14 */ 12, /* #15 */  6
    };

    static const uint8_t SHIFT_LO_26[16] = {
        /* #0  */  0, /* #1  */  6, /* #2  */ 12, /* #3  */ 18, 
        /* #4  */ 24, /* #5  */  0, /* #6  */  4, /* #7  */ 10, 
        /* #8  */ 16, /* #9  */ 22, /* #10 */  0, /* #11 */  2, 
        /* #12 */  8, /* #13 */ 14, /* #14 */ 20, /* #15 */  0
    };

    // Mark which steps don't need to read new data
    // Based on original code, expansions #5, #10, #15 don't need to read new data
    static const uint8_t READ_FLAG_26[16] = {
        /* #0  */ 1, /* #1  */ 1, /* #2  */ 1, /* #3  */ 1, 
        /* #4  */ 1, /* #5  */ 0, /* #6  */ 1, /* #7  */ 1, 
        /* #8  */ 1, /* #9  */ 1, /* #10 */ 0, /* #11 */ 1, 
        /* #12 */ 1, /* #13 */ 1, /* #14 */ 1, /* #15 */ 0
    };

    if (pPEX != NULL && pBB != NULL) {
      for (int i=0; i<2; i++) {
        bitunblk256v32_scalarBlock_ex_template(
            pIn, pOut, pPEX, pBB,
            expansions_count_26,
            SHIFT_HI_26,
            SHIFT_LO_26,
            READ_FLAG_26,
            mask26,
            nb26
        );
      }
    } else {
      for (int i=0; i<2; i++) {
        bitunblk256v32_scalar_template(
            pIn, pOut, expansions_count_26,
            SHIFT_HI_26, SHIFT_LO_26, READ_FLAG_26,
            mask26, nb26
        );
      }
    }
}

static void bitunpack256v32_27_scalar(uint32_t **pIn,
                                             uint32_t **pOut,
                                             uint32_t **pPEX,
                                             unsigned char **pBB)
{
    // mask & base bits
    const uint32_t mask27 = (1u<<27) -1;  // 0x7FFFFFF
    const int nb27 = 27;

    // 32 expansions total
    const int expansions_count_27 = 32;

    // Extract high and low shift amounts from original implementation
    static const uint8_t SHIFT_HI_27[32] = {
        /* #0  */  0, /* #1  */ 27, /* #2  */ 22, /* #3  */ 17,
        /* #4  */ 12, /* #5  */  7, /* #6  */  2, /* #7  */ 29,
        /* #8  */ 24, /* #9  */ 19, /* #10 */ 14, /* #11 */  9,
        /* #12 */  4, /* #13 */ 31, /* #14 */ 26, /* #15 */ 21,
        /* #16 */ 16, /* #17 */ 11, /* #18 */  6, /* #19 */  1,
        /* #20 */ 28, /* #21 */ 23, /* #22 */ 18, /* #23 */ 13,
        /* #24 */  8, /* #25 */  3, /* #26 */ 30, /* #27 */ 25,
        /* #28 */ 20, /* #29 */ 15, /* #30 */ 10, /* #31 */  5
    };

    static const uint8_t SHIFT_LO_27[32] = {
        /* #0  */  0, /* #1  */  5, /* #2  */ 10, /* #3  */ 15,
        /* #4  */ 20, /* #5  */ 25, /* #6  */  0, /* #7  */  3,
        /* #8  */  8, /* #9  */ 13, /* #10 */ 18, /* #11 */ 23,
        /* #12 */  0, /* #13 */  1, /* #14 */  6, /* #15 */ 11,
        /* #16 */ 16, /* #17 */ 21, /* #18 */ 26, /* #19 */  0,
        /* #20 */  4, /* #21 */  9, /* #22 */ 14, /* #23 */ 19,
        /* #24 */ 24, /* #25 */  0, /* #26 */  2, /* #27 */  7,
        /* #28 */ 12, /* #29 */ 17, /* #30 */ 22, /* #31 */  0
    };

    // Mark which steps don't need to read new data
    // From original code, steps #6, #12, #19, #25, #31 don't have CPY8(iv, *pIn)
    static const uint8_t READ_FLAG_27[32] = {
        /* #0  */ 1, /* #1  */ 1, /* #2  */ 1, /* #3  */ 1,
        /* #4  */ 1, /* #5  */ 1, /* #6  */ 0, /* #7  */ 1,
        /* #8  */ 1, /* #9  */ 1, /* #10 */ 1, /* #11 */ 1,
        /* #12 */ 0, /* #13 */ 1, /* #14 */ 1, /* #15 */ 1,
        /* #16 */ 1, /* #17 */ 1, /* #18 */ 1, /* #19 */ 0,
        /* #20 */ 1, /* #21 */ 1, /* #22 */ 1, /* #23 */ 1,
        /* #24 */ 1, /* #25 */ 0, /* #26 */ 1, /* #27 */ 1,
        /* #28 */ 1, /* #29 */ 1, /* #30 */ 1, /* #31 */ 0
    };
    if (pPEX != NULL && pBB != NULL) {
        bitunblk256v32_scalarBlock_ex_template(
            pIn, pOut, pPEX, pBB,
            expansions_count_27,
            SHIFT_HI_27,
            SHIFT_LO_27,
            READ_FLAG_27,
        mask27,
            nb27
        );
    } else {
        bitunblk256v32_scalar_template(
            pIn, pOut, expansions_count_27,
            SHIFT_HI_27, SHIFT_LO_27, READ_FLAG_27,
            mask27, nb27
        );
    }
}
static void bitunpack256v32_28_scalar(uint32_t **pIn,
                               uint32_t **pOut,
                               uint32_t **pPEX,      // Optional parameter, non-NULL for extended version
                               unsigned char **pBB)  // Optional parameter, non-NULL for extended version
{
    // Common constant definitions
    static const uint32_t BITUN_MASK_28      = (1u << 28) - 1;  // 0xFFFFFFF
    static const int      BITUN_NB_28        = 28;
    static const int      EXPANSIONS_COUNT_28 = 8;
    static const uint8_t  SHIFT_HI_28[EXPANSIONS_COUNT_28] = { 0, 28, 24, 20, 16, 12, 8, 4 };
    static const uint8_t  SHIFT_LO_28[EXPANSIONS_COUNT_28] = { 0, 4, 8, 12, 16, 20, 24, 0 };
    static const uint8_t  READ_FLAG_28[EXPANSIONS_COUNT_28] = { 1, 1, 1, 1, 1, 1, 1, 0 };

    // Choose template based on whether extension parameters are provided
    if (pPEX != NULL && pBB != NULL) {
        // Call extended template, each call outputs 64 values, loop 4 times to get 256
        for (int i = 0; i < 4; i++) {
            bitunblk256v32_scalarBlock_ex_template(
                pIn, pOut, pPEX, pBB,
                EXPANSIONS_COUNT_28,
                SHIFT_HI_28,
                SHIFT_LO_28,
                READ_FLAG_28,
                BITUN_MASK_28,
                BITUN_NB_28
            );
        }
    } else {
        // Call non-extended template, also each call outputs 64 values, loop 4 times to get 256
        for (int i = 0; i < 4; i++) {
            bitunblk256v32_scalar_template(
                pIn, pOut,
                EXPANSIONS_COUNT_28,
                SHIFT_HI_28,
                SHIFT_LO_28,
                READ_FLAG_28,
                BITUN_MASK_28,
                BITUN_NB_28
            );
        }
    }
}


static void bitunpack256v32_29_scalar(uint32_t **pIn, uint32_t **pOut,
                                      uint32_t **pPEX,
                                      unsigned char **pBB)
{
    const uint32_t mask29 = (1U << 29) - 1;  // 0x1FFFFFFF
    const int expansions_count = 32;
    static const uint8_t SHIFT_HI_29[32] = {
         0, 29, 26, 23, 20, 17, 14, 11,
         8,  5,  2, 31, 28, 25, 22, 19,
        16, 13, 10,  7,  4,  1, 30, 27,
        24, 21, 18, 15, 12,  9,  6,  3
    };
    static const uint8_t SHIFT_LO_29[32] = {
         0,  3,  6,  9, 12, 15, 18, 21,
        24, 27,  0,  1,  4,  7, 10, 13,
        16, 19, 22, 25, 28,  0,  2,  5,
         8, 11, 14, 17, 20, 23, 26,  0
    };
    static const uint8_t READ_FLAG_29[32] = {
         1, 1, 1, 1, 1, 1, 1, 1,
         1, 1, 0, 1, 1, 1, 1, 1,
         1, 1, 1, 1, 1, 0, 1, 1,
         1, 1, 1, 1, 1, 1, 1, 0
    };
    if (pPEX != NULL && pBB != NULL) {
        bitunblk256v32_scalarBlock_ex_template(
            pIn, pOut, pPEX, pBB,
            expansions_count,
            SHIFT_HI_29,
            SHIFT_LO_29,
            READ_FLAG_29,
            mask29,
            29
        );
    } else {
        bitunblk256v32_scalar_template(
            pIn, pOut,
            expansions_count,
            SHIFT_HI_29,
            SHIFT_LO_29,
            READ_FLAG_29,
            mask29,
            29
        );
    }
}

static void bitunpack256v32_30_scalar(uint32_t **pIn, uint32_t **pOut,
                                      uint32_t **pPEX,
                                      unsigned char **pBB)
{
    const uint32_t mask30 = (1U << 30) - 1;  // 0x3FFFFFFF
    const int expansions_count = 16;
    static const uint8_t SHIFT_HI_30[16] = {
         0, 30, 28, 26, 24, 22, 20, 18,
        16, 14, 12, 10,  8,  6,  4,  2
    };
    static const uint8_t SHIFT_LO_30[16] = {
         0,  2,  4,  6,  8, 10, 12, 14,
        16, 18, 20, 22, 24, 26, 28,  0
    };
    static const uint8_t READ_FLAG_30[16] = {
         1, 1, 1, 1, 1, 1, 1, 1,
         1, 1, 1, 1, 1, 1, 1, 0
    };

    if (pPEX != NULL && pBB != NULL) {
      for (int i = 0; i < 2; i++) {
        bitunblk256v32_scalarBlock_ex_template(
            pIn, pOut, pPEX, pBB,
            expansions_count,
            SHIFT_HI_30, SHIFT_LO_30, READ_FLAG_30,
            mask30, 30
        );
      }
    } else {
        for (int i = 0; i < 2; i++) {
            bitunblk256v32_scalar_template(
                pIn, pOut, expansions_count,
                SHIFT_HI_30, SHIFT_LO_30, READ_FLAG_30,
                mask30, 30
            );
        }
    }
}

// 31 位非异常 PFOR 解压函数，采用模板函数优化
static void bitunpack256v32_31_scalar(uint32_t **pIn, uint32_t **pOut,
                                      uint32_t **pPEX,
                                      unsigned char **pBB)
{
    // 31 位掩码
    const uint32_t mask31 = (1U << 31) - 1;  // 0x7FFFFFFF
    const int expansions_count = 32;
    // 构造参数数组：
    // 对于 k==0: SHIFT_HI = 0, SHIFT_LO = 0, READ_FLAG = 1
    // 对于 k = 1 .. 30: SHIFT_HI = 32 - k, SHIFT_LO = k, READ_FLAG = 1
    // 对于 k==31: SHIFT_HI = 1, SHIFT_LO = 0, READ_FLAG = 0
    static const uint8_t SHIFT_HI[32] = {
         0,
        31, 30, 29, 28, 27, 26, 25, 24,
        23, 22, 21, 20, 19, 18, 17, 16,
        15, 14, 13, 12, 11, 10, 9, 8,
         7, 6, 5, 4, 3, 2, 1
    };
    static const uint8_t SHIFT_LO[32] = {
         0,
         1,  2,  3,  4,  5,  6,  7,  8,
         9, 10, 11, 12, 13, 14, 15, 16,
        17, 18, 19, 20, 21, 22, 23, 24,
        25, 26, 27, 28, 29, 30, 31, 0
    };
    static const uint8_t READ_FLAG[32] = {
         1,
         1, 1, 1, 1, 1, 1, 1, 1,
         1, 1, 1, 1, 1, 1, 1, 1,
         1, 1, 1, 1, 1, 1, 1, 1,
         1, 1, 1, 1, 1, 1, 1, 0
    };
    if (pPEX != NULL && pBB != NULL) {
        bitunblk256v32_scalarBlock_ex_template(
            pIn, pOut, pPEX, pBB,
            expansions_count,
            SHIFT_HI, SHIFT_LO, READ_FLAG,
            mask31, 31
        );
    } else {
        bitunblk256v32_scalar_template(
            pIn, pOut,
            expansions_count,
            SHIFT_HI,
            SHIFT_LO,
         READ_FLAG,
         mask31,
         31
        );
    }
}

static void bitunpack256v32_32_scalar(uint32_t **pIn,
                               uint32_t **pOut,
                               uint32_t **pPEX,      // Optional parameter, non-NULL for extended version
                               unsigned char **pBB)  // Optional parameter, non-NULL for extended version
{
    uint32_t *ip = *pIn;
    uint32_t *op = *pOut;
    const int nb = 32;  // When b=32, each 32-bit integer stores a value directly

    // There are 32 groups, each group has 8 numbers, totaling 256 numbers
    for (int i = 0; i < 32; i++) {
        // Copy 8 input values directly to output (avoid calling CPY8)
        for (int j = 0; j < 8; j++) {
            op[j] = ip[j];
        }
        ip += 8;

        if (pPEX != NULL && pBB != NULL) {
            uint8_t xm8 = **pBB;
            (*pBB)++;
            if (xm8 != 0) {
                applyException_8bits(xm8, pPEX, nb, op);
            }
        }
        op += 8;
    }
    *pIn = ip;
    *pOut = op;
}

// Define function pointer type for unpacking functions
typedef void (*unpack_func_t)(uint32_t**, uint32_t**, unsigned**, unsigned char**);

// Array of function pointers for each bit width (0 to 32)
static unpack_func_t unpack_funcs[33] = {
    bitunpack256v32_0_scalar,
    bitunpack256v32_1_scalar,
    bitunpack256v32_2_scalar,
    bitunpack256v32_3_scalar,
    bitunpack256v32_4_scalar,
    bitunpack256v32_5_scalar,
    bitunpack256v32_6_scalar,
    bitunpack256v32_7_scalar,
    bitunpack256v32_8_scalar,
    bitunpack256v32_9_scalar,
    bitunpack256v32_10_scalar,
    bitunpack256v32_11_scalar,
    bitunpack256v32_12_scalar,
    bitunpack256v32_13_scalar,
    bitunpack256v32_14_scalar,
    bitunpack256v32_15_scalar,
    bitunpack256v32_16_scalar,
    bitunpack256v32_17_scalar,
    bitunpack256v32_18_scalar,
    bitunpack256v32_19_scalar,
    bitunpack256v32_20_scalar,
    bitunpack256v32_21_scalar,
    bitunpack256v32_22_scalar,
    bitunpack256v32_23_scalar,
    bitunpack256v32_24_scalar,
    bitunpack256v32_25_scalar,
    bitunpack256v32_26_scalar,
    bitunpack256v32_27_scalar,
    bitunpack256v32_28_scalar,
    bitunpack256v32_29_scalar,
    bitunpack256v32_30_scalar,
    bitunpack256v32_31_scalar,
    bitunpack256v32_32_scalar
};
/**
 *
 * @param in   Compressed data input stream
 * @param n    Currently unused, can be processed according to actual needs
 * @param out  Output buffer for decompressed 32-bit integers (must accommodate at least 256 32-bit integers)
 * @param b    Bit width for each integer, this example only demonstrates the b=8 branch
 * @return     Returns the next readable input position after decompression (consistent with original logic)
 */
unsigned char *bitunpack256scalarv32(const unsigned char *__restrict in, 
                               unsigned n, 
                               unsigned *__restrict out, 
                               unsigned b)
{
    // Debug output (optional, can be removed in production)
    printf("bitunpack256scalarv32 b=%d bits=%d\n", b, b&0x3f);

    // Calculate input pointer offset
    unsigned char *ip = (unsigned char *)(in + PAD8(256 * b));
    
    // Initialize pointers
    uint32_t *pIn32  = (uint32_t *)in;
    uint32_t *pOut32 = (uint32_t *)out;

    unsigned bits = b & 0x3f;
    // Execute unpacking if b is in valid range
    if (bits <= 32) {
        unpack_funcs[bits](&pIn32, &pOut32, NULL, NULL);
    }

    return ip;
}

unsigned char *_bitd1unpack256scalarv32(const unsigned char *__restrict in, unsigned n,
                                  unsigned *__restrict out, unsigned start, unsigned b,
                                  unsigned *__restrict pex, unsigned char *bb) {
printf("_bitd1unpack256scalarv32, b=%d\n", b);
unsigned* deltas = (unsigned*)malloc(n * sizeof(unsigned));
    if (!deltas) return NULL;

    const unsigned char *orig_in = in;
    in = _bitunpack256scalarv32(in, n, deltas, b, pex, bb);

    unsigned running_sum = start;
    for (unsigned i = 0; i < n; ++i) {
        running_sum += deltas[i] + 1;
        out[i] = running_sum;
    }

    free(deltas);
    return (unsigned char*)in;
}

// Add this after the definition of _bitunpack256w32 in the SSE2/SSSE3 section

// Delta1 unpacking for 256 32-bit integers (no exceptions)
unsigned char *bitd1unpack256scalarv32(const unsigned char *__restrict in, unsigned n, unsigned *__restrict out, unsigned start, unsigned b) {
    printf("bitd1unpack256scalarv32, b=%d\n", b);
    const unsigned char *_in = in;
    unsigned deltas[n];

    in = bitunpack256scalarv32(in, n, deltas, b);

    unsigned running_sum = start;
    for (unsigned i = 0; i < n; ++i) {
        running_sum += deltas[i] + 1;
        out[i] = running_sum;
    }

    return (unsigned char*)in;
}

unsigned char *_bitunpack256scalarv32(const unsigned char *__restrict in,
                                unsigned n,
                                unsigned *__restrict out,
                                unsigned b,
                                unsigned *__restrict pex,
                                unsigned char *bb)
{
    // Debug output (optional, can be removed in production)
    printf("_bitunpack256scalarv32 bits=%d\n", b & 0x3f);

    // Calculate input pointer offset
    unsigned char *ip = (unsigned char *)(in + PAD8(256 * b));
    
    // Initialize pointers
    unsigned *pPEX = pex;
    unsigned char *pBB = bb;
    uint32_t *pIn32 = (uint32_t *)in;
    uint32_t *pOut32 = (uint32_t *)out;

    unsigned bits = b & 0x3f;
    // Execute unpacking if b is in valid range
    if (bits <= 32) {
        unpack_funcs[bits](&pIn32, &pOut32, &pPEX, &pBB);
    }

    return ip;
}

#define STOZ64(_op_, _ov_) _mm_storeu_si128(_op_++, _ov_); _mm_storeu_si128(_op_++, _ov_)
#define STO64( _op_, _ov_, _zv_) _mm_storeu_si128(_op_++, _mm_unpacklo_epi32(_ov_,_zv_));_mm_storeu_si128(_op_++, _mm_unpacklo_epi32(_mm_srli_si128(_ov_,8),_zv_))

#define VOZ32(_op_, _i_, ov, _nb_,_parm_) STOZ64(_op_, _parm_)
#define VO32( _op_, _i_, ov, _nb_,_parm_) STO64(_op_, ov, zv)
#include "bitunpack_.h"
unsigned char *bitunpack128v64( const unsigned char *__restrict in, unsigned n, uint64_t      *__restrict out, unsigned b) {
  if(b <= 32) { const unsigned char *ip = in+PAD8(128*b);
    __m128i sv,zv = _mm_setzero_si128();
    BITUNPACK128V32(in, b, out, sv);
    return (unsigned char *)ip;
  } else return bitunpack64(in,n,out,b);
}
#undef VO32
#undef VOZ32
#undef VO16
#undef VOZ16
#undef BITUNPACK0

    #if defined(__SSSE3__) || defined(__ARM_NEON)
  #define _ 0x80
ALIGNED(char, _shuffle_32[16][16],16) = {
        { _,_,_,_, _,_,_,_, _,_, _, _,  _, _, _,_  },
        { 0,1,2,3, _,_,_,_, _,_, _, _,  _, _, _,_  },
        { _,_,_,_, 0,1,2,3, _,_, _, _,  _, _, _,_  },
        { 0,1,2,3, 4,5,6,7, _,_, _, _,  _, _, _,_  },
        { _,_,_,_, _,_,_,_, 0,1, 2, 3,  _, _, _,_  },
        { 0,1,2,3, _,_,_,_, 4,5, 6, 7,  _, _, _,_  },
        { _,_,_,_, 0,1,2,3, 4,5, 6, 7,  _, _, _,_  },
        { 0,1,2,3, 4,5,6,7, 8,9,10,11,  _, _, _,_  },
        { _,_,_,_, _,_,_,_, _,_,_,_,    0, 1, 2, 3 },
        { 0,1,2,3, _,_,_,_, _,_,_,  _,  4, 5, 6, 7 },
        { _,_,_,_, 0,1,2,3, _,_,_,  _,  4, 5, 6, 7 },
        { 0,1,2,3, 4,5,6,7, _,_, _, _,  8, 9,10,11 },
        { _,_,_,_, _,_,_,_, 0,1, 2, 3,  4, 5, 6, 7 },
        { 0,1,2,3, _,_,_,_, 4,5, 6, 7,  8, 9,10,11 },
        { _,_,_,_, 0,1,2,3, 4,5, 6, 7,  8, 9,10,11 },
        { 0,1,2,3, 4,5,6,7, 8,9,10,11, 12,13,14,15 },
};
ALIGNED(char, _shuffle_16[256][16],16) = {
  { _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _ },
  { 0, 1, _, _, _, _, _, _, _, _, _, _, _, _, _, _ },
  { _, _, 0, 1, _, _, _, _, _, _, _, _, _, _, _, _ },
  { 0, 1, 2, 3, _, _, _, _, _, _, _, _, _, _, _, _ },
  { _, _, _, _, 0, 1, _, _, _, _, _, _, _, _, _, _ },
  { 0, 1, _, _, 2, 3, _, _, _, _, _, _, _, _, _, _ },
  { _, _, 0, 1, 2, 3, _, _, _, _, _, _, _, _, _, _ },
  { 0, 1, 2, 3, 4, 5, _, _, _, _, _, _, _, _, _, _ },
  { _, _, _, _, _, _, 0, 1, _, _, _, _, _, _, _, _ },
  { 0, 1, _, _, _, _, 2, 3, _, _, _, _, _, _, _, _ },
  { _, _, 0, 1, _, _, 2, 3, _, _, _, _, _, _, _, _ },
  { 0, 1, 2, 3, _, _, 4, 5, _, _, _, _, _, _, _, _ },
  { _, _, _, _, 0, 1, 2, 3, _, _, _, _, _, _, _, _ },
  { 0, 1, _, _, 2, 3, 4, 5, _, _, _, _, _, _, _, _ },
  { _, _, 0, 1, 2, 3, 4, 5, _, _, _, _, _, _, _, _ },
  { 0, 1, 2, 3, 4, 5, 6, 7, _, _, _, _, _, _, _, _ },
  { _, _, _, _, _, _, _, _, 0, 1, _, _, _, _, _, _ },
  { 0, 1, _, _, _, _, _, _, 2, 3, _, _, _, _, _, _ },
  { _, _, 0, 1, _, _, _, _, 2, 3, _, _, _, _, _, _ },
  { 0, 1, 2, 3, _, _, _, _, 4, 5, _, _, _, _, _, _ },
  { _, _, _, _, 0, 1, _, _, 2, 3, _, _, _, _, _, _ },
  { 0, 1, _, _, 2, 3, _, _, 4, 5, _, _, _, _, _, _ },
  { _, _, 0, 1, 2, 3, _, _, 4, 5, _, _, _, _, _, _ },
  { 0, 1, 2, 3, 4, 5, _, _, 6, 7, _, _, _, _, _, _ },
  { _, _, _, _, _, _, 0, 1, 2, 3, _, _, _, _, _, _ },
  { 0, 1, _, _, _, _, 2, 3, 4, 5, _, _, _, _, _, _ },
  { _, _, 0, 1, _, _, 2, 3, 4, 5, _, _, _, _, _, _ },
  { 0, 1, 2, 3, _, _, 4, 5, 6, 7, _, _, _, _, _, _ },
  { _, _, _, _, 0, 1, 2, 3, 4, 5, _, _, _, _, _, _ },
  { 0, 1, _, _, 2, 3, 4, 5, 6, 7, _, _, _, _, _, _ },
  { _, _, 0, 1, 2, 3, 4, 5, 6, 7, _, _, _, _, _, _ },
  { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, _, _, _, _, _, _ },
  { _, _, _, _, _, _, _, _, _, _, 0, 1, _, _, _, _ },
  { 0, 1, _, _, _, _, _, _, _, _, 2, 3, _, _, _, _ },
  { _, _, 0, 1, _, _, _, _, _, _, 2, 3, _, _, _, _ },
  { 0, 1, 2, 3, _, _, _, _, _, _, 4, 5, _, _, _, _ },
  { _, _, _, _, 0, 1, _, _, _, _, 2, 3, _, _, _, _ },
  { 0, 1, _, _, 2, 3, _, _, _, _, 4, 5, _, _, _, _ },
  { _, _, 0, 1, 2, 3, _, _, _, _, 4, 5, _, _, _, _ },
  { 0, 1, 2, 3, 4, 5, _, _, _, _, 6, 7, _, _, _, _ },
  { _, _, _, _, _, _, 0, 1, _, _, 2, 3, _, _, _, _ },
  { 0, 1, _, _, _, _, 2, 3, _, _, 4, 5, _, _, _, _ },
  { _, _, 0, 1, _, _, 2, 3, _, _, 4, 5, _, _, _, _ },
  { 0, 1, 2, 3, _, _, 4, 5, _, _, 6, 7, _, _, _, _ },
  { _, _, _, _, 0, 1, 2, 3, _, _, 4, 5, _, _, _, _ },
  { 0, 1, _, _, 2, 3, 4, 5, _, _, 6, 7, _, _, _, _ },
  { _, _, 0, 1, 2, 3, 4, 5, _, _, 6, 7, _, _, _, _ },
  { 0, 1, 2, 3, 4, 5, 6, 7, _, _, 8, 9, _, _, _, _ },
  { _, _, _, _, _, _, _, _, 0, 1, 2, 3, _, _, _, _ },
  { 0, 1, _, _, _, _, _, _, 2, 3, 4, 5, _, _, _, _ },
  { _, _, 0, 1, _, _, _, _, 2, 3, 4, 5, _, _, _, _ },
  { 0, 1, 2, 3, _, _, _, _, 4, 5, 6, 7, _, _, _, _ },
  { _, _, _, _, 0, 1, _, _, 2, 3, 4, 5, _, _, _, _ },
  { 0, 1, _, _, 2, 3, _, _, 4, 5, 6, 7, _, _, _, _ },
  { _, _, 0, 1, 2, 3, _, _, 4, 5, 6, 7, _, _, _, _ },
  { 0, 1, 2, 3, 4, 5, _, _, 6, 7, 8, 9, _, _, _, _ },
  { _, _, _, _, _, _, 0, 1, 2, 3, 4, 5, _, _, _, _ },
  { 0, 1, _, _, _, _, 2, 3, 4, 5, 6, 7, _, _, _, _ },
  { _, _, 0, 1, _, _, 2, 3, 4, 5, 6, 7, _, _, _, _ },
  { 0, 1, 2, 3, _, _, 4, 5, 6, 7, 8, 9, _, _, _, _ },
  { _, _, _, _, 0, 1, 2, 3, 4, 5, 6, 7, _, _, _, _ },
  { 0, 1, _, _, 2, 3, 4, 5, 6, 7, 8, 9, _, _, _, _ },
  { _, _, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, _, _, _, _ },
  { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9,10,11, _, _, _, _ },
  { _, _, _, _, _, _, _, _, _, _, _, _, 0, 1, _, _ },
  { 0, 1, _, _, _, _, _, _, _, _, _, _, 2, 3, _, _ },
  { _, _, 0, 1, _, _, _, _, _, _, _, _, 2, 3, _, _ },
  { 0, 1, 2, 3, _, _, _, _, _, _, _, _, 4, 5, _, _ },
  { _, _, _, _, 0, 1, _, _, _, _, _, _, 2, 3, _, _ },
  { 0, 1, _, _, 2, 3, _, _, _, _, _, _, 4, 5, _, _ },
  { _, _, 0, 1, 2, 3, _, _, _, _, _, _, 4, 5, _, _ },
  { 0, 1, 2, 3, 4, 5, _, _, _, _, _, _, 6, 7, _, _ },
  { _, _, _, _, _, _, 0, 1, _, _, _, _, 2, 3, _, _ },
  { 0, 1, _, _, _, _, 2, 3, _, _, _, _, 4, 5, _, _ },
  { _, _, 0, 1, _, _, 2, 3, _, _, _, _, 4, 5, _, _ },
  { 0, 1, 2, 3, _, _, 4, 5, _, _, _, _, 6, 7, _, _ },
  { _, _, _, _, 0, 1, 2, 3, _, _, _, _, 4, 5, _, _ },
  { 0, 1, _, _, 2, 3, 4, 5, _, _, _, _, 6, 7, _, _ },
  { _, _, 0, 1, 2, 3, 4, 5, _, _, _, _, 6, 7, _, _ },
  { 0, 1, 2, 3, 4, 5, 6, 7, _, _, _, _, 8, 9, _, _ },
  { _, _, _, _, _, _, _, _, 0, 1, _, _, 2, 3, _, _ },
  { 0, 1, _, _, _, _, _, _, 2, 3, _, _, 4, 5, _, _ },
  { _, _, 0, 1, _, _, _, _, 2, 3, _, _, 4, 5, _, _ },
  { 0, 1, 2, 3, _, _, _, _, 4, 5, _, _, 6, 7, _, _ },
  { _, _, _, _, 0, 1, _, _, 2, 3, _, _, 4, 5, _, _ },
  { 0, 1, _, _, 2, 3, _, _, 4, 5, _, _, 6, 7, _, _ },
  { _, _, 0, 1, 2, 3, _, _, 4, 5, _, _, 6, 7, _, _ },
  { 0, 1, 2, 3, 4, 5, _, _, 6, 7, _, _, 8, 9, _, _ },
  { _, _, _, _, _, _, 0, 1, 2, 3, _, _, 4, 5, _, _ },
  { 0, 1, _, _, _, _, 2, 3, 4, 5, _, _, 6, 7, _, _ },
  { _, _, 0, 1, _, _, 2, 3, 4, 5, _, _, 6, 7, _, _ },
  { 0, 1, 2, 3, _, _, 4, 5, 6, 7, _, _, 8, 9, _, _ },
  { _, _, _, _, 0, 1, 2, 3, 4, 5, _, _, 6, 7, _, _ },
  { 0, 1, _, _, 2, 3, 4, 5, 6, 7, _, _, 8, 9, _, _ },
  { _, _, 0, 1, 2, 3, 4, 5, 6, 7, _, _, 8, 9, _, _ },
  { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, _, _,10,11, _, _ },
  { _, _, _, _, _, _, _, _, _, _, 0, 1, 2, 3, _, _ },
  { 0, 1, _, _, _, _, _, _, _, _, 2, 3, 4, 5, _, _ },
  { _, _, 0, 1, _, _, _, _, _, _, 2, 3, 4, 5, _, _ },
  { 0, 1, 2, 3, _, _, _, _, _, _, 4, 5, 6, 7, _, _ },
  { _, _, _, _, 0, 1, _, _, _, _, 2, 3, 4, 5, _, _ },
  { 0, 1, _, _, 2, 3, _, _, _, _, 4, 5, 6, 7, _, _ },
  { _, _, 0, 1, 2, 3, _, _, _, _, 4, 5, 6, 7, _, _ },
  { 0, 1, 2, 3, 4, 5, _, _, _, _, 6, 7, 8, 9, _, _ },
  { _, _, _, _, _, _, 0, 1, _, _, 2, 3, 4, 5, _, _ },
  { 0, 1, _, _, _, _, 2, 3, _, _, 4, 5, 6, 7, _, _ },
  { _, _, 0, 1, _, _, 2, 3, _, _, 4, 5, 6, 7, _, _ },
  { 0, 1, 2, 3, _, _, 4, 5, _, _, 6, 7, 8, 9, _, _ },
  { _, _, _, _, 0, 1, 2, 3, _, _, 4, 5, 6, 7, _, _ },
  { 0, 1, _, _, 2, 3, 4, 5, _, _, 6, 7, 8, 9, _, _ },
  { _, _, 0, 1, 2, 3, 4, 5, _, _, 6, 7, 8, 9, _, _ },
  { 0, 1, 2, 3, 4, 5, 6, 7, _, _, 8, 9,10,11, _, _ },
  { _, _, _, _, _, _, _, _, 0, 1, 2, 3, 4, 5, _, _ },
  { 0, 1, _, _, _, _, _, _, 2, 3, 4, 5, 6, 7, _, _ },
  { _, _, 0, 1, _, _, _, _, 2, 3, 4, 5, 6, 7, _, _ },
  { 0, 1, 2, 3, _, _, _, _, 4, 5, 6, 7, 8, 9, _, _ },
  { _, _, _, _, 0, 1, _, _, 2, 3, 4, 5, 6, 7, _, _ },
  { 0, 1, _, _, 2, 3, _, _, 4, 5, 6, 7, 8, 9, _, _ },
  { _, _, 0, 1, 2, 3, _, _, 4, 5, 6, 7, 8, 9, _, _ },
  { 0, 1, 2, 3, 4, 5, _, _, 6, 7, 8, 9,10,11, _, _ },
  { _, _, _, _, _, _, 0, 1, 2, 3, 4, 5, 6, 7, _, _ },
  { 0, 1, _, _, _, _, 2, 3, 4, 5, 6, 7, 8, 9, _, _ },
  { _, _, 0, 1, _, _, 2, 3, 4, 5, 6, 7, 8, 9, _, _ },
  { 0, 1, 2, 3, _, _, 4, 5, 6, 7, 8, 9,10,11, _, _ },
  { _, _, _, _, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, _, _ },
  { 0, 1, _, _, 2, 3, 4, 5, 6, 7, 8, 9,10,11, _, _ },
  { _, _, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9,10,11, _, _ },
  { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9,10,11,12,13, _, _ },
  { _, _, _, _, _, _, _, _, _, _, _, _, _, _, 0, 1 },
  { 0, 1, _, _, _, _, _, _, _, _, _, _, _, _, 2, 3 },
  { _, _, 0, 1, _, _, _, _, _, _, _, _, _, _, 2, 3 },
  { 0, 1, 2, 3, _, _, _, _, _, _, _, _, _, _, 4, 5 },
  { _, _, _, _, 0, 1, _, _, _, _, _, _, _, _, 2, 3 },
  { 0, 1, _, _, 2, 3, _, _, _, _, _, _, _, _, 4, 5 },
  { _, _, 0, 1, 2, 3, _, _, _, _, _, _, _, _, 4, 5 },
  { 0, 1, 2, 3, 4, 5, _, _, _, _, _, _, _, _, 6, 7 },
  { _, _, _, _, _, _, 0, 1, _, _, _, _, _, _, 2, 3 },
  { 0, 1, _, _, _, _, 2, 3, _, _, _, _, _, _, 4, 5 },
  { _, _, 0, 1, _, _, 2, 3, _, _, _, _, _, _, 4, 5 },
  { 0, 1, 2, 3, _, _, 4, 5, _, _, _, _, _, _, 6, 7 },
  { _, _, _, _, 0, 1, 2, 3, _, _, _, _, _, _, 4, 5 },
  { 0, 1, _, _, 2, 3, 4, 5, _, _, _, _, _, _, 6, 7 },
  { _, _, 0, 1, 2, 3, 4, 5, _, _, _, _, _, _, 6, 7 },
  { 0, 1, 2, 3, 4, 5, 6, 7, _, _, _, _, _, _, 8, 9 },
  { _, _, _, _, _, _, _, _, 0, 1, _, _, _, _, 2, 3 },
  { 0, 1, _, _, _, _, _, _, 2, 3, _, _, _, _, 4, 5 },
  { _, _, 0, 1, _, _, _, _, 2, 3, _, _, _, _, 4, 5 },
  { 0, 1, 2, 3, _, _, _, _, 4, 5, _, _, _, _, 6, 7 },
  { _, _, _, _, 0, 1, _, _, 2, 3, _, _, _, _, 4, 5 },
  { 0, 1, _, _, 2, 3, _, _, 4, 5, _, _, _, _, 6, 7 },
  { _, _, 0, 1, 2, 3, _, _, 4, 5, _, _, _, _, 6, 7 },
  { 0, 1, 2, 3, 4, 5, _, _, 6, 7, _, _, _, _, 8, 9 },
  { _, _, _, _, _, _, 0, 1, 2, 3, _, _, _, _, 4, 5 },
  { 0, 1, _, _, _, _, 2, 3, 4, 5, _, _, _, _, 6, 7 },
  { _, _, 0, 1, _, _, 2, 3, 4, 5, _, _, _, _, 6, 7 },
  { 0, 1, 2, 3, _, _, 4, 5, 6, 7, _, _, _, _, 8, 9 },
  { _, _, _, _, 0, 1, 2, 3, 4, 5, _, _, _, _, 6, 7 },
  { 0, 1, _, _, 2, 3, 4, 5, 6, 7, _, _, _, _, 8, 9 },
  { _, _, 0, 1, 2, 3, 4, 5, 6, 7, _, _, _, _, 8, 9 },
  { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, _, _, _, _,10,11 },
  { _, _, _, _, _, _, _, _, _, _, 0, 1, _, _, 2, 3 },
  { 0, 1, _, _, _, _, _, _, _, _, 2, 3, _, _, 4, 5 },
  { _, _, 0, 1, _, _, _, _, _, _, 2, 3, _, _, 4, 5 },
  { 0, 1, 2, 3, _, _, _, _, _, _, 4, 5, _, _, 6, 7 },
  { _, _, _, _, 0, 1, _, _, _, _, 2, 3, _, _, 4, 5 },
  { 0, 1, _, _, 2, 3, _, _, _, _, 4, 5, _, _, 6, 7 },
  { _, _, 0, 1, 2, 3, _, _, _, _, 4, 5, _, _, 6, 7 },
  { 0, 1, 2, 3, 4, 5, _, _, _, _, 6, 7, _, _, 8, 9 },
  { _, _, _, _, _, _, 0, 1, _, _, 2, 3, _, _, 4, 5 },
  { 0, 1, _, _, _, _, 2, 3, _, _, 4, 5, _, _, 6, 7 },
  { _, _, 0, 1, _, _, 2, 3, _, _, 4, 5, _, _, 6, 7 },
  { 0, 1, 2, 3, _, _, 4, 5, _, _, 6, 7, _, _, 8, 9 },
  { _, _, _, _, 0, 1, 2, 3, _, _, 4, 5, _, _, 6, 7 },
  { 0, 1, _, _, 2, 3, 4, 5, _, _, 6, 7, _, _, 8, 9 },
  { _, _, 0, 1, 2, 3, 4, 5, _, _, 6, 7, _, _, 8, 9 },
  { 0, 1, 2, 3, 4, 5, 6, 7, _, _, 8, 9, _, _,10,11 },
  { _, _, _, _, _, _, _, _, 0, 1, 2, 3, _, _, 4, 5 },
  { 0, 1, _, _, _, _, _, _, 2, 3, 4, 5, _, _, 6, 7 },
  { _, _, 0, 1, _, _, _, _, 2, 3, 4, 5, _, _, 6, 7 },
  { 0, 1, 2, 3, _, _, _, _, 4, 5, 6, 7, _, _, 8, 9 },
  { _, _, _, _, 0, 1, _, _, 2, 3, 4, 5, _, _, 6, 7 },
  { 0, 1, _, _, 2, 3, _, _, 4, 5, 6, 7, _, _, 8, 9 },
  { _, _, 0, 1, 2, 3, _, _, 4, 5, 6, 7, _, _, 8, 9 },
  { 0, 1, 2, 3, 4, 5, _, _, 6, 7, 8, 9, _, _,10,11 },
  { _, _, _, _, _, _, 0, 1, 2, 3, 4, 5, _, _, 6, 7 },
  { 0, 1, _, _, _, _, 2, 3, 4, 5, 6, 7, _, _, 8, 9 },
  { _, _, 0, 1, _, _, 2, 3, 4, 5, 6, 7, _, _, 8, 9 },
  { 0, 1, 2, 3, _, _, 4, 5, 6, 7, 8, 9, _, _,10,11 },
  { _, _, _, _, 0, 1, 2, 3, 4, 5, 6, 7, _, _, 8, 9 },
  { 0, 1, _, _, 2, 3, 4, 5, 6, 7, 8, 9, _, _,10,11 },
  { _, _, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, _, _,10,11 },
  { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9,10,11, _, _,12,13 },
  { _, _, _, _, _, _, _, _, _, _, _, _, 0, 1, 2, 3 },
  { 0, 1, _, _, _, _, _, _, _, _, _, _, 2, 3, 4, 5 },
  { _, _, 0, 1, _, _, _, _, _, _, _, _, 2, 3, 4, 5 },
  { 0, 1, 2, 3, _, _, _, _, _, _, _, _, 4, 5, 6, 7 },
  { _, _, _, _, 0, 1, _, _, _, _, _, _, 2, 3, 4, 5 },
  { 0, 1, _, _, 2, 3, _, _, _, _, _, _, 4, 5, 6, 7 },
  { _, _, 0, 1, 2, 3, _, _, _, _, _, _, 4, 5, 6, 7 },
  { 0, 1, 2, 3, 4, 5, _, _, _, _, _, _, 6, 7, 8, 9 },
  { _, _, _, _, _, _, 0, 1, _, _, _, _, 2, 3, 4, 5 },
  { 0, 1, _, _, _, _, 2, 3, _, _, _, _, 4, 5, 6, 7 },
  { _, _, 0, 1, _, _, 2, 3, _, _, _, _, 4, 5, 6, 7 },
  { 0, 1, 2, 3, _, _, 4, 5, _, _, _, _, 6, 7, 8, 9 },
  { _, _, _, _, 0, 1, 2, 3, _, _, _, _, 4, 5, 6, 7 },
  { 0, 1, _, _, 2, 3, 4, 5, _, _, _, _, 6, 7, 8, 9 },
  { _, _, 0, 1, 2, 3, 4, 5, _, _, _, _, 6, 7, 8, 9 },
  { 0, 1, 2, 3, 4, 5, 6, 7, _, _, _, _, 8, 9,10,11 },
  { _, _, _, _, _, _, _, _, 0, 1, _, _, 2, 3, 4, 5 },
  { 0, 1, _, _, _, _, _, _, 2, 3, _, _, 4, 5, 6, 7 },
  { _, _, 0, 1, _, _, _, _, 2, 3, _, _, 4, 5, 6, 7 },
  { 0, 1, 2, 3, _, _, _, _, 4, 5, _, _, 6, 7, 8, 9 },
  { _, _, _, _, 0, 1, _, _, 2, 3, _, _, 4, 5, 6, 7 },
  { 0, 1, _, _, 2, 3, _, _, 4, 5, _, _, 6, 7, 8, 9 },
  { _, _, 0, 1, 2, 3, _, _, 4, 5, _, _, 6, 7, 8, 9 },
  { 0, 1, 2, 3, 4, 5, _, _, 6, 7, _, _, 8, 9,10,11 },
  { _, _, _, _, _, _, 0, 1, 2, 3, _, _, 4, 5, 6, 7 },
  { 0, 1, _, _, _, _, 2, 3, 4, 5, _, _, 6, 7, 8, 9 },
  { _, _, 0, 1, _, _, 2, 3, 4, 5, _, _, 6, 7, 8, 9 },
  { 0, 1, 2, 3, _, _, 4, 5, 6, 7, _, _, 8, 9,10,11 },
  { _, _, _, _, 0, 1, 2, 3, 4, 5, _, _, 6, 7, 8, 9 },
  { 0, 1, _, _, 2, 3, 4, 5, 6, 7, _, _, 8, 9,10,11 },
  { _, _, 0, 1, 2, 3, 4, 5, 6, 7, _, _, 8, 9,10,11 },
  { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, _, _,10,11,12,13 },
  { _, _, _, _, _, _, _, _, _, _, 0, 1, 2, 3, 4, 5 },
  { 0, 1, _, _, _, _, _, _, _, _, 2, 3, 4, 5, 6, 7 },
  { _, _, 0, 1, _, _, _, _, _, _, 2, 3, 4, 5, 6, 7 },
  { 0, 1, 2, 3, _, _, _, _, _, _, 4, 5, 6, 7, 8, 9 },
  { _, _, _, _, 0, 1, _, _, _, _, 2, 3, 4, 5, 6, 7 },
  { 0, 1, _, _, 2, 3, _, _, _, _, 4, 5, 6, 7, 8, 9 },
  { _, _, 0, 1, 2, 3, _, _, _, _, 4, 5, 6, 7, 8, 9 },
  { 0, 1, 2, 3, 4, 5, _, _, _, _, 6, 7, 8, 9,10,11 },
  { _, _, _, _, _, _, 0, 1, _, _, 2, 3, 4, 5, 6, 7 },
  { 0, 1, _, _, _, _, 2, 3, _, _, 4, 5, 6, 7, 8, 9 },
  { _, _, 0, 1, _, _, 2, 3, _, _, 4, 5, 6, 7, 8, 9 },
  { 0, 1, 2, 3, _, _, 4, 5, _, _, 6, 7, 8, 9,10,11 },
  { _, _, _, _, 0, 1, 2, 3, _, _, 4, 5, 6, 7, 8, 9 },
  { 0, 1, _, _, 2, 3, 4, 5, _, _, 6, 7, 8, 9,10,11 },
  { _, _, 0, 1, 2, 3, 4, 5, _, _, 6, 7, 8, 9,10,11 },
  { 0, 1, 2, 3, 4, 5, 6, 7, _, _, 8, 9,10,11,12,13 },
  { _, _, _, _, _, _, _, _, 0, 1, 2, 3, 4, 5, 6, 7 },
  { 0, 1, _, _, _, _, _, _, 2, 3, 4, 5, 6, 7, 8, 9 },
  { _, _, 0, 1, _, _, _, _, 2, 3, 4, 5, 6, 7, 8, 9 },
  { 0, 1, 2, 3, _, _, _, _, 4, 5, 6, 7, 8, 9,10,11 },
  { _, _, _, _, 0, 1, _, _, 2, 3, 4, 5, 6, 7, 8, 9 },
  { 0, 1, _, _, 2, 3, _, _, 4, 5, 6, 7, 8, 9,10,11 },
  { _, _, 0, 1, 2, 3, _, _, 4, 5, 6, 7, 8, 9,10,11 },
  { 0, 1, 2, 3, 4, 5, _, _, 6, 7, 8, 9,10,11,12,13 },
  { _, _, _, _, _, _, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 },
  { 0, 1, _, _, _, _, 2, 3, 4, 5, 6, 7, 8, 9,10,11 },
  { _, _, 0, 1, _, _, 2, 3, 4, 5, 6, 7, 8, 9,10,11 },
  { 0, 1, 2, 3, _, _, 4, 5, 6, 7, 8, 9,10,11,12,13 },
  { _, _, _, _, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9,10,11 },
  { 0, 1, _, _, 2, 3, 4, 5, 6, 7, 8, 9,10,11,12,13 },
  { _, _, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9,10,11,12,13 },
  { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9,10,11,12,13,14,15 },
};
  #undef _
    #endif // SSSE3

#define BITMAX16 15
#define BITMAX32 31

#define VO16( _op_, _i_, _ov_, _nb_,_parm_) m = *bb++;                                            _mm_storeu_si128(_op_++, _mm_add_epi16(_ov_, _mm_shuffle_epi8( mm_slli_epi16(_mm_loadu_si128((__m128i*)pex), _nb_), _mm_loadu_si128((__m128i*)_shuffle_16[m]) ) )); pex += popcnt32(m)
#define VO32( _op_, _i_, _ov_, _nb_,_parm_) if((_i_) & 1) m = (*bb++) >> 4; else m = (*bb) & 0xf; _mm_storeu_si128(_op_++, _mm_add_epi32(_ov_, _mm_shuffle_epi8( mm_slli_epi32(_mm_loadu_si128((__m128i*)pex), _nb_), _mm_loadu_si128((__m128i*)_shuffle_32[m]) ) )); pex += popcnt32(m)
#define VOZ16(_op_, _i_, _ov_, _nb_,_parm_) m = *bb++;                                            _mm_storeu_si128(_op_++,                     _mm_shuffle_epi8(               _mm_loadu_si128((__m128i*)pex),        _mm_loadu_si128((__m128i*)_shuffle_16[m]) ) );  pex += popcnt32(m)
#define VOZ32(_op_, _i_, _ov_, _nb_,_parm_) if((_i_) & 1) m = (*bb++) >> 4; else m = (*bb) & 0xf; _mm_storeu_si128(_op_++,                     _mm_shuffle_epi8(               _mm_loadu_si128((__m128i*)pex),        _mm_loadu_si128((__m128i*)_shuffle_32[m]) ) );  pex += popcnt32(m)
#define BITUNPACK0(_parm_) //_parm_ = _mm_setzero_si128()
#include "bitunpack_.h"

unsigned char *_bitunpack128v16( const unsigned char *__restrict in, unsigned n, unsigned short *__restrict out, unsigned b, unsigned short *__restrict pex, unsigned char *bb) {
  const unsigned char *ip = in+PAD8(128*b); unsigned m; __m128i sv; BITUNPACK128V16(in, b, out, sv); return (unsigned char *)ip;
}
unsigned char *_bitunpack128v32( const unsigned char *__restrict in, unsigned n, unsigned       *__restrict out, unsigned b, unsigned *__restrict pex, unsigned char *bb) {
  const unsigned char *ip = in+PAD8(128*b); unsigned m; __m128i sv; BITUNPACK128V32(in, b, out, sv); return (unsigned char *)ip;
}
unsigned char *_bitunpack256w32( const unsigned char *__restrict in, unsigned n, unsigned *__restrict out, unsigned b, unsigned *__restrict pex, unsigned char *bb) {
  const unsigned char *_in=in; unsigned *_out=out, m; __m128i sv;
  BITUNPACK128V32(in, b, out, sv); out = _out+128; in=_in+PAD8(128*b);
  BITUNPACK128V32(in, b, out, sv);
  return (unsigned char *)_in+PAD8(256*b);
}

//#define STOZ64(_op_, _ov_) _mm_storeu_si128(_op_++, _ov_); _mm_storeu_si128(_op_++, _ov_)
#define STO64( _op_, _ov_, _zv_) _mm_storeu_si128(_op_++, _mm_unpacklo_epi32(_ov_,_zv_));_mm_storeu_si128(_op_++, _mm_unpacklo_epi32(_mm_srli_si128(_ov_,8),_zv_))

#define VO32( _op_, _i_, _ov_, _nb_,_parm_) if((_i_) & 1) m = (*bb++) >> 4; else m = (*bb) & 0xf; { __m128i _wv = _mm_add_epi32(_ov_, _mm_shuffle_epi8( mm_slli_epi32(_mm_loadu_si128((__m128i*)pex), _nb_), _mm_loadu_si128((__m128i*)_shuffle_32[m]) ) ); STO64(_op_, _wv, zv);} pex += popcnt32(m)
#define VOZ32(_op_, _i_, _ov_, _nb_,_parm_) if((_i_) & 1) m = (*bb++) >> 4; else m = (*bb) & 0xf; { __m128i _wv =                     _mm_shuffle_epi8(               _mm_loadu_si128((__m128i*)pex),        _mm_loadu_si128((__m128i*)_shuffle_32[m]) ) ;  STO64(_op_, _wv, zv);} pex += popcnt32(m)
#define BITUNPACK0(_parm_)

#include "bitunpack_.h"
unsigned char *_bitunpack128v64( const unsigned char *__restrict in, unsigned n, uint64_t       *__restrict out, unsigned b, uint32_t *__restrict pex, unsigned char *bb) {
  const unsigned char *ip = in+PAD8(128*b); unsigned m; __m128i zv = _mm_setzero_si128(); BITUNPACK128V32(in, b, out, 0); return (unsigned char *)ip;
}

#define BITMAX16 16
#define BITMAX32 32

#undef VO32
#undef VOZ32
#undef VO16
#undef VOZ16
#undef BITUNPACK0

//-------------------------------------------------------------------
#define VOZ16(_op_, _i_, _ov_, _nb_,_parm_) _mm_storeu_si128(_op_++, _parm_)
#define VOZ32(_op_, _i_, _ov_, _nb_,_parm_) _mm_storeu_si128(_op_++, _parm_)
#define VO16( _op_, _i_, _ov_, _nb_,_sv_) _ov_ = mm_zzagd_epi16(_ov_); _sv_ = mm_scan_epi16(_ov_,_sv_); _mm_storeu_si128(_op_++, _sv_)
#define VO32( _op_, _i_, _ov_, _nb_,_sv_) _ov_ = mm_zzagd_epi32(_ov_); _sv_ = mm_scan_epi32(_ov_,_sv_); _mm_storeu_si128(_op_++, _sv_)
#include "bitunpack_.h"
#define BITUNPACK0(_parm_)
unsigned char *bitzunpack128v16( const unsigned char *__restrict in, unsigned n, unsigned short *__restrict out, unsigned short start, unsigned b) {
  const unsigned char *ip = in+PAD8(128*b); __m128i sv = _mm_set1_epi16(start); BITUNPACK128V16(in, b, out, sv); return (unsigned char *)ip;
}
unsigned char *bitzunpack128v32( const unsigned char *__restrict in, unsigned n, unsigned *__restrict out, unsigned start, unsigned b) {
  const unsigned char *ip = in+PAD8(128*b); __m128i sv = _mm_set1_epi32(start); BITUNPACK128V32(in, b, out, sv); return (unsigned char *)ip;
}

#define VO32(_op_, i, _ov_, _nb_,_sv_) _sv_ = mm_scan_epi32(_ov_,_sv_); _mm_storeu_si128(_op_++, _sv_)
#define VO16(_op_, i, _ov_, _nb_,_sv_) _sv_ = mm_scan_epi16(_ov_,_sv_); _mm_storeu_si128(_op_++, _sv_)
#include "bitunpack_.h"
#define BITUNPACK0(_parm_)
unsigned char *bitdunpack128v16( const unsigned char *__restrict in, unsigned n, uint16_t *__restrict out, uint16_t start, unsigned b) {
  const unsigned char *ip = in+PAD8(128*b); __m128i sv = _mm_set1_epi16(start); BITUNPACK128V16(in, b, out, sv); return (unsigned char *)ip;
}
unsigned char *bitdunpack128v32( const unsigned char *__restrict in, unsigned n, unsigned *__restrict out, unsigned start, unsigned b) {
  const unsigned char *ip = in+PAD8(128*b); __m128i sv = _mm_set1_epi32(start); BITUNPACK128V32(in, b, out, sv); return (unsigned char *)ip;
}

#define VO32( _op_, _i_, _ov_, _nb_,_parm_) _mm_storeu_si128(_op_++, _mm_add_epi32(_ov_, sv))
#define VO16( _op_, _i_, _ov_, _nb_,_parm_) _mm_storeu_si128(_op_++, _mm_add_epi16(_ov_, sv))
#include "bitunpack_.h"
#define BITUNPACK0(_parm_)
unsigned char *bitfunpack128v16( const unsigned char *__restrict in, unsigned n, uint16_t *__restrict out, uint16_t start, unsigned b) {
  const unsigned char *ip = in+PAD8(128*b); __m128i sv = _mm_set1_epi16(start); BITUNPACK128V16(in, b, out, sv); return (unsigned char *)ip;
}
unsigned char *bitfunpack128v32( const unsigned char *__restrict in, unsigned n, unsigned *__restrict out, unsigned start, unsigned b) {
  const unsigned char *ip = in+PAD8(128*b); __m128i sv = _mm_set1_epi32(start); BITUNPACK128V32(in, b, out, sv); return (unsigned char *)ip;
}

    #if defined(__SSSE3__) || defined(__ARM_NEON)
#define BITMAX16 15
#define BITMAX32 31

#define VX32(_i_, _nb_,_ov_)         if(!((_i_) & 1)) m = (*bb) & 0xf;else m = (*bb++) >> 4; _ov_ = _mm_add_epi32(_ov_, _mm_shuffle_epi8( mm_slli_epi32(_mm_loadu_si128((__m128i*)pex), _nb_), _mm_loadu_si128((__m128i*)_shuffle_32[m]))); pex += popcnt32(m)
#define VXZ32(_i_, _nb_,_ov_)        if(!((_i_) & 1)) m = (*bb) & 0xf;else m = (*bb++) >> 4; _ov_ =                     _mm_shuffle_epi8(               _mm_loadu_si128((__m128i*)pex),        _mm_loadu_si128((__m128i*)_shuffle_32[m]));  pex += popcnt32(m)
#define VO32( _op_, _i_, _ov_, _nb_,_sv_)   VX32( _i_, _nb_,_ov_); _sv_ = mm_scan_epi32(_ov_,_sv_); _mm_storeu_si128(_op_++, _sv_);
#define VOZ32(_op_, _i_, _ov_, _nb_,_sv_)   VXZ32(_i_, _nb_,_ov_); _sv_ = mm_scan_epi32(_ov_,_sv_); _mm_storeu_si128(_op_++, _sv_);

#define VX16(_i_, _nb_,_ov_)         m = *bb++; _ov_ = _mm_add_epi16(_ov_, _mm_shuffle_epi8( mm_slli_epi16(_mm_loadu_si128((__m128i*)pex), _nb_), _mm_loadu_si128((__m128i*)_shuffle_16[m]) ) ); pex += popcnt32(m)
#define VXZ16(_i_, _nb_,_ov_)        m = *bb++; _ov_ =                     _mm_shuffle_epi8(               _mm_loadu_si128((__m128i*)pex),        _mm_loadu_si128((__m128i*)_shuffle_16[m]) );   pex += popcnt32(m)
#define VO16( _op_, _i_, _ov_, _nb_,_sv_)   VX16(  _i_, _nb_,_ov_); _sv_ = mm_scan_epi16(_ov_,_sv_); _mm_storeu_si128(_op_++, _sv_);
#define VOZ16(_op_, _i_, _ov_, _nb_,_sv_)   VXZ16( _i_, _nb_,_ov_); _sv_ = mm_scan_epi16(_ov_,_sv_); _mm_storeu_si128(_op_++, _sv_);
#include "bitunpack_.h"
#define BITUNPACK0(_parm_)
unsigned char *_bitdunpack128v16( const unsigned char *__restrict in, unsigned n, unsigned short *__restrict out, unsigned short start, unsigned b, unsigned short *__restrict pex, unsigned char *bb) {
  const unsigned char *ip = in+PAD8(128*b); unsigned m; __m128i sv = _mm_set1_epi16(start); BITUNPACK128V16(in, b, out, sv); return (unsigned char *)ip;
}
unsigned char *_bitdunpack128v32( const unsigned char *__restrict in, unsigned n, unsigned       *__restrict out, unsigned       start, unsigned b, unsigned       *__restrict pex, unsigned char *bb) {
  const unsigned char *ip = in+PAD8(128*b); unsigned m; __m128i sv = _mm_set1_epi32(start); BITUNPACK128V32(in, b, out, sv); return (unsigned char *)ip;
}

/*
#define VO32( _op_, _i_, _ov_, _nb_,_sv_)   VX32( _i_, _ov_); mm_scan_epi32(_ov_,_sv_);   STO64( _op_, _sv_) //_mm_storeu_si128(_op_++, _sv_);
#define VOZ32(_op_, _i_, _ov_, _nb_,_sv_)   VXZ32( _i_, _ov_); mm_scan_epi32(_ov_,_sv_); STOZ64( _op_, _sv_, zv) //_mm_storeu_si128(_op_++, _sv_);
unsigned char *_bitdunpack128v64( const unsigned char *__restrict in, unsigned n, uint64_t       *__restrict out, uint64_t       start, unsigned b, uint64_t       *__restrict pex, unsigned char *bb) {
  const unsigned char *ip = in+PAD8(128*b); unsigned m; __m128i sv = _mm_set1_epi32(start),zv = _mm_setzero_si128(); BITUNPACK128V32(in, b, out, sv); return (unsigned char *)ip;
}*/

#define VX16(_i_, _nb_,_ov_)              m = *bb++; _ov_ = _mm_add_epi16(_ov_, _mm_shuffle_epi8( mm_slli_epi16(_mm_loadu_si128((__m128i*)pex), _nb_), _mm_loadu_si128((__m128i*)_shuffle_16[m]) ) ); pex += popcnt32(m)
#define VXZ16(_i_, _nb_,_ov_)             m = *bb++; _ov_ =                     _mm_shuffle_epi8(               _mm_loadu_si128((__m128i*)pex),        _mm_loadu_si128((__m128i*)_shuffle_16[m]) );   pex += popcnt32(m)
#define VO16( _op_, _i_, _ov_, _nb_,_sv_) VX16( _i_, _nb_,_ov_);  _ov_ = mm_zzagd_epi16(_ov_); _sv_ = mm_scan_epi16(_ov_,_sv_); _mm_storeu_si128(_op_++, _sv_);
#define VOZ16(_op_, _i_, _ov_, _nb_,_sv_) VXZ16( _i_, _nb_,_ov_); _ov_ = mm_zzagd_epi16(_ov_); _sv_ = mm_scan_epi16(_ov_,_sv_); _mm_storeu_si128(_op_++, _sv_);

#define VX32(_i_, _nb_,_ov_)              if(!((_i_) & 1)) m = (*bb) & 0xf;else m = (*bb++) >> 4; _ov_ = _mm_add_epi32(_ov_, _mm_shuffle_epi8( mm_slli_epi32(_mm_loadu_si128((__m128i*)pex), _nb_), _mm_loadu_si128((__m128i*)_shuffle_32[m]) ) ); pex += popcnt32(m)
#define VXZ32(_i_, _nb_,_ov_)             if(!((_i_) & 1)) m = (*bb) & 0xf;else m = (*bb++) >> 4; _ov_ =                     _mm_shuffle_epi8(               _mm_loadu_si128((__m128i*)pex),        _mm_loadu_si128((__m128i*)_shuffle_32[m]) ); pex += popcnt32(m)
#define VO32( _op_, _i_, _ov_, _nb_,_sv_) VX32( _i_, _nb_,_ov_); _ov_ = mm_zzagd_epi32(_ov_); _sv_ = mm_scan_epi32(_ov_,_sv_); _mm_storeu_si128(_op_++, _sv_);
#define VOZ32(_op_, _i_, _ov_, _nb_,_sv_) VXZ32(_i_, _nb_,_ov_); _ov_ = mm_zzagd_epi32(_ov_); _sv_ = mm_scan_epi32(_ov_,_sv_); _mm_storeu_si128(_op_++, _sv_);

#include "bitunpack_.h"
#define BITUNPACK0(_parm_)
unsigned char *_bitzunpack128v16( const unsigned char *__restrict in, unsigned n, unsigned short *__restrict out, unsigned short start, unsigned b, unsigned short *__restrict pex, unsigned char *bb) {
  const unsigned char *ip = in+PAD8(128*b); unsigned m; __m128i sv = _mm_set1_epi16(start); BITUNPACK128V16(in, b, out, sv); return (unsigned char *)ip;
}
unsigned char *_bitzunpack128v32( const unsigned char *__restrict in, unsigned n, unsigned *__restrict out, unsigned start, unsigned b, unsigned *__restrict pex, unsigned char *bb) {
  const unsigned char *ip = in+PAD8(128*b); unsigned m; __m128i sv = _mm_set1_epi32(start); BITUNPACK128V32(in, b, out, sv); return (unsigned char *)ip;
}
#define BITMAX16 16
#define BITMAX32 32
    #endif

#define VO16(_op_, i, _ov_, _nb_,_sv_) _sv_ = mm_scani_epi16(_ov_,_sv_,cv); _mm_storeu_si128(_op_++, _sv_);
#define VO32(_op_, i, _ov_, _nb_,_sv_) _sv_ = mm_scani_epi32(_ov_,_sv_,cv); _mm_storeu_si128(_op_++, _sv_);
#define VOZ16(_op_, _i_, ov, _nb_,_parm_) _mm_storeu_si128(_op_++, _parm_); _parm_ = _mm_add_epi16(_parm_, cv)
#define VOZ32(_op_, _i_, ov, _nb_,_parm_) _mm_storeu_si128(_op_++, _parm_); _parm_ = _mm_add_epi32(_parm_, cv)
#include "bitunpack_.h"
#define BITUNPACK0(_parm_) _parm_ = _mm_add_epi16(_parm_, cv); cv = _mm_set1_epi16(8)
unsigned char *bitd1unpack128v16( const unsigned char *__restrict in, unsigned n, uint16_t *__restrict out, uint16_t start, unsigned b) {
  const unsigned char *ip = in+PAD8(128*b); __m128i sv = _mm_set1_epi16(start), cv = _mm_set_epi16(8,7,6,5,4,3,2,1); BITUNPACK128V16(in, b, out, sv); return (unsigned char *)ip;
}
#define BITUNPACK0(_parm_) _parm_ = _mm_add_epi32(_parm_, cv); cv = _mm_set1_epi32(4)
unsigned char *bitd1unpack128v32( const unsigned char *__restrict in, unsigned n, unsigned *__restrict out, unsigned start, unsigned b) {
  const unsigned char *ip = in+PAD8(128*b); __m128i sv = _mm_set1_epi32(start), cv = _mm_set_epi32(4,3,2,1); BITUNPACK128V32(in, b, out, sv); return (unsigned char *)ip;
}

#define VO16(_op_, i, _ov_, _nb_,_sv_) ADDI16x8(_ov_,_sv_,cv); _mm_storeu_si128(_op_++, _sv_);
#define VO32(_op_, i, _ov_, _nb_,_sv_) ADDI32x4(_ov_,_sv_,cv); _mm_storeu_si128(_op_++, _sv_);
#define VOZ16(_op_, _i_, ov, _nb_,_parm_) _mm_storeu_si128(_op_++, _parm_); _parm_ = _mm_add_epi16(_parm_, cv)
#define VOZ32(_op_, _i_, ov, _nb_,_parm_) _mm_storeu_si128(_op_++, _parm_); _parm_ = _mm_add_epi32(_parm_, cv)
#include "bitunpack_.h"
#define BITUNPACK0(_parm_) _parm_ = _mm_add_epi16(_parm_, cv); cv = _mm_set1_epi16(8)
unsigned char *bits1unpack128v16( const unsigned char *__restrict in, unsigned n, uint16_t *__restrict out, uint16_t start, unsigned b) {
  const unsigned char *ip = in+PAD8(128*b); __m128i sv = _mm_set1_epi16(start), cv = _mm_set1_epi16(8); BITUNPACK128V16(in, b, out, sv); return (unsigned char *)ip;
}
#define BITUNPACK0(_parm_) _parm_ = _mm_add_epi32(_parm_, cv); cv = _mm_set1_epi32(4)
unsigned char *bits1unpack128v32( const unsigned char *__restrict in, unsigned n, unsigned *__restrict out, unsigned start, unsigned b) {
  const unsigned char *ip = in+PAD8(128*b); __m128i sv = _mm_set1_epi32(start), cv = _mm_set1_epi32(4); BITUNPACK128V32(in, b, out, sv); return (unsigned char *)ip;
}

#define VO16( _op_, _i_, _ov_, _nb_,_sv_) _mm_storeu_si128(_op_++, _mm_add_epi16(_ov_, _sv_)); _sv_ = _mm_add_epi16(_sv_, cv)
#define VO32( _op_, _i_, _ov_, _nb_,_sv_) _mm_storeu_si128(_op_++, _mm_add_epi32(_ov_, _sv_)); _sv_ = _mm_add_epi32(_sv_, cv)
#define VOZ32(_op_, _i_, _ov_, _nb_,_sv_) _mm_storeu_si128(_op_++, _sv_);                      _sv_ = _mm_add_epi32(_sv_, cv);
#include "bitunpack_.h"
#define BITUNPACK0(_parm_)
unsigned char *bitf1unpack128v16( const unsigned char *__restrict in, unsigned n, unsigned short *__restrict out, unsigned short start, unsigned b) {
  const unsigned char *ip = in+PAD8(128*b); __m128i sv = _mm_set_epi16(start+8,start+7,start+6,start+5,start+4,start+3,start+2,start+1), cv = _mm_set1_epi16(8); BITUNPACK128V16(in, b, out, sv); return (unsigned char *)ip;
}
unsigned char *bitf1unpack128v32( const unsigned char *__restrict in, unsigned n, unsigned *__restrict out, unsigned start, unsigned b) {
  const unsigned char *ip = in+PAD8(128*b); __m128i sv = _mm_set_epi32(start+4,start+3,start+2,start+1),                                 cv = _mm_set1_epi32(4); BITUNPACK128V32(in, b, out, sv); return (unsigned char *)ip;
}

    #if defined(__SSSE3__) || defined(__ARM_NEON)
#define BITMAX16 15
#define BITMAX32 31

#define VX16(_i_, _nb_,_ov_)                                                    m =  *bb++;       _ov_ = _mm_add_epi16(_ov_, _mm_shuffle_epi8( mm_slli_epi16(_mm_loadu_si128((__m128i*)pex), _nb_), _mm_loadu_si128((__m128i*)_shuffle_16[m]))); pex += popcnt32(m)
#define VX32(_i_, _nb_,_ov_)              if(!((_i_) & 1)) m = (*bb) & 0xf;else m = (*bb++) >> 4; _ov_ = _mm_add_epi32(_ov_, _mm_shuffle_epi8( mm_slli_epi32(_mm_loadu_si128((__m128i*)pex), _nb_), _mm_loadu_si128((__m128i*)_shuffle_32[m]))); pex += popcnt32(m)
#define VXZ16(_i_, _nb_,_ov_)                                                   m =  *bb++;       _ov_ =                     _mm_shuffle_epi8(               _mm_loadu_si128((__m128i*)pex),        _mm_loadu_si128((__m128i*)_shuffle_16[m]));  pex += popcnt32(m)
#define VXZ32(_i_, _nb_,_ov_)             if(!((_i_) & 1)) m = (*bb) & 0xf;else m = (*bb++) >> 4; _ov_ =                     _mm_shuffle_epi8(               _mm_loadu_si128((__m128i*)pex),        _mm_loadu_si128((__m128i*)_shuffle_32[m]));  pex += popcnt32(m)

#define VO16( _op_, _i_, _ov_, _nb_,_sv_) VX16( _i_, _nb_,_ov_);  _sv_ = mm_scani_epi16(_ov_,_sv_,cv); _mm_storeu_si128(_op_++, _sv_);
#define VOZ16(_op_, _i_, _ov_, _nb_,_sv_) VXZ16( _i_, _nb_,_ov_); _sv_ = mm_scani_epi16(_ov_,_sv_,cv); _mm_storeu_si128(_op_++, _sv_);
#define VO32( _op_, _i_, _ov_, _nb_,_sv_) VX32( _i_, _nb_,_ov_);  _sv_ = mm_scani_epi32(_ov_,_sv_,cv); _mm_storeu_si128(_op_++, _sv_);
#define VOZ32(_op_, _i_, _ov_, _nb_,_sv_) VXZ32( _i_, _nb_,_ov_); _sv_ = mm_scani_epi32(_ov_,_sv_,cv); _mm_storeu_si128(_op_++, _sv_);

#include "bitunpack_.h"
#define BITUNPACK0(_parm_) mv = _mm_setzero_si128() //_parm_ = _mm_setzero_si128()
unsigned char *_bitd1unpack128v16( const unsigned char *__restrict in, unsigned n, unsigned short *__restrict out, unsigned short start, unsigned b, unsigned short *__restrict pex, unsigned char *bb) {
  const unsigned char *ip = in+PAD8(128*b); unsigned m; __m128i sv = _mm_set1_epi16(start), cv = _mm_set_epi16(8,7,6,5,4,3,2,1); BITUNPACK128V16(in, b, out, sv); return (unsigned char *)ip;
}
#define BITUNPACK0(_parm_) mv = _mm_setzero_si128()
unsigned char *_bitd1unpack128v32( const unsigned char *__restrict in, unsigned n, unsigned       *__restrict out, unsigned       start, unsigned b, unsigned       *__restrict pex, unsigned char *bb) {
  const unsigned char *ip = in+PAD8(128*b); unsigned m; __m128i sv = _mm_set1_epi32(start), cv = _mm_set_epi32(        4,3,2,1); BITUNPACK128V32(in, b, out, sv); return (unsigned char *)ip;
}

#define VO16( _op_, _i_, _ov_, _nb_,_sv_) VX16( _i_, _nb_,_ov_);  ADDI16x8(_ov_,_sv_,cv); _mm_storeu_si128(_op_++, _sv_);
#define VOZ16(_op_, _i_, _ov_, _nb_,_sv_) VXZ16( _i_, _nb_,_ov_); ADDI16x8(_ov_,_sv_,cv); _mm_storeu_si128(_op_++, _sv_);
#define VO32( _op_, _i_, _ov_, _nb_,_sv_) VX32( _i_, _nb_,_ov_);  ADDI32x4(_ov_,_sv_,cv); _mm_storeu_si128(_op_++, _sv_);
#define VOZ32(_op_, _i_, _ov_, _nb_,_sv_) VXZ32( _i_, _nb_,_ov_); ADDI32x4(_ov_,_sv_,cv); _mm_storeu_si128(_op_++, _sv_);

#include "bitunpack_.h"
#define BITUNPACK0(_parm_) mv = _mm_setzero_si128() //_parm_ = _mm_setzero_si128()
unsigned char *_bits1unpack128v16( const unsigned char *__restrict in, unsigned n, unsigned short *__restrict out, unsigned short start, unsigned b, unsigned short *__restrict pex, unsigned char *bb) {
  const unsigned char *ip = in+PAD8(128*b); unsigned m; __m128i sv = _mm_set1_epi16(start), cv = _mm_set1_epi16(8); BITUNPACK128V16(in, b, out, sv); return (unsigned char *)ip;
}
#define BITUNPACK0(_parm_) mv = _mm_setzero_si128()
unsigned char *_bits1unpack128v32( const unsigned char *__restrict in, unsigned n, unsigned       *__restrict out, unsigned       start, unsigned b, unsigned       *__restrict pex, unsigned char *bb) {
  const unsigned char *ip = in+PAD8(128*b); unsigned m; __m128i sv = _mm_set1_epi32(start), cv = _mm_set1_epi32(4); BITUNPACK128V32(in, b, out, sv); return (unsigned char *)ip;
}
#define BITMAX16 16
#define BITMAX32 32
    #endif

size_t bitnunpack128v16(  unsigned char *__restrict in, size_t n, uint16_t *__restrict out) { uint16_t *op;       _BITNUNPACKV( in, n, out, 128, 16, bitunpack128v); }
size_t bitnunpack128v32(  unsigned char *__restrict in, size_t n, uint32_t *__restrict out) { uint32_t *op;       _BITNUNPACKV( in, n, out, 128, 32, bitunpack128v); }
size_t bitnunpack128v64(  unsigned char *__restrict in, size_t n, uint64_t *__restrict out) { uint64_t *op;       _BITNUNPACKV( in, n, out, 128, 64, bitunpack128v); }
size_t bitnunpack256w32(  unsigned char *__restrict in, size_t n, uint32_t *__restrict out) { uint32_t *op;       _BITNUNPACKV( in, n, out, 256, 32, bitunpack256w); }

size_t bitndunpack128v16( unsigned char *__restrict in, size_t n, uint16_t *__restrict out) { uint16_t *op,start; _BITNDUNPACKV(in, n, out, 128, 16, bitdunpack128v, bitdunpack); }
size_t bitndunpack128v32( unsigned char *__restrict in, size_t n, uint32_t *__restrict out) { uint32_t *op,start; _BITNDUNPACKV(in, n, out, 128, 32, bitdunpack128v, bitdunpack); }

size_t bitnd1unpack128v16(unsigned char *__restrict in, size_t n, uint16_t *__restrict out) { uint16_t *op,start; _BITNDUNPACKV(in, n, out, 128, 16, bitd1unpack128v, bitd1unpack); }
size_t bitnd1unpack128v32(unsigned char *__restrict in, size_t n, uint32_t *__restrict out) { uint32_t *op,start; _BITNDUNPACKV(in, n, out, 128, 32, bitd1unpack128v, bitd1unpack); }

size_t bitns1unpack128v16(unsigned char *__restrict in, size_t n, uint16_t *__restrict out) { uint16_t *op,start; _BITNDUNPACKV(in, n, out, 128, 16, bits1unpack128v, bitd1unpack); }
size_t bitns1unpack128v32(unsigned char *__restrict in, size_t n, uint32_t *__restrict out) { uint32_t *op,start; _BITNDUNPACKV(in, n, out, 128, 32, bits1unpack128v, bitd1unpack); }

size_t bitnzunpack128v16( unsigned char *__restrict in, size_t n, uint16_t *__restrict out) { uint16_t *op,start; _BITNDUNPACKV(in, n, out, 128, 16, bitzunpack128v, bitzunpack); }
size_t bitnzunpack128v32( unsigned char *__restrict in, size_t n, uint32_t *__restrict out) { uint32_t *op,start; _BITNDUNPACKV(in, n, out, 128, 32, bitzunpack128v, bitzunpack); }

size_t bitnfunpack128v16( unsigned char *__restrict in, size_t n, uint16_t *__restrict out) { uint16_t *op,start; _BITNDUNPACKV(in, n, out, 128, 16, bitfunpack128v, bitfunpack); }
size_t bitnfunpack128v32( unsigned char *__restrict in, size_t n, uint32_t *__restrict out) { uint32_t *op,start; _BITNDUNPACKV(in, n, out, 128, 32, bitfunpack128v, bitfunpack); }

#endif
#endif

#pragma clang diagnostic pop
#pragma GCC pop_options
