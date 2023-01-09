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

//    "Integer Compression" header for idxcr/idxqry
#include <stdint.h>

#define BLK_DIDNUM (128+1)  //  Block size 128 + 1 (1 stored in skips)

                            //              compressed size for 62 GB clueweb09.sorted
                            // Defaut is bitpackv/bitunpackv    18 GB
//#define _TURBOPFOR        // for compact version              12 GB

  #ifdef _TURBOPFOR
//#define SKIP_S 6
#define SKIP_SIZE 2         // always no implicit skip.
  #else
//#define SKIP_S 5
#define SKIP_SIZE 2         // no implicit skips
//#define SKIP_SIZE 1       //    implicit skips
  #endif
#define SKIP_M ((1<<SKIP_S)-1)

  #ifdef _WIN32
#define __off64_t _off64_t
  #endif
//-------------------------- Mapping term id <-> posting offset in file ----------------------------------
typedef struct { uint8_t offseth; uint32_t offsetl; } __attribute__ ((packed)) tmap_t;   // 40 bits offsets -> 1 Terabyte

#define TIDMAPSET(_t_, _ofs_) { (_t_)->offseth = (_ofs_)>>32; (_t_)->offsetl = (_ofs_) & 0xffffffff; }
//#define TIDMAPGET(_t_) ((__off64_t)(_t_)->offseth << 32 | (_t_)->offsetl)
#define TIDMAPGET(_t_) ((unsigned long long)(_t_)->offseth << 32 | (_t_)->offsetl)
#define TIDMAP(_fdm_, _tid_) ({ unsigned char *_bp = _fdm_; tmap_t *_t = (tmap_t *)&_bp[(_tid_)*sizeof(tmap_t)]; TIDMAPGET(_t); })
