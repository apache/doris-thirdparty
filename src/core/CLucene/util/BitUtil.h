#pragma once
#include "CLucene/SharedHeader.h"

#include <vector>

CL_NS_DEF(util)
    class BitUtil
    {

        // magic numbers for bit interleaving
    private:
        static std::vector<int64_t> const MAGIC;
        // shift values for bit interleaving
        static std::vector<short> const SHIFT;

        BitUtil();

        // The pop methods used to rely on bit-manipulation tricks for speed but it
        // turns out that it is faster to use the Long.bitCount method (which is an
        // intrinsic since Java 6u18) in a naive loop, see LUCENE-2221

        /** Returns the number of set bits in an array of longs. */
    public:
        static int64_t pop_array(std::vector<int64_t> &arr, int wordOffset,
                                 int numWords);
    };

CL_NS_END