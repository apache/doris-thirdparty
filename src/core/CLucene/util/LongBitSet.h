#pragma once

#include "CLucene/SharedHeader.h"

#include <vector>

CL_NS_DEF(util)
class LongBitSet {
public:
    explicit LongBitSet(int64_t numBits);

private:
    std::vector<int64_t> bits;// Array of longs holding the bits
    const int64_t numBits;        // The number of bits in use
    const int numWords;       // The exact number of longs needed to hold numBits (<=
                              // bits.length)
public:
    void Set(int64_t index);
    bool Get(int64_t index);
    int64_t Cardinality();
    void Clear(int64_t index);
    static int bits2words(int64_t numBits);
};

CL_NS_END