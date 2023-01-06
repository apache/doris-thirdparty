#pragma once

#include "CLucene/SharedHeader.h"

#include <vector>

CL_NS_DEF(util)
class FixedBitSet {
public:
    explicit FixedBitSet(int numBits);

private:
    std::vector<int64_t> bits;// Array of longs holding the bits
    const int numBits;        // The number of bits in use
    const int numWords;       // The exact number of longs needed to hold numBits (<=
                              // bits.length)
public:
    void Set(int index);
    bool Get(int index);
    int Cardinality();
    static int bits2words(int numBits);
};

CL_NS_END