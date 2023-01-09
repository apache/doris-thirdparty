#include "LongBitSet.h"
#include "BitUtil.h"

CL_NS_DEF(util)
LongBitSet::LongBitSet(int64_t nb)
    : bits(std::vector<int64_t>(bits2words(nb))), numBits(nb),
      numWords(bits.size()) {
}

int LongBitSet::bits2words(int64_t numBits) {
    return static_cast<int>((numBits - 1) >> 6) + 1;
}

void LongBitSet::Clear(int64_t index)
{
    int wordNum = static_cast<int>(index >> 6);
    int64_t bitmask = 1LL << index;
    bits[wordNum] &= ~bitmask;
}

int64_t LongBitSet::Cardinality() {
    return BitUtil::pop_array(bits, 0, numWords);
}

bool LongBitSet::Get(int64_t index) {
    int i = static_cast<int>(index >> 6);// div 64

    // signed shift will keep a negative index and force an
    // array-index-out-of-bounds-exception, removing the need for an explicit
    // check.

    int64_t bitmask = 1LL << index;
    return (bits[i] & bitmask) != 0;
}

void LongBitSet::Set(int64_t index) {
    int wordNum = static_cast<int>(index >> 6);// div 64
    int64_t bitmask = 1LL << index;
    bits[wordNum] |= bitmask;
}
CL_NS_END