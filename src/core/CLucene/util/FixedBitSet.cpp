#include "FixedBitSet.h"
#include "BitUtil.h"

CL_NS_DEF(util)
FixedBitSet::FixedBitSet(int nb)
    : numBits(nb),bits(std::vector<int64_t>(bits2words(nb))),
      numWords(bits.size()) {
}

int FixedBitSet::bits2words(int numBits) {
    return ((numBits - 1) >> 6) + 1;// I.e.: get the word-offset of the last bit and add one (make sure
                                    // to use >> so 0 returns 0!)
}

int FixedBitSet::Cardinality()
{
    // Depends on the ghost bits being clear!
    return static_cast<int>(BitUtil::pop_array(bits, 0, numWords));
}

bool FixedBitSet::Get(int index)
{
    int i = index >> 6; // div 64
    // signed shift will keep a negative index and force an
    // array-index-out-of-bounds-exception, removing the need for an explicit
    // check.
    int64_t bitmask = 1LL << index;
    return (bits[i] & bitmask) != 0;
}

void FixedBitSet::Set(int index)
{
    int wordNum = index >> 6; // div 64
    int64_t bitmask = 1LL << index;
    bits[wordNum] |= bitmask;
}
CL_NS_END