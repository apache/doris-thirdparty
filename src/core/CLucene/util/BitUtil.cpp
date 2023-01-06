#include "BitUtil.h"

#include <bitset>
CL_NS_DEF(util)

std::vector<int64_t> const BitUtil::MAGIC = {
        0x5555555555555555LL, 0x3333333333333333LL, 0x0F0F0F0F0F0F0F0FLL,
        0x00FF00FF00FF00FFLL, 0x0000FFFF0000FFFFLL, 0x00000000FFFFFFFFLL,
        0xAAAAAAAAAAAAAAAALL};
std::vector<short> const BitUtil::SHIFT = {1, 2, 4, 8, 16};

BitUtil::BitUtil() = default; // no instance

int64_t BitUtil::pop_array(std::vector<int64_t> &arr, int wordOffset,
                           int numWords)
{
    int64_t popCount = 0;
    for (int i = wordOffset, end = wordOffset + numWords; i < end; ++i) {
        std::bitset<64> bit_count{arr[i]};
        popCount += bit_count.count();
    }
    return popCount;
}
CL_NS_END