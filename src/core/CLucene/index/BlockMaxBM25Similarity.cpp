//
// BlockMaxBM25Similarity - 用于 Block Max Score 计算的 BM25 相似度类实现
//
// 与 doris::segment_v2::BM25Similarity 保持一致的实现方式。
//

#include "BlockMaxBM25Similarity.h"

#include <cstddef>
#include <limits>

namespace lucene::index {

const int32_t BlockMaxBM25Similarity::MAX_INT32 = std::numeric_limits<int32_t>::max();
const uint32_t BlockMaxBM25Similarity::MAX_INT4 = long_to_int4(static_cast<uint64_t>(MAX_INT32));
const int32_t BlockMaxBM25Similarity::NUM_FREE_VALUES = 255 - static_cast<int>(MAX_INT4);

std::vector<float> BlockMaxBM25Similarity::LENGTH_TABLE = []() {
    std::vector<float> table(256);
    for (int32_t i = 0; i < 256; i++) {
        table[i] = byte4_to_int(static_cast<uint8_t>(i));
    }
    return table;
}();

BlockMaxBM25Similarity::BlockMaxBM25Similarity(float avgdl) : _avgdl(avgdl), _cache(256) {
    compute_cache();
}

void BlockMaxBM25Similarity::compute_cache() {
    for (size_t i = 0; i < 256; i++) {
        _cache[i] = 1.0F / (_k1 * ((1 - _b) + _b * LENGTH_TABLE[i] / _avgdl));
    }
}

int32_t BlockMaxBM25Similarity::number_of_leading_zeros(uint64_t value) {
    if (value == 0) {
        return 64;
    }
    return __builtin_clzll(value);
}

uint32_t BlockMaxBM25Similarity::long_to_int4(uint64_t i) {
    if (i > std::numeric_limits<uint64_t>::max()) {
        throw;
    }

    int32_t numBits = 64 - number_of_leading_zeros(i);
    if (numBits < 4) {
        return static_cast<uint32_t>(i);
    } else {
        int32_t shift = numBits - 4;
        uint32_t encoded = static_cast<uint32_t>(i >> shift) & 0x07;
        return encoded | ((shift + 1) << 3);
    }
}

uint64_t BlockMaxBM25Similarity::int4_to_long(uint32_t i) {
    uint64_t bits = i & 0x07;
    int32_t shift = (i >> 3) - 1;

    if (shift < 0) {
        return bits;
    } else {
        return (bits | 0x08) << shift;
    }
}

float BlockMaxBM25Similarity::byte4_to_int(uint8_t b) {
    if (b < NUM_FREE_VALUES) {
        return static_cast<float>(b);
    } else {
        uint64_t decoded = NUM_FREE_VALUES + int4_to_long(b - NUM_FREE_VALUES);
        return static_cast<float>(decoded);
    }
}

} // namespace lucene::index
