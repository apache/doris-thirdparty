#pragma once

#include <cstdint>
#include <vector>

namespace lucene::index {

class BlockMaxBM25Similarity {
public:
    explicit BlockMaxBM25Similarity(float avgdl);
    ~BlockMaxBM25Similarity() = default;

    float tf_factor(int32_t freq, uint8_t norm_byte) const {
        float norm_inverse = _cache[norm_byte];
        return 1.0F - 1.0F / (1.0F + static_cast<float>(freq) * norm_inverse);
    }

    float avgdl() const { return _avgdl; }

private:
    void compute_cache();

    static int32_t number_of_leading_zeros(uint64_t value);
    static uint32_t long_to_int4(uint64_t i);
    static uint64_t int4_to_long(uint32_t i);
    static float byte4_to_int(uint8_t b);

    static const int32_t MAX_INT32;
    static const uint32_t MAX_INT4;
    static const int32_t NUM_FREE_VALUES;

    static std::vector<float> LENGTH_TABLE;

    float _k1 = 1.2F;
    float _b = 0.75F;
    float _avgdl = 0.0F;

    std::vector<float> _cache;
};

} // namespace lucene::index