#pragma once

#include <vector>
#include <cstdint>
#include <algorithm>
#include <cassert>

#include "CLucene/util/BytesRef.h"

#include "heap_point_reader.h"
#include "point_writer.h"

CL_NS_DEF(util)

namespace bkd
{
    class heap_point_writer : public point_writer {
    
    public:
        std::vector<int32_t> doc_IDs_;
        std::vector<int64_t> ords_long_;
        std::vector<int32_t> ords_;
        uint32_t next_write_;
        bool closed_;
        int32_t max_size_;
        
        uint32_t packed_bytes_length_;
        const int32_t values_per_block_;
        const bool single_value_per_doc_;
        ByteArrayList blocks_;
        std::shared_ptr<BytesRef> cache_;
        std::shared_ptr<BytesRef> cache1_;
    public:
        heap_point_writer(int32_t initSize, int32_t maxSize, uint32_t packedBytesLength,
                bool longOrds, bool singleValuePerDoc);
        void copy_from(heap_point_writer &other);
        inline void copy_from(std::shared_ptr<heap_point_writer> &other){ copy_from(*other);};
        void read_packed_value(int32_t index, std::vector<uint8_t> &bytes);
        std::shared_ptr<BytesRef> read_packed_value(int32_t index);
        std::shared_ptr<BytesRef> read_packed_value2(int32_t index);
        void get_packed_value_slice(int32_t index, BytesRef& result);
        void write_packed_value(int32_t index, std::shared_ptr<std::vector<uint8_t>> &bytes){ write_packed_value(index, *bytes);};
        void write_packed_value(int32_t index, const std::vector<uint8_t> &bytes);
        void write_packed_value(int32_t index, const uint8_t* bytes, uint32_t length);
        std::vector<int> compute_cardinality(int from, int to, int num_dims, int bytes_per_dim, std::vector<int32_t> &common_prefix_lengths);
        void append(std::shared_ptr<std::vector<uint8_t>> &packedValue, int64_t ord, int32_t docid);
        void append(const uint8_t* packedValue, uint32_t value_length, int64_t ord, int32_t docid) override;
        void append(const std::vector<uint8_t> &packedValue, int64_t ord, int32_t docid) override;
        std::shared_ptr<point_reader> get_reader(int64_t start, int64_t length) override;
        std::shared_ptr<point_reader> get_shared_reader(int64_t start, int64_t length, const std::vector<std::shared_ptr<point_reader>> &toCloseHeroically) override;
        void close() override;
        void destroy() override;
        std::string to_string();

        ~heap_point_writer() override;
    };
} // namespace bkd
CL_NS_END
