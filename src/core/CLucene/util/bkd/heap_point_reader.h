#pragma once

#include "point_reader.h"
#include <memory>
#include <vector>

CL_NS_DEF(util)

namespace bkd {
    //TODO:make vectorptr to unique_ptr
    //typedef std::vector<std::shared_ptr<std::vector<uint8_t>>> ByteArrayList;
    typedef std::vector<std::vector<uint8_t>> ByteArrayList;
    //using vectorptr = std::shared_ptr<std::vector<uint8_t>>;

    class heap_point_reader final : public point_reader {
    public:
        heap_point_reader(ByteArrayList *blocks,
                        int32_t valuesPerBlock,
                        int32_t packedBytesLength,
                        const std::vector<int32_t> &ords,
                        const std::vector<int64_t> &ordsLong,
                        std::vector<int32_t> *docIDs,
                        int32_t start,
                        int32_t end,
                        bool singleValuePerDoc);

        //void write_packed_value(int32_t index, const std::vector<uint8_t> &bytes);
        void read_packed_value(int32_t index, std::vector<uint8_t> &bytes);
        bool next() override;
        const std::vector<uint8_t> &packed_value() override;
        uint8_t* packed_value_raw() override;
        int32_t docId() override;
        int64_t ord() override;

    public:
        bool single_value_per_doc_;
        int32_t values_per_block_;
        int32_t packed_bytes_length_;
        int32_t end_;
        ByteArrayList* blocks_{};
        std::vector<int64_t> ords_long_;
        std::vector<int32_t> ords_;
        std::vector<int32_t> *doc_ids_;
        std::vector<uint8_t> scratch_;

    private:
        int32_t cur_read_ = 0;
    };

}// namespace bkd
CL_NS_END