#include "heap_point_reader.h"

CL_NS_DEF(util)
namespace bkd {

    heap_point_reader::heap_point_reader(ByteArrayList* blocks,
                                     int32_t valuesPerBlock,
                                     int32_t packedBytesLength,
                                     const std::vector<int32_t> &ords,
                                     const std::vector<int64_t> &ordsLong,
                                     std::vector<int32_t> *docids,
                                     int32_t start,
                                     int32_t end,
                                     bool singleValuePerDoc) {
        blocks_ = blocks;
        values_per_block_ = valuesPerBlock;
        single_value_per_doc_ = singleValuePerDoc;
        ords_ = ords;
        ords_long_ = ordsLong;
        doc_ids_ = docids;
        cur_read_ = start - 1;
        end_ = end;
        packed_bytes_length_ = packedBytesLength;
        scratch_ = std::vector<uint8_t>(packedBytesLength);
    }

    void heap_point_reader::read_packed_value(int32_t index, std::vector<uint8_t> &bytes) {
        int32_t block = index / values_per_block_;
        int32_t blockIndex = index % values_per_block_;
        std::copy((*blocks_)[block].begin() + blockIndex * packed_bytes_length_,
                  (*blocks_)[block].begin() + blockIndex * packed_bytes_length_ + packed_bytes_length_,
                  bytes.begin());
    }

    bool heap_point_reader::next() {
        cur_read_++;
        return cur_read_ < end_;
    }

    const std::vector<uint8_t> &heap_point_reader::packed_value() {
        read_packed_value(cur_read_, scratch_);
        return scratch_;
    }

    uint8_t* heap_point_reader::packed_value_raw() {
        int32_t block = cur_read_ / values_per_block_;
        int32_t blockIndex = cur_read_ % values_per_block_;
        return (*blocks_)[block].data() + blockIndex * packed_bytes_length_;
    }

    int32_t heap_point_reader::docid() {
        return (*doc_ids_)[cur_read_];
    }

    int64_t heap_point_reader::ord() {
        if (single_value_per_doc_) {
            return (*doc_ids_)[cur_read_];
        } else if (ords_long_.size() > 0) {
            return ords_long_[cur_read_];
        } else {
            return ords_[cur_read_];
        }
    }

}// namespace bkd
CL_NS_END