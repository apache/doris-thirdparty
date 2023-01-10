#include "bkd_msb_radix_sorter.h"

#include <utility>

CL_NS_DEF2(util, bkd)
bkd_msb_radix_sorter::bkd_msb_radix_sorter(
        bkd_writer * writer,
        heap_point_writer * heap_writer,
        int dim, int32_t bytes) : MSBRadixSorter(bytes), dim(dim), writer(writer), heap_writer(heap_writer) {
}

int bkd_msb_radix_sorter::byteAt(int i, int k) {
    assert(k >= 0);
    if (k < writer->bytes_per_dim_) {
        // dim bytes
        int block = i / heap_writer->values_per_block_;
        int index = i % heap_writer->values_per_block_;
        return heap_writer->blocks_[block][index * writer->packed_bytes_length_ +
                                          dim * writer->bytes_per_dim_ + k] &
               0xff;
    } else {
        // doc id
        int s = 3 - (k - writer->bytes_per_dim_);
        return (static_cast<int>(static_cast<unsigned int>(heap_writer->doc_IDs_[i]) >>
                                 (s * 8))) &
               0xff;
    }
    if (k < writer->bytes_per_dim_) {
        int32_t block = i / heap_writer->values_per_block_;
        int32_t index = i % heap_writer->values_per_block_;
        return heap_writer->blocks_[block][index * writer->packed_bytes_length_ + dim * writer->bytes_per_dim_ + k] & 0xff;
    } else {
        int32_t s = 3 - (k - writer->bytes_per_dim_);
        return (static_cast<int>(static_cast<unsigned int>(heap_writer->doc_IDs_[i]) >> (s * 8))) & 0xff;
    }
}

void bkd_msb_radix_sorter::swap(int i, int j) {
    int32_t doc_id = heap_writer->doc_IDs_[i];
    heap_writer->doc_IDs_[i] = heap_writer->doc_IDs_[j];
    heap_writer->doc_IDs_[j] = doc_id;

    if (!writer->single_value_per_doc_) {
        if (writer->long_ords_) {
            int64_t ord = heap_writer->ords_long_[j];
            heap_writer->ords_long_[i] = heap_writer->ords_long_[j];
            heap_writer->ords_long_[j] = ord;
        } else {
            int32_t ord = heap_writer->ords_[i];
            heap_writer->ords_[i] = heap_writer->ords_[j];
            heap_writer->ords_[j] = ord;
        }
    }

    int indexI = (i % heap_writer->values_per_block_) * writer->packed_bytes_length_;
    int indexJ = (j % heap_writer->values_per_block_) * writer->packed_bytes_length_;

    if (writer->packed_bytes_length_ == 4) {
        auto *value1 = reinterpret_cast<uint32_t*>(heap_writer->blocks_[i / heap_writer->values_per_block_].data() + indexI);
        auto *value2 = reinterpret_cast<uint32_t*>(heap_writer->blocks_[j / heap_writer->values_per_block_].data() + indexJ);
        uint32_t tmp = *value1;
        *value1 = *value2;
        *value2 = tmp;
    }  else {
        auto& blockI = heap_writer->blocks_[i / heap_writer->values_per_block_];
        auto& blockJ = heap_writer->blocks_[j / heap_writer->values_per_block_];
        std::copy(blockI.begin() + indexI,
                  blockI.begin() + indexI + writer->packed_bytes_length_,
                  writer->scratch1_.begin());
        std::copy(blockJ.begin() + indexJ,
                  blockJ.begin() + indexJ + writer->packed_bytes_length_,
                  blockI.begin() + indexI);
        std::copy(writer->scratch1_.begin(),
                  writer->scratch1_.begin() + writer->packed_bytes_length_,
                  blockJ.begin() + indexJ);
    }
}

CL_NS_END2
