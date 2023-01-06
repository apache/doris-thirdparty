#include "bkd_msb_radix_sorter.h"

#include <utility>

CL_NS_DEF2(util, bkd)
bkd_msb_radix_sorter::bkd_msb_radix_sorter(
        //std::shared_ptr<BKDWriter>&& writer,
        bkd_writer * writer,
        //std::shared_ptr<HeapPointWriter>& heapWriter,
        heap_point_writer * heapWriter,
        int dim, int32_t bytes) : MSBRadixSorter(bytes), dim(dim), writer(writer), heapWriter(heapWriter) {
}

int bkd_msb_radix_sorter::byteAt(int i, int k) {
    assert(k >= 0);
    if (k < writer->bytes_per_dim_) {
        // dim bytes
        int block = i / heapWriter->values_per_block_;
        int index = i % heapWriter->values_per_block_;
        return heapWriter->blocks_[block][index * writer->packed_bytes_length_ +
                                          dim * writer->bytes_per_dim_ + k] &
               0xff;
    } else {
        // doc id
        int s = 3 - (k - writer->bytes_per_dim_);
        return (static_cast<int>(static_cast<unsigned int>(heapWriter->doc_IDs_[i]) >>
                                 (s * 8))) &
               0xff;
    }
    if (k < writer->bytes_per_dim_) {
        int32_t block = i / heapWriter->values_per_block_;
        int32_t index = i % heapWriter->values_per_block_;
        return heapWriter->blocks_[block][index * writer->packed_bytes_length_ + dim * writer->bytes_per_dim_ + k] & 0xff;
    } else {
        //TODO:try to figure out what is doing here.
        int32_t s = 3 - (k - writer->bytes_per_dim_);
        //return (MiscUtils::UnsignedShift(writer->doc_IDs_[i], s * 8)) & 0xff;
        return (static_cast<int>(static_cast<unsigned int>(heapWriter->doc_IDs_[i]) >> (s * 8))) & 0xff;
    }
}

void bkd_msb_radix_sorter::swap(int i, int j) {
    int32_t doc_id = heapWriter->doc_IDs_[i];
    heapWriter->doc_IDs_[i] = heapWriter->doc_IDs_[j];
    heapWriter->doc_IDs_[j] = doc_id;

    if (!writer->single_value_per_doc_) {
        if (writer->long_ords_) {
            int64_t ord = heapWriter->ords_long_[j];
            heapWriter->ords_long_[i] = heapWriter->ords_long_[j];
            heapWriter->ords_long_[j] = ord;
        } else {
            int32_t ord = heapWriter->ords_[i];
            heapWriter->ords_[i] = heapWriter->ords_[j];
            heapWriter->ords_[j] = ord;
        }
    }

    //shared_ptr<vector<uint8_t>>& blockI = heapWriter->blocks_[i / heapWriter->values_per_block_];
    int indexI = (i % heapWriter->values_per_block_) * writer->packed_bytes_length_;
    //shared_ptr<vector<uint8_t>>& blockJ = heapWriter->blocks_[j / heapWriter->values_per_block_];
    int indexJ = (j % heapWriter->values_per_block_) * writer->packed_bytes_length_;

    if (writer->packed_bytes_length_ == 4) {
        auto *value1 = reinterpret_cast<uint32_t*>(heapWriter->blocks_[i / heapWriter->values_per_block_].data() + indexI);
        auto *value2 = reinterpret_cast<uint32_t*>(heapWriter->blocks_[j / heapWriter->values_per_block_].data() + indexJ);
        uint32_t tmp = *value1;
        *value1 = *value2;
        *value2 = tmp;
    }  else {
        // scratch1 = values[i]
        auto& blockI = heapWriter->blocks_[i / heapWriter->values_per_block_];
        auto& blockJ = heapWriter->blocks_[j / heapWriter->values_per_block_];
        std::copy(blockI.begin() + indexI,
                  blockI.begin() + indexI + writer->packed_bytes_length_,
                  writer->scratch1_.begin());
        // values[i] = values[j]
        std::copy(blockJ.begin() + indexJ,
                  blockJ.begin() + indexJ + writer->packed_bytes_length_,
                  blockI.begin() + indexI);
        // values[j] = scratch1
        std::copy(writer->scratch1_.begin(),
                  writer->scratch1_.begin() + writer->packed_bytes_length_,
                  blockJ.begin() + indexJ);
    }

    /*int32_t doc_id = heapWriter->doc_IDs_[i];
    heapWriter->doc_IDs_[i] = heapWriter->doc_IDs_[j];
    heapWriter->doc_IDs_[j] = doc_id;
    if (!writer->single_value_per_doc_) {
        if (writer->long_ords_) {
            int64_t ord = heapWriter->ords_long_.at(j);
            heapWriter->ords_long_.at(i) = heapWriter->ords_long_.at(j);
            heapWriter->ords_long_.at(j) = ord;
        } else {
            int32_t ord = heapWriter->ords_.at(i);
            heapWriter->ords_.at(i) = heapWriter->ords_.at(j);
            heapWriter->ords_.at(j) = ord;
        }
    }
    std::vector<uint8_t> &block_i = heapWriter->blocks_.at(i / heapWriter->values_per_block_);
    int32_t index_i = (i % heapWriter->values_per_block_) * writer->packed_bytes_length_;
    std::vector<uint8_t> &block_j = heapWriter->blocks_.at(j / heapWriter->values_per_block_);
    int32_t index_j = (j % heapWriter->values_per_block_) * writer->packed_bytes_length_;
    //scratch1 = values[i]
    std::copy(block_i.begin() + index_i,
              block_i.begin() + index_i + writer->packed_bytes_length_,
              writer->scratch1_->begin());
    // values[i] = values[j]
    std::copy(block_j.begin() + index_j,
              block_j.begin() + index_j + writer->packed_bytes_length_,
              block_i.begin() + index_i);
    // values[j] = scratch1
    std::copy(writer->scratch1_->begin(),
              writer->scratch1_->begin() + writer->packed_bytes_length_,
              block_j.begin() + index_j);*/
}

CL_NS_END2