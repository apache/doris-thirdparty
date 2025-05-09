#include "CLucene/SharedHeader.h"
#include "CLucene/_ApiHeader.h"

#include "CLucene/util/CodecUtil.h"
#include "CLucene/util/FutureArrays.h"
#include "CLucene/util/Time.h"
#include "CLucene/index/_IndexFileNames.h"
#include "bkd_reader.h"
#include "bkd_writer.h"
#include "docids_writer.h"
#include "legacy_index_tree.h"
#include "packed_index_tree.h"

#include <cmath>
#include <iostream>
#include <iomanip>

CL_NS_USE(index)
CL_NS_DEF2(util, bkd)

bkd_reader::bkd_reader(store::IndexInput *in) {
    in_ = std::unique_ptr<store::IndexInput>(in);
}

bkd_reader::~bkd_reader() {
    if(_close_directory && _dir){
        _dir->close();
    }
    _CLDECDELETE(_dir);
}

bkd_reader::bkd_reader(store::Directory *dir, bool close_directory): _close_directory(close_directory) {
    _dir = _CL_POINTER(dir);
}

bool bkd_reader::open() {
    in_ = std::unique_ptr<store::IndexInput>(_dir->openInput(IndexFileNames::BKD_DATA));
    auto meta_in = std::unique_ptr<store::IndexInput>(_dir->openInput(IndexFileNames::BKD_META));
    auto index_in =std::unique_ptr<store::IndexInput>(_dir->openInput(IndexFileNames::BKD_INDEX));
    if (0 == read_meta(meta_in.get())) {
        return false;
    }
    read_index(index_in.get());
    return true;
}

int bkd_reader::read_meta(store::IndexInput* meta_in) {
    type = meta_in->readInt();
    indexFP = meta_in->readLong();
    if (0 == indexFP) {
        return 0;
    } else {
        return 1;
    }
}

void bkd_reader::read_index(store::IndexInput* index_in) {
    version_ = CodecUtil::checkHeader(index_in,
                                      bkd_writer::CODEC_NAME,
                                      bkd_writer::VERSION_START,
                                      bkd_writer::VERSION_CURRENT);
    num_data_dims_ = index_in->readVInt();
    if (version_ >= bkd_writer::VERSION_SELECTIVE_INDEXING) {
        num_index_dims_ = index_in->readVInt();
    } else {
        num_index_dims_ = num_data_dims_;
    }
    max_points_in_leaf_node_ = index_in->readVInt();
    bytes_per_dim_ = index_in->readVInt();
    bytes_per_index_entry_ = (num_data_dims_ == 1) && version_ >= bkd_writer::VERSION_IMPLICIT_SPLIT_DIM_1D ? bytes_per_dim_ : bytes_per_dim_ + 1;
    packed_bytes_length_ = num_data_dims_ * bytes_per_dim_;
    packed_index_bytes_length_ = num_index_dims_ * bytes_per_dim_;

    num_leaves_ = index_in->readVInt();
    assert(num_leaves_ > 0);
    leaf_node_offset_ = num_leaves_;

    min_packed_value_ = std::vector<uint8_t>(packed_index_bytes_length_);
    max_packed_value_ = std::vector<uint8_t>(packed_index_bytes_length_);

    index_in->readBytes(min_packed_value_.data(), packed_index_bytes_length_);
    index_in->readBytes(max_packed_value_.data(), packed_index_bytes_length_);

    for (int32_t dim = 0; dim < num_index_dims_; dim++) {
        if (FutureArrays::CompareUnsigned(min_packed_value_, dim * bytes_per_dim_,
                                          dim * bytes_per_dim_ + bytes_per_dim_,
                                          max_packed_value_,
                                          dim * bytes_per_dim_,
                                          dim * bytes_per_dim_ + bytes_per_dim_) > 0) {
            _CLTHROWA(CL_ERR_CorruptIndex, ("minPackedValue > maxPackedValue for dim=" + std::to_string(dim)).c_str());
        }
    }

    point_count_ = index_in->readVLong();

    doc_count_ = index_in->readVInt();

    if (version_ >= bkd_writer::VERSION_PACKED_INDEX) {
        int32_t numBytes = index_in->readVInt();
        metaOffset = index_in->getFilePointer();

        packed_index_ = std::vector<uint8_t>(numBytes);
        index_in->readBytes(packed_index_.data(), numBytes);
        leaf_block_fps_.clear();
        split_packed_values_.clear();
    } else {
        split_packed_values_ = std::vector<uint8_t>(bytes_per_index_entry_ * num_leaves_);

        index_in->readBytes(split_packed_values_.data(), split_packed_values_.size());

        std::vector<int64_t> leafBlockFPs(num_leaves_);
        int64_t lastFP = 0;
        for (int32_t i = 0; i < num_leaves_; i++) {
            int64_t delta = index_in->readVLong();
            leafBlockFPs[i] = lastFP + delta;
            lastFP += delta;
        }

        if (num_data_dims_ == 1 && num_leaves_ > 1) {
            int32_t levelCount = 2;
            while (true) {
                if (num_leaves_ >= levelCount && num_leaves_ <= 2 * levelCount) {
                    int32_t lastLevel = 2 * (num_leaves_ - levelCount);
                    assert(lastLevel >= 0);
                    if (lastLevel != 0) {
                        std::vector<int64_t> newLeafBlockFPs(num_leaves_);
                        std::copy(leafBlockFPs.begin() + lastLevel,
                                  leafBlockFPs.begin() + lastLevel + (leafBlockFPs.size() - lastLevel),
                                  newLeafBlockFPs.begin());
                        std::copy(leafBlockFPs.begin(),
                                  leafBlockFPs.begin() + lastLevel,
                                  newLeafBlockFPs.begin() + (leafBlockFPs.size() - lastLevel));
                        leafBlockFPs = newLeafBlockFPs;
                    }
                    break;
                }

                levelCount *= 2;
            }
        }

        leaf_block_fps_ = leafBlockFPs;
        packed_index_.clear();
    }
}

bkd_reader::intersect_state::intersect_state(store::IndexInput *in,
                                             int32_t numDims,
                                             int32_t packedBytesLength,
                                             int32_t packedIndexBytesLength,
                                             int32_t maxPointsInLeafNode,
                                             bkd_reader::intersect_visitor *visitor,
                                             index_tree* indexVisitor) {
    in_ = std::unique_ptr<store::IndexInput>(in);
    in_->setIoContext(visitor->get_io_context());
    visitor_ = visitor;
    common_prefix_lengths_.resize(numDims);
    docid_set_iterator = std::make_unique<bkd_docid_set_iterator>(maxPointsInLeafNode);
    scratch_data_packed_value_.resize(packedBytesLength);
    scratch_min_index_packed_value_.resize(packedIndexBytesLength);
    scratch_max_index_packed_value_.resize(packedIndexBytesLength);
    index_ = std::unique_ptr<index_tree>(indexVisitor);
}

std::shared_ptr<bkd_reader::intersect_state> bkd_reader::get_intersect_state(bkd_reader::intersect_visitor *visitor) {
    index_tree* index;
    if (!packed_index_.empty()) {
        index = new packed_index_tree(shared_from_this());
    } else {
        index = new legacy_index_tree(shared_from_this());
    }
    return std::make_shared<intersect_state>(in_->clone(),
                                             num_data_dims_,
                                             packed_bytes_length_,
                                             packed_index_bytes_length_,
                                             max_points_in_leaf_node_,
                                             visitor,
                                             index);
}

void bkd_reader::intersect(bkd_reader::intersect_visitor *visitor) {
    if (indexFP == 0) {
        return;
    }
    // because we will modify min/max packed value in intersect, so we copy them in the first time.
    auto min_packed_value = min_packed_value_;
    auto max_packed_value = max_packed_value_;
    intersect(get_intersect_state(visitor), min_packed_value, max_packed_value);
}

int64_t bkd_reader::estimate_point_count(bkd_reader::intersect_visitor *visitor) {
    // indexFP is 0 means it's a empty tree, just return 0
    if (indexFP == 0) {
        return 0;
    }
    // because we will modify min/max packed value in intersect, so we copy them in the first time.
    auto min_packed_value = min_packed_value_;
    auto max_packed_value = max_packed_value_;
    return estimate_point_count(get_intersect_state(visitor), min_packed_value, max_packed_value);
}

int64_t bkd_reader::estimate_point_count(const std::shared_ptr<bkd_reader::intersect_state> &s, std::vector<uint8_t> &cellMinPacked, std::vector<uint8_t> &cellMaxPacked)
{
    relation r = s->visitor_->compare(cellMinPacked, cellMaxPacked);

    if (r == relation::CELL_OUTSIDE_QUERY) {
        return 0LL;
    } else if (r == relation::CELL_INSIDE_QUERY) {
        return static_cast<int64_t>(max_points_in_leaf_node_) * s->index_->get_num_leaves();
    } else if (s->index_->is_leaf_node()) {
        return (max_points_in_leaf_node_ + 1) / 2;
    } else {
        int32_t splitDim = s->index_->get_split_dim();
        assert(splitDim >= 0);
        assert(splitDim < num_index_dims_);

        std::vector<uint8_t> &splitPackedValue = s->index_->get_split_packed_value();
        std::shared_ptr<BytesRef> splitDimValue = s->index_->get_split_dim_value();
        assert(splitDimValue->length == bytes_per_dim_);
        assert(FutureArrays::CompareUnsigned(cellMinPacked,
                                             splitDim * bytes_per_dim_,
                                             splitDim * bytes_per_dim_ + bytes_per_dim_,
                                             (splitDimValue->bytes),
                                             splitDimValue->offset,
                                             splitDimValue->offset + bytes_per_dim_) <= 0);
        assert(FutureArrays::CompareUnsigned(cellMaxPacked,
                                             splitDim * bytes_per_dim_,
                                             splitDim * bytes_per_dim_ + bytes_per_dim_,
                                             (splitDimValue->bytes),
                                             splitDimValue->offset,
                                             splitDimValue->offset + bytes_per_dim_) >= 0);

        std::copy(cellMaxPacked.begin(),
                  cellMaxPacked.begin() + packed_index_bytes_length_,
                  splitPackedValue.begin());
        std::copy(splitDimValue->bytes.begin() + splitDimValue->offset,
                  splitDimValue->bytes.begin() + splitDimValue->offset + bytes_per_dim_,
                  splitPackedValue.begin() + splitDim * bytes_per_dim_);
        s->index_->push_left();
        int64_t leftCost =
                estimate_point_count(s, cellMinPacked, splitPackedValue);
        s->index_->pop();

        std::copy(splitPackedValue.begin() + splitDim * bytes_per_dim_,
                  splitPackedValue.begin() + splitDim * bytes_per_dim_ + bytes_per_dim_,
                  splitDimValue->bytes.begin() + splitDimValue->offset);

        std::copy(cellMinPacked.begin(),
                  cellMinPacked.begin() + packed_index_bytes_length_,
                  splitPackedValue.begin());
        std::copy(splitDimValue->bytes.begin() + splitDimValue->offset,
                  splitDimValue->bytes.begin() + splitDimValue->offset + bytes_per_dim_,
                  splitPackedValue.begin() + splitDim * bytes_per_dim_);
        s->index_->push_right();
        int64_t rightCost =
                estimate_point_count(s, splitPackedValue, cellMaxPacked);
        s->index_->pop();
        return leftCost + rightCost;
    }
}

void bkd_reader::visit_doc_ids(store::IndexInput *in, int64_t blockFP, bkd_reader::intersect_visitor *visitor) const {
    in->seek(blockFP);

    int32_t count = in->readVInt();
    // avoid read_ints
    if (visitor->only_hits()) {
        visitor->inc_hits(count);
        return;
    }
    if (version_ < bkd_writer::VERSION_COMPRESSED_DOC_IDS) {
        docids_writer::read_ints32(in, count, visitor);
    } else {
        docids_writer::read_ints(in, count, visitor);
    }
}

void bkd_reader::add_all(const std::shared_ptr<intersect_state> &state, bool grown) {
    if (!grown) {
        int64_t maxPointCount = static_cast<int64_t>(max_points_in_leaf_node_) * state->index_->get_num_leaves();
        if (maxPointCount <= std::numeric_limits<int32_t>::max()) {
            state->visitor_->grow(static_cast<int32_t>(maxPointCount));
            grown = true;
        }
    }

    if (state->index_->is_leaf_node()) {
        assert(grown);
        if (state->index_->node_exists()) {
            visit_doc_ids(state->in_.get(), state->index_->get_leaf_blockFP(), state->visitor_);
        }
    } else {
        state->index_->push_left();
        add_all(state, grown);
        state->index_->pop();

        state->index_->push_right();
        add_all(state, grown);
        state->index_->pop();
    }
}

int32_t bkd_reader::read_doc_ids(store::IndexInput *in, int64_t blockFP, bkd_docid_set_iterator *iter) const {
    in->seek(blockFP);

    int32_t count = in->readVInt();

    if (version_ < bkd_writer::VERSION_COMPRESSED_DOC_IDS) {
        iter->docid_set->reset(0, count);
        docids_writer::read_ints32(in, count, iter->docid_set->docids);
    } else {
        docids_writer::read_ints(in, count, iter);
    }

    return count;
}

void bkd_reader::read_common_prefixes(std::vector<int32_t> &commonPrefixLengths, std::vector<uint8_t> &scratchPackedValue, store::IndexInput *in) const {
    for (int32_t dim = 0; dim < num_data_dims_; dim++) {
        int32_t prefix = in->readVInt();
        commonPrefixLengths[dim] = prefix;
        if (prefix > 0) {
            in->readBytes(scratchPackedValue.data(), prefix, dim * bytes_per_dim_);
        }
    }
}

void bkd_reader::read_min_max(const std::vector<int32_t> &commonPrefixLengths,
                              std::vector<uint8_t> &minPackedValue,
                              std::vector<uint8_t> &maxPackedValue,
                              store::IndexInput *in) const {
    for (int32_t dim = 0; dim < num_index_dims_; dim++) {
        int32_t prefix = commonPrefixLengths[dim];
        in->readBytes(minPackedValue.data(), bytes_per_dim_ - prefix, dim * bytes_per_dim_ + prefix);
        in->readBytes(maxPackedValue.data(), bytes_per_dim_ - prefix, dim * bytes_per_dim_ + prefix);
    }
}

void bkd_reader::visit_compressed_doc_values(std::vector<int32_t> &commonPrefixLengths,
                                             std::vector<uint8_t> &scratchPackedValue,
                                             store::IndexInput *in,
                                             bkd_docid_set_iterator *iter,
                                             int32_t count,
                                             bkd_reader::intersect_visitor *visitor,
                                             int32_t compressedDim) const {
    int32_t compressedByteOffset = compressedDim * bytes_per_dim_ + commonPrefixLengths[compressedDim];
    commonPrefixLengths[compressedDim]++;
    int32_t i;
    for (i = 0; i < count;) {
        scratchPackedValue[compressedByteOffset] = in->readByte();
        int32_t runLen = static_cast<int32_t>(in->readByte()) & 0xFF;
        // under runLen, we compare prefix first, if outside matched value, we can skip the whole runLen values.
        std::vector<uint8_t> prefix(scratchPackedValue.begin(), scratchPackedValue.begin() + compressedByteOffset + 1);
        if (visitor->compare_prefix(prefix) == relation::CELL_OUTSIDE_QUERY) {
            size_t skip_bytes = 0;
            for (int32_t dim = 0; dim < num_data_dims_; dim++) {
                int32_t prefix = commonPrefixLengths[dim];
                skip_bytes += bytes_per_dim_ - prefix;
            }
            in->seek(in->getFilePointer() + skip_bytes * (runLen));
            i += runLen;
            continue;
        }
        for (int32_t j = 0; j < runLen; ++j) {
            for (int32_t dim = 0; dim < num_data_dims_; dim++) {
                int32_t prefix = commonPrefixLengths[dim];
                in->readBytes(scratchPackedValue.data(), bytes_per_dim_ - prefix, dim * bytes_per_dim_ + prefix);
            }
            // if scratchPackedValue is larger than matched value, we can skip the left values match because values are sorted from low to high.
            auto res = visitor->visit(iter->docid_set->docids[i + j], scratchPackedValue);
            if ( res > 0) {
                return;
            }
        }
        i += runLen;
    }
    if (i != count) {
        _CLTHROWA(CL_ERR_CorruptIndex, ("Sub blocks do not add up to the expected count: " + std::to_string(count) + " != " + std::to_string(i)).c_str());
    }
}

int32_t bkd_reader::read_compressed_dim(store::IndexInput *in) const {
    auto compressedDim = static_cast<int8_t>(in->readByte());
    auto compressed_dim = static_cast<int32_t>((compressedDim));
    if (compressed_dim < -2 || compressed_dim >= num_data_dims_) {
        _CLTHROWA(CL_ERR_CorruptIndex, ("Got compressedDim=" + std::to_string(compressed_dim)).c_str());
    }
    return compressed_dim;
}

void bkd_reader::visit_unique_raw_doc_values(std::vector<uint8_t> &scratchPackedValue,
                                             bkd_docid_set_iterator *iter,
                                             int32_t count,
                                             bkd_reader::intersect_visitor *visitor) const {
    auto bitmap = iter->bitmap_set->nextDoc();
    while(!bitmap.empty()) {
        visitor->visit(bitmap, scratchPackedValue);
        bitmap = iter->bitmap_set->nextDoc();
    }
}

void bkd_reader::visit_sparse_raw_doc_values(const std::vector<int32_t> &commonPrefixLengths,
                                             std::vector<uint8_t> &scratchPackedValue,
                                             store::IndexInput *in,
                                             bkd_docid_set_iterator *iter,
                                             int32_t count,
                                             bkd_reader::intersect_visitor *visitor) const {
    int i = 0;
    int cardinality = 0;
    if (iter->is_bitmap_set) {
        for (i = 0; i < count;) {
            int length = in->readVInt();
            for (int dim = 0; dim < num_data_dims_; dim++) {
                int prefix = commonPrefixLengths[dim];
                in->readBytes(scratchPackedValue.data(), bytes_per_dim_ - prefix, dim * bytes_per_dim_ + prefix);
            }
            auto bitmap = iter->bitmap_set->nextDoc();
            if (!bitmap.empty()) {
                visitor->visit(bitmap, scratchPackedValue);
            }
            i += length;
            cardinality++;
        }
    } else {
        if (!iter->docid_set->docids.empty()) {
            for (i = 0; i < count;) {
                int length = in->readVInt();
                for (int dim = 0; dim < num_data_dims_; dim++) {
                    int prefix = commonPrefixLengths[dim];
                    in->readBytes(scratchPackedValue.data(), bytes_per_dim_ - prefix, dim * bytes_per_dim_ + prefix);
                }
                iter->docid_set->reset(i, length);
                visitor->visit(iter, scratchPackedValue);
                i += length;
                cardinality++;
            }
        }
    }
    if (i != count) {
        _CLTHROWA(CL_ERR_CorruptIndex, "Sub blocks do not add up to the expected count");
    }
}

void bkd_reader::visit_raw_doc_values(const std::vector<int32_t> &commonPrefixLengths,
                                      std::vector<uint8_t> &scratchPackedValue,
                                      store::IndexInput *in,
                                      bkd_docid_set_iterator *iter,
                                      int32_t count,
                                      bkd_reader::intersect_visitor *visitor) const {
    for (int32_t i = 0; i < count; ++i) {
        for (int32_t dim = 0; dim < num_data_dims_; dim++) {
            int32_t prefix = commonPrefixLengths[dim];
            in->readBytes(scratchPackedValue.data(), bytes_per_dim_ - prefix, dim * bytes_per_dim_ + prefix);
        }
        visitor->visit(iter->docid_set->docids[i], scratchPackedValue);
    }
}

void bkd_reader::visit_doc_values(std::vector<int32_t> &commonPrefixLengths,
                                  std::vector<uint8_t> &scratchDataPackedValue,
                                  const std::vector<uint8_t> &scratchMinIndexPackedValue,
                                  const std::vector<uint8_t> &scratchMaxIndexPackedValue,
                                  store::IndexInput *in,
                                  bkd_docid_set_iterator *iter,
                                  int32_t count,
                                  bkd_reader::intersect_visitor *visitor) {
    read_common_prefixes(commonPrefixLengths, scratchDataPackedValue, in);

    if (num_index_dims_ != 1 && version_ >= bkd_writer::VERSION_LEAF_STORES_BOUNDS) {
        std::vector<uint8_t> minPackedValue = scratchMinIndexPackedValue;
        std::copy(scratchDataPackedValue.begin(),
                  scratchDataPackedValue.begin() + packed_index_bytes_length_,
                  minPackedValue.begin());

        std::vector<uint8_t> maxPackedValue = scratchMaxIndexPackedValue;
        std::copy(minPackedValue.begin(),
                  minPackedValue.begin() + packed_index_bytes_length_,
                  maxPackedValue.begin());

        read_min_max(commonPrefixLengths, minPackedValue, maxPackedValue, in);

        relation r = visitor->compare(minPackedValue, maxPackedValue);
        if (r == relation::CELL_OUTSIDE_QUERY) {
            return;
        }
        visitor->grow(count);

        if (r == relation::CELL_INSIDE_QUERY) {
            for (int32_t i = 0; i < count; ++i) {
                visitor->visit(iter->docid_set->docids[i]);
            }
            return;
        }
    } else {
        visitor->grow(count);
    }

    int32_t compressedDim = version_ < bkd_writer::VERSION_COMPRESSED_VALUES ? -1 : read_compressed_dim(in);

    if (compressedDim == -1) {
        assert(iter->bitmap_set!= nullptr);
        assert(iter->bitmap_set->docids.size() == 1);
        visit_unique_raw_doc_values(scratchDataPackedValue, iter, count, visitor);
    } else {
        if (compressedDim == -2) {
            visit_sparse_raw_doc_values(commonPrefixLengths,
                                        scratchDataPackedValue,
                                        in,
                                        iter,
                                        count,
                                        visitor);
        } else {
            // high cardinality
            visit_compressed_doc_values(commonPrefixLengths,
                                        scratchDataPackedValue,
                                        in,
                                        iter,
                                        count,
                                        visitor,
                                        compressedDim);
        }
    }
}

void bkd_reader::intersect(const std::shared_ptr<bkd_reader::intersect_state> &s, std::vector<uint8_t> &cellMinPacked, std::vector<uint8_t> &cellMaxPacked) {
    relation r = s->visitor_->compare(cellMinPacked, cellMaxPacked);

    if (r == relation::CELL_OUTSIDE_QUERY) {
    } else if (r == relation::CELL_INSIDE_QUERY) {
        add_all(s, false);
    } else if (s->index_->is_leaf_node()) {
        if (s->index_->node_exists()) {
            int32_t count = read_doc_ids(s->in_.get(), s->index_->get_leaf_blockFP(), s->docid_set_iterator.get());

            visit_doc_values(s->common_prefix_lengths_,
                             s->scratch_data_packed_value_,
                             s->scratch_min_index_packed_value_,
                             s->scratch_max_index_packed_value_,
                             s->in_.get(),
                             s->docid_set_iterator.get(),
                             count,
                             s->visitor_);
        }
    } else {
        int32_t splitDim = s->index_->get_split_dim();
        assert(splitDim >= 0);
        assert(splitDim < num_index_dims_);
        // fast path for 1-D BKD
        if (splitDim == 0) {
            auto &splitPackedValue = s->index_->get_split_packed_value();
            auto& splitValue = s->index_->get_split_1dim_value();
            std::copy(splitValue.begin(), splitValue.end(), splitPackedValue.begin());

            s->index_->push_left();
            intersect(s, cellMinPacked, splitPackedValue);
            s->index_->pop();

            s->index_->push_right();
            intersect(s, splitPackedValue, cellMaxPacked);
            s->index_->pop();
        } else {
            std::vector<uint8_t>& splitPackedValue = s->index_->get_split_packed_value();
            std::shared_ptr<BytesRef> splitDimValue = s->index_->get_split_dim_value();
            assert(splitDimValue->length == bytes_per_dim_);
            assert(FutureArrays::CompareUnsigned(cellMinPacked, splitDim * bytes_per_dim_,
                                                 splitDim * bytes_per_dim_ + bytes_per_dim_,
                                                 (splitDimValue->bytes), splitDimValue->offset,
                                                 splitDimValue->offset + bytes_per_dim_) <= 0);
            assert(FutureArrays::CompareUnsigned(cellMaxPacked, splitDim * bytes_per_dim_,
                                                 splitDim * bytes_per_dim_ + bytes_per_dim_,
                                                 (splitDimValue->bytes), splitDimValue->offset,
                                                 splitDimValue->offset + bytes_per_dim_) >= 0);

            std::copy(cellMaxPacked.begin(), cellMaxPacked.begin() + packed_index_bytes_length_,
                      splitPackedValue.begin());
            std::copy(splitDimValue->bytes.begin() + splitDimValue->offset,
                      splitDimValue->bytes.begin() + splitDimValue->offset + bytes_per_dim_,
                      splitPackedValue.begin() + splitDim * bytes_per_dim_);
            s->index_->push_left();
            intersect(s, cellMinPacked, splitPackedValue);
            s->index_->pop();

            std::copy(splitPackedValue.begin() + splitDim * bytes_per_dim_,
                      splitPackedValue.begin() + splitDim * bytes_per_dim_ + bytes_per_dim_,
                      splitDimValue->bytes.begin() + splitDimValue->offset);

            std::copy(cellMinPacked.begin(), cellMinPacked.begin() + packed_index_bytes_length_,
                      splitPackedValue.begin());
            std::copy(splitDimValue->bytes.begin() + splitDimValue->offset,
                      splitDimValue->bytes.begin() + splitDimValue->offset + bytes_per_dim_,
                      splitPackedValue.begin() + splitDim * bytes_per_dim_);
            s->index_->push_right();
            intersect(s, splitPackedValue, cellMaxPacked);
            s->index_->pop();
        }
    }
}

int32_t bkd_reader::get_tree_depth() const {
    return floor(log2(num_leaves_)) + 2;
}

int64_t bkd_reader::ram_bytes_used() {
    if (!packed_index_.empty()) {
        return packed_index_.capacity();
    } else {
        return split_packed_values_.capacity() + leaf_block_fps_.capacity() * sizeof(int64_t);
    }
}

CL_NS_END2
