#include "CLucene/SharedHeader.h"
#include "CLucene/util/CodecUtil.h"
#include "CLucene/util/FixedBitSet.h"
#include "CLucene/util/FutureArrays.h"
#include "CLucene/util/NumericUtils.h"
#include "CLucene/store/_RAMDirectory.h"

#include "bkd_msb_radix_sorter.h"
#include "bkd_writer.h"
#include "docids_writer.h"

#include <functional>
#include <limits>
#include <numeric>
#include <utility>
CL_NS_DEF(util)

namespace bkd {
    const std::wstring bkd_writer::CODEC_NAME = L"BKD";
    bkd_writer::bkd_writer(int32_t maxDoc, int32_t numDataDims,
                           int32_t num_index_dims, int32_t bytes_per_dim, int32_t maxPointsInLeafNode, double maxMBSortInHeap,
                           int64_t totalPointCount, bool singleValuePerDoc, int32_t maxDepth) : bkd_writer(maxDoc, numDataDims,num_index_dims, bytes_per_dim, maxPointsInLeafNode, maxMBSortInHeap, totalPointCount, singleValuePerDoc,
                                                                                         totalPointCount > std::numeric_limits<std::int32_t>::max(), maxDepth) {
    }

    bkd_writer::bkd_writer(int32_t maxDoc, int32_t numDataDims,
                           int32_t num_index_dims, int32_t bytes_per_dim, int32_t maxPointsInLeafNode, double maxMBSortInHeap,
                           int64_t totalPointCount, bool singleValuePerDoc, bool longOrds, int32_t maxDepth) {
        verify_params(numDataDims, num_index_dims, maxPointsInLeafNode, maxMBSortInHeap, totalPointCount);
        max_depth_ = maxDepth;
        max_points_in_leaf_node_ = maxPointsInLeafNode;
        num_data_dims_ = numDataDims;
        num_index_dims_ = num_index_dims;
        bytes_per_dim_ = bytes_per_dim;
        total_point_count_ = totalPointCount;
        max_doc_ = maxDoc;
        max_depth_ = maxDepth;
        packed_bytes_length_ = numDataDims * bytes_per_dim;
        packed_index_bytes_length_ = num_index_dims_ * bytes_per_dim;


        // If we may have more than 1+Integer.MAX_VALUE values, then we must encode ords with int64_t (8 bytes), else we can use int32_t (4 bytes).
        long_ords_ = longOrds;
        single_value_per_doc_ = singleValuePerDoc;

        // dimensional values (numDims * bytes_per_dim_) + ord (int32_t or int64_t) + docid (int32_t)
        if (singleValuePerDoc) {
            // Lucene only supports up to 2.1 docs, so we better not need longOrds in this case:
            assert(longOrds == false);
            bytes_per_doc_ = packed_bytes_length_ + 4;
        } else if (longOrds) {
            bytes_per_doc_ = packed_bytes_length_ + 8 + 4;
        } else {
            bytes_per_doc_ = packed_bytes_length_ + 4 + 4;
        }

        // because offline sort is not implemented yet, we just use heap for all points
        max_points_sort_in_heap_ = totalPointCount;
        //max_points_sort_in_heap_ = (int32_t) (0.5 * (maxMBSortInHeap * 1024 * 1024) / (bytes_per_doc_ * numDataDims));
        // Finally, we must be able to hold at least the leaf node in heap during build:
        if (max_points_sort_in_heap_ < maxPointsInLeafNode) {
            auto msg = "maxMBSortInHeap=" + std::to_string(maxMBSortInHeap) + " only allows for maxPointsSortInHeap=" + std::to_string(max_points_sort_in_heap_) + ", but this is less than maxPointsInLeafNode=" + std::to_string(maxPointsInLeafNode) + "; either increase maxMBSortInHeap or decrease maxPointsInLeafNode";
            _CLTHROWA(CL_ERR_IllegalArgument, msg.c_str());
        }
        // max_points_sort_in_heap_ = 100 * 1024 * 1024;
        // We write first maxPointsSortInHeap in heap, then cutover to offline for additional points:
        heap_point_writer_ = std::make_shared<heap_point_writer>(16, max_points_sort_in_heap_, packed_bytes_length_, longOrds, singleValuePerDoc);
        max_mb_sort_in_heap_ = maxMBSortInHeap;

        max_packed_value_.resize(packed_bytes_length_);
        min_packed_value_.resize(packed_bytes_length_);

        common_prefix_lengths_.resize(num_data_dims_);
        scratch1_.resize(packed_bytes_length_);
        scratch_diff_.resize(bytes_per_dim_);
        scratch2_.resize(packed_bytes_length_);
    }

    void bkd_writer::verify_params(int32_t numDataDims, int32_t numIndexDims, int32_t maxPointsInLeafNode,
                                   double maxMBSortInHeap, int64_t totalPointCount) {
        std::string msg;
        // We encode dim in a single byte in the splitPackedValues, but we only expose 4 bits for it now, in case we want to use
        // remaining 4 bits for another purpose later
        if (numDataDims < 1 || numDataDims > MAX_DIMS) {
            msg = "numDataDims must be 1 .. " + std::to_string(MAX_DIMS) + " (got: " + std::to_string(numDataDims) + ")";
        }
        if (numIndexDims < 1 || numIndexDims > numDataDims) {
            msg = "num_index_dims_ must be 1 .. " + std::to_string(numDataDims) + " (got: " + std::to_string(numIndexDims) + ")";
        }
        if (maxPointsInLeafNode <= 0) {
            msg = "maxPointsInLeafNode must be > 0; got " + std::to_string(maxPointsInLeafNode);
        }
        if (maxPointsInLeafNode > MAX_ARRAY_LENGTH) {
            msg = "maxPointsInLeafNode must be <= MAX_ARRAY_LENGTH (= " + std::to_string(MAX_ARRAY_LENGTH) + "); got " + std::to_string(maxPointsInLeafNode);
        }
        if (maxMBSortInHeap < 0.0) {
            msg = "maxMBSortInHeap must be >= 0.0 (got: " + std::to_string(maxMBSortInHeap) + ")";
        }
        if (totalPointCount < 0) {
            msg = "totalPointCount must be >=0 (got: " + std::to_string(totalPointCount) + ")";
        }
        if (!msg.empty()) {
            _CLTHROWA(CL_ERR_IllegalArgument, msg.c_str());
        }
    }

    void bkd_writer::check_max_leaf_node_count(int32_t num_leaves) const {
        if ((1 + bytes_per_dim_) * (int64_t) num_leaves > MAX_ARRAY_LENGTH) {
            _CLTHROWA(CL_ERR_IllegalArgument, "too many nodes; increase max_points_in_leaf_node");
        }
    }

    void bkd_writer::add(const uint8_t *packed_value, uint32_t value_len, int32_t doc_id) {
        if (value_len != (size_t) packed_bytes_length_) {
            _CLTHROWA(CL_ERR_IllegalArgument, ("packedValue should be length=" + std::to_string(packed_bytes_length_) + ", got " + std::to_string(value_len)).c_str());
        }
        heap_point_writer_->append(packed_value, value_len, std::min(point_count_, (int64_t) max_points_sort_in_heap_), doc_id);
        if (point_count_ == 0) {
            memcpy(min_packed_value_.data(), packed_value, packed_index_bytes_length_);
            memcpy(max_packed_value_.data(), packed_value, packed_index_bytes_length_);
        } else {
            for (int32_t dim = 0; dim < num_index_dims_; dim++) {
                int32_t offset = dim * bytes_per_dim_;
                if (FutureArrays::CompareUnsigned(packed_value, offset, offset + bytes_per_dim_, min_packed_value_.data(), offset, offset + bytes_per_dim_) < 0) {
                    memcpy(min_packed_value_.data() + offset, packed_value + offset, bytes_per_dim_);
                }
                if (FutureArrays::CompareUnsigned(packed_value, offset, offset + bytes_per_dim_, max_packed_value_.data(), offset, offset + bytes_per_dim_) > 0) {
                    memcpy(max_packed_value_.data() + offset, packed_value + offset, bytes_per_dim_);
                }
            }
        }

        point_count_++;
        if (point_count_ > total_point_count_)
            _CLTHROWA(CL_ERR_IllegalArgument, ("total_point_count_=" + std::to_string(total_point_count_) + " was passed when we were created, but we just hit " + std::to_string(point_count_) + " values").c_str());
    }

    void bkd_writer::add(std::vector<uint8_t> &packedValue, int32_t docid) {
        if (packedValue.size() != (size_t) packed_bytes_length_) {
            _CLTHROWA(CL_ERR_IllegalArgument, ("packedValue should be length=" + std::to_string(packed_bytes_length_) + ", got " + std::to_string(packedValue.size())).c_str());
        }
        heap_point_writer_->append(packedValue, std::min(point_count_, (int64_t) max_points_sort_in_heap_), docid);

        if (point_count_ == 0) {
            std::copy(packedValue.begin(),
                      packedValue.begin() + packed_index_bytes_length_,
                      min_packed_value_.begin());
            std::copy(packedValue.begin(),
                      packedValue.begin() + packed_index_bytes_length_,
                      max_packed_value_.begin());
        } else {
            for (int32_t dim = 0; dim < num_index_dims_; dim++) {
                int32_t offset = dim * bytes_per_dim_;
                if (FutureArrays::CompareUnsigned(packedValue, offset, offset + bytes_per_dim_, min_packed_value_, offset, offset + bytes_per_dim_) < 0) {
                    std::copy(packedValue.begin() + offset,
                              packedValue.begin() + offset + bytes_per_dim_,
                              min_packed_value_.begin() + offset);
                }
                if (FutureArrays::CompareUnsigned(packedValue, offset, offset + bytes_per_dim_, max_packed_value_, offset, offset + bytes_per_dim_) > 0) {
                    std::copy(packedValue.begin() + offset,
                              packedValue.begin() + offset + bytes_per_dim_,
                              max_packed_value_.begin() + offset);
                }
            }
        }

        point_count_++;
        if (point_count_ > total_point_count_)
            _CLTHROWA(CL_ERR_IllegalArgument, ("total_point_count_=" + std::to_string(total_point_count_) + " was passed when we were created, but we just hit " + std::to_string(point_count_) + " values").c_str());
    }

    void bkd_writer::meta_finish(store::IndexOutput *out, int64_t fPointer, int32_t type) {
        out->writeInt(type);
        out->writeLong(fPointer);
    }

    int64_t bkd_writer::finish(store::IndexOutput *out, store::IndexOutput *index_out) {
        if (point_count_ == 0) {
            return 0;
        }
        std::shared_ptr<LongBitSet> ord_bit_set = nullptr;
        if (num_index_dims_ > 1) {
            if (single_value_per_doc_)
                ord_bit_set = std::make_shared<LongBitSet>(max_doc_);
            else
                ord_bit_set = std::make_shared<LongBitSet>(point_count_);
        }

        auto max_points_in_leaf_node = point_count_ / (1LL << max_depth_);
        if (max_points_in_leaf_node > 0) {
            max_points_in_leaf_node_ = max_points_in_leaf_node;
        }

        int64_t count_per_leaf = point_count_;
        int64_t inner_node_count = 1;
        while (count_per_leaf > max_points_in_leaf_node_) {
            count_per_leaf = (count_per_leaf + 1) / 2;
            inner_node_count *= 2;
        }
        auto num_leaves = (int32_t) inner_node_count;
        check_max_leaf_node_count(num_leaves);
        // Indexed by node_id, but first (root) node_id is 1.  We do 1+ because the lead byte at each recursion says which dim we split on.
        std::vector<uint8_t> split_packed_values(num_leaves * (1 + bytes_per_dim_));
        std::vector<int64_t> leaf_block_fps(num_leaves);
        std::vector<std::shared_ptr<path_slice>> sorted_point_writers(num_index_dims_);
        std::vector<std::shared_ptr<point_reader>> to_close_heroically;
        bool success = false;
        std::vector<int32_t> parent_splits(num_index_dims_);
        build(1, num_leaves, sorted_point_writers,
              ord_bit_set, out,
              min_packed_value_, max_packed_value_,
              parent_splits,
              split_packed_values,
              leaf_block_fps,
              to_close_heroically);
        int64_t index_fp = out->getFilePointer();
        write_index(index_out, count_per_leaf, leaf_block_fps, split_packed_values);
        return index_fp;
    }

    /** Pull a partition back into heap once the point count is low enough while recursing. */
    PathSlicePtr bkd_writer::switch_to_heap(const PathSlicePtr &source, const std::vector<std::shared_ptr<point_reader>> &toCloseHeroically) {
        auto count = (int32_t) (source->count_);
        // Not inside the try because we don't want to close it here:
        std::shared_ptr<point_reader> reader = source->writer_->get_shared_reader(source->start_, source->count_, toCloseHeroically);
        std::shared_ptr<point_writer> writer = std::make_shared<heap_point_writer>(count, count, packed_bytes_length_, long_ords_, single_value_per_doc_);
        if (writer != nullptr) {
            for (int32_t i = 0; i < count; i++) {
                bool has_next = reader->next();
                assert(has_next);
                writer->append(reader->packed_value_raw(), packed_bytes_length_, reader->ord(), reader->docid());
            }
        }
        return std::make_shared<path_slice>(writer, 0, count);
    }

    void bkd_writer::write_leaf_block_docs(store::IndexOutput *out, std::vector<int32_t> &docids, int32_t start, int32_t count) {
        assert(count > 0);
        out->writeVInt(count);
        docids_writer::write_doc_ids(docids, start, count, out);
    }
    void bkd_writer::write_leaf_block_docs_bitmap(store::IndexOutput *out, std::vector<int32_t> &docids, int32_t start, int32_t count) {
        assert(count > 0);
        out->writeVInt(count);
        out->writeByte((uint8_t) 1);
        docids_writer::write_doc_ids_bitmap(docids, start, count, out);
    }
    void bkd_writer::write_common_prefixes(store::IndexOutput *out, std::vector<int32_t> &commonPrefixes, std::vector<uint8_t> &packedValue) const {
        for (int32_t dim = 0; dim < num_data_dims_; dim++) {
            out->writeVInt(commonPrefixes[dim]);
            out->writeBytes(packedValue.data(), commonPrefixes[dim], dim * bytes_per_dim_);
        }
    }

    void bkd_writer::write_common_prefixes(store::IndexOutput *out, std::vector<int32_t> &commonPrefixes, std::vector<uint8_t> &packedValue, int offset) const {
        for (int32_t dim = 0; dim < num_data_dims_; dim++) {
            out->writeVInt(commonPrefixes[dim]);
            out->writeBytes(packedValue.data(), commonPrefixes[dim], dim * bytes_per_dim_ + offset);
        }
    }

    int32_t bkd_writer::append_block(const std::shared_ptr<store::RAMOutputStream> &writeBuffer, ByteArrayList &blocks) {
        int32_t pos = writeBuffer->getFilePointer();
        std::vector<uint8_t> bytes = std::vector<uint8_t>(pos);
        writeBuffer->writeTo(bytes, 0);
        writeBuffer->reset();
        blocks.emplace_back(bytes);
        return pos;
    }

    int64_t bkd_writer::get_left_most_leaf_blockFP(std::vector<int64_t> &leafBlockFPs, int32_t nodeID) {
        while (nodeID < (int32_t) leafBlockFPs.size())
            nodeID *= 2;
        int32_t leaf_id = nodeID - leafBlockFPs.size();
        int64_t result = leafBlockFPs.at(leaf_id);
        if (result < 0)
            _CLTHROWA(CL_ERR_IllegalArgument, "file pointer less than 0.");
        return result;
    }

    int32_t bkd_writer::recurse_pack_index(const std::shared_ptr<store::RAMOutputStream> &writeBuffer, std::vector<int64_t> &leafBlockFPs,
                                           std::vector<uint8_t> &splitPackedValues, int64_t minBlockFP,
                                           ByteArrayList &blocks, int32_t nodeID, std::vector<uint8_t> &lastSplitValues,
                                           std::vector<bool> &negativeDeltas, bool isLeft) const {
        if (nodeID >= (int32_t) leafBlockFPs.size()) {
            int32_t leaf_id = nodeID - leafBlockFPs.size();
            if (leaf_id < (int32_t) leafBlockFPs.size()) {
                int64_t delta = leafBlockFPs.at(leaf_id) - minBlockFP;
                if (isLeft) {
                    assert(delta == 0);
                    return 0;
                } else {
                    assert(nodeID == 1 || delta > 0);
                    writeBuffer->writeVLong(delta);
                    return append_block(writeBuffer, blocks);
                }
            } else {
                return 0;
            }
        } else {
            int64_t left_block_fp;
            if (!isLeft) {
                left_block_fp = get_left_most_leaf_blockFP(leafBlockFPs, nodeID);
                int64_t delta = left_block_fp - minBlockFP;
                assert(nodeID == 1 || delta > 0);
                writeBuffer->writeVLong(delta);
            } else {
                left_block_fp = minBlockFP;
            }


            int32_t address = nodeID * (1 + bytes_per_dim_);
            int32_t split_dim = splitPackedValues[address++] & 0xff;

            int32_t prefix = FutureArrays::Mismatch(splitPackedValues, address, address + bytes_per_dim_,
                                                    lastSplitValues, split_dim * bytes_per_dim_, split_dim * bytes_per_dim_ + bytes_per_dim_);
            if (prefix == -1)
                prefix = bytes_per_dim_;

            int32_t first_diff_byte_delta;
            if (prefix < bytes_per_dim_) {
                first_diff_byte_delta = (splitPackedValues[address + prefix] & 0xff) -
                                        (lastSplitValues[split_dim * bytes_per_dim_ + prefix] & 0xff);
                if (negativeDeltas[split_dim])
                    first_diff_byte_delta = -first_diff_byte_delta;
                assert(first_diff_byte_delta > 0);

            } else {
                first_diff_byte_delta = 0;
            }
            int32_t code = (first_diff_byte_delta * (1 + bytes_per_dim_) + prefix) * num_index_dims_ + split_dim;
            writeBuffer->writeVInt(code);
            int32_t suffix = bytes_per_dim_ - prefix;
            std::vector<uint8_t> save_split_value(suffix);
            if (suffix > 1)
                writeBuffer->writeBytes(splitPackedValues.data(), suffix - 1, address + prefix + 1);

            std::vector<uint8_t> cmp = lastSplitValues;

            std::copy(lastSplitValues.begin() + split_dim * bytes_per_dim_ + prefix,
                      lastSplitValues.begin() + split_dim * bytes_per_dim_ + prefix + suffix,
                      save_split_value.begin());

            std::copy(splitPackedValues.begin() + address + prefix,
                      splitPackedValues.begin() + address + prefix + suffix,
                      lastSplitValues.begin() + split_dim * bytes_per_dim_ + prefix);
            int32_t num_bytes = append_block(writeBuffer, blocks);

            int32_t idx_save = blocks.size();
            blocks.emplace_back(std::vector<uint8_t>(0));
            bool save_negative_delta = negativeDeltas[split_dim];
            negativeDeltas[split_dim] = true;
            int32_t left_num_bytes = recurse_pack_index(writeBuffer, leafBlockFPs, splitPackedValues, left_block_fp, blocks,
                                                        2 * nodeID, lastSplitValues, negativeDeltas, true);
            if (nodeID * 2 < (int32_t) leafBlockFPs.size())
                writeBuffer->writeVInt(left_num_bytes);
            else
                assert(left_num_bytes == 0);
            int32_t num_bytes2 = writeBuffer->getFilePointer();
            std::vector<uint8_t> bytes2(num_bytes2);
            writeBuffer->writeTo(bytes2, 0);
            writeBuffer->reset();
            blocks[idx_save] = std::move(bytes2);

            negativeDeltas[split_dim] = false;
            int32_t right_num_bytes = recurse_pack_index(writeBuffer, leafBlockFPs, splitPackedValues, left_block_fp, blocks,
                                                         2 * nodeID + 1, lastSplitValues, negativeDeltas, false);
            negativeDeltas[split_dim] = save_negative_delta;
            std::copy(save_split_value.begin(),
                      save_split_value.begin() + suffix,
                      lastSplitValues.begin() + split_dim * bytes_per_dim_ + prefix);

            assert(std::equal(lastSplitValues.begin(), lastSplitValues.end(), cmp.begin()));
            return num_bytes + num_bytes2 + left_num_bytes + right_num_bytes;
        }
    }

    std::shared_ptr<std::vector<uint8_t>> bkd_writer::pack_index(std::vector<int64_t> &leaf_block_fps,
                                                                 std::vector<uint8_t> &split_packed_values) const {
        int32_t num_leaves = leaf_block_fps.size();
        if (num_index_dims_ == 1 && num_leaves > 1) {
            int32_t level_count = 2;
            while (true) {
                if (num_leaves >= level_count && num_leaves <= 2 * level_count) {
                    int32_t last_level = 2 * (num_leaves - level_count);
                    assert(last_level >= 0);
                    if (last_level != 0) {
                        std::vector<int64_t> new_leaf_block_fps(num_leaves);
                        std::copy(leaf_block_fps.begin() + last_level,
                                  leaf_block_fps.begin() + last_level + (leaf_block_fps.size() - last_level),
                                  new_leaf_block_fps.begin());
                        std::copy(leaf_block_fps.begin(),
                                  leaf_block_fps.begin() + last_level,
                                  new_leaf_block_fps.begin() + (leaf_block_fps.size() - last_level));
                        leaf_block_fps.swap(new_leaf_block_fps);
                    }
                    break;
                }
                level_count *= 2;
            }
        }
        std::shared_ptr<store::RAMOutputStream> write_buffer = std::make_shared<store::RAMOutputStream>();
        ByteArrayList blocks;
        std::vector<uint8_t> last_split_values(bytes_per_dim_ * num_index_dims_);
        std::vector<bool> bools(num_index_dims_);
        int32_t total_size = recurse_pack_index(write_buffer, leaf_block_fps, split_packed_values, (int64_t) 0, blocks, 1, last_split_values, bools, false);
        std::shared_ptr<std::vector<uint8_t>> index = std::make_shared<std::vector<uint8_t>>(total_size);
        int32_t upto = 0;
        for (auto &block: blocks) {
            std::copy(block.begin(), block.begin() + block.size(), index->begin() + upto);
            upto += block.size();
        }
        assert(upto == total_size);
        return index;
    }

    void bkd_writer::write_index(store::IndexOutput *out, int32_t countPerLeaf, std::vector<int64_t> &leafBlockFPs,
                                 std::vector<uint8_t> &splitPackedValues) {
        std::shared_ptr<std::vector<uint8_t>> packed_index = pack_index(leafBlockFPs, splitPackedValues);
        write_index(out, countPerLeaf, leafBlockFPs.size(), *packed_index);
    }

    void bkd_writer::write_index(store::IndexOutput *out, int32_t countPerLeaf, int32_t numLeaves, std::vector<uint8_t> &packedIndex) {
        CodecUtil::writeHeader(out, CODEC_NAME, VERSION_CURRENT);
        out->writeVInt(num_data_dims_);
        out->writeVInt(num_index_dims_);
        out->writeVInt(countPerLeaf);
        out->writeVInt(bytes_per_dim_);

        assert(numLeaves > 0);
        out->writeVInt(numLeaves);
        out->writeBytes(min_packed_value_.data(), packed_index_bytes_length_);
        out->writeBytes(max_packed_value_.data(), packed_index_bytes_length_);

        out->writeVLong(point_count_);
        out->writeVInt(docs_seen_);
        out->writeVInt(packedIndex.size());

        out->writeBytes(packedIndex.data(), packedIndex.size());
    }

    int32_t bkd_writer::run_len(const IntFunction<BytesRef *> &packedValues, int32_t start, int32_t end, int32_t byteOffset) {
        BytesRef *first = packedValues(start);
        uint8_t b = first->bytes.at(first->offset + byteOffset);
        for (int32_t i = start + 1; i < end; ++i) {
            BytesRef *ref = packedValues(i);
            uint8_t b2 = ref->bytes.at(ref->offset + byteOffset);
            if (b != b2)
                return i - start;
        }
        return end - start;
    }

    void bkd_writer::write_leaf_block_packed_values_range(store::IndexOutput *out, std::vector<int32_t> &commonPrefixLengths,
                                                          int32_t start, int32_t end, const IntFunction<BytesRef *> &packedValues) const {
        for (int32_t i = start; i < end; i++) {
            BytesRef *ref = packedValues(i);
            assert(ref->length == packed_bytes_length_);
            for (int32_t dim = 0; dim < num_data_dims_; dim++) {
                int32_t prefix = commonPrefixLengths[dim];
                out->writeBytes(ref->bytes.data(), bytes_per_dim_ - prefix, ref->offset + dim * bytes_per_dim_ + prefix);
            }
        }
    }

    void bkd_writer::write_low_cardinality_leaf_block_packed_values(store::IndexOutput *out, std::vector<int32_t> &common_prefix_lengths,
                                                                    int32_t count, const IntFunction<BytesRef *> &packed_values) {
        BytesRef *value = packed_values(0);
        std::copy(value->bytes.begin() + value->offset, value->bytes.begin() + value->offset + num_data_dims_ * bytes_per_dim_, scratch1_.begin());
        int cardinality = 1;
        for (int i = 1; i < count; i++) {
            value = packed_values(i);
            for (int dim = 0; dim < num_data_dims_; dim++) {
                int start = dim * bytes_per_dim_ + common_prefix_lengths[dim];
                int end = dim * bytes_per_dim_ + bytes_per_dim_;
                if (FutureArrays::Mismatch(value->bytes, value->offset + start, value->offset + end, scratch1_, start, end) != -1) {
                    out->writeVInt(cardinality);
                    for (int j = 0; j < num_data_dims_; j++) {
                        out->writeBytes(scratch1_.data(), bytes_per_dim_ - common_prefix_lengths[j], j * bytes_per_dim_ + common_prefix_lengths[j]);
                    }
                    std::copy(value->bytes.begin() + value->offset, value->bytes.begin() + value->offset + num_data_dims_ * bytes_per_dim_, scratch1_.begin());
                    cardinality = 1;
                    break;
                } else if (dim == num_data_dims_ - 1) {
                    cardinality++;
                }
            }
        }
        out->writeVInt(cardinality);
        for (int i = 0; i < num_data_dims_; i++) {
            out->writeBytes(scratch1_.data(), bytes_per_dim_ - common_prefix_lengths[i], i * bytes_per_dim_ + common_prefix_lengths[i]);
        }
    }

    void bkd_writer::write_high_cardinality_leaf_block_packed_values(store::IndexOutput *out, std::vector<int32_t> &common_prefix_lengths,
                                                                     int32_t count, int32_t sorted_dim, const IntFunction<BytesRef *> &packed_values,
                                                                     int32_t compressed_byte_offset) const {
        common_prefix_lengths[sorted_dim]++;
        for (int32_t i = 0; i < count;) {
            int32_t run_length = run_len(packed_values, i, std::min(i + 0xff, count), compressed_byte_offset);
            assert(run_length <= 0xff);
            BytesRef *first = packed_values(i);
            uint8_t prefix_byte = first->bytes.at(first->offset + compressed_byte_offset);
            out->writeByte(prefix_byte);
            out->writeByte((uint8_t) run_length);
            write_leaf_block_packed_values_range(out, common_prefix_lengths, i, i + run_length, packed_values);
            i += run_length;
            assert(i <= count);
        }
    }

    void bkd_writer::write_leaf_block_packed_values(store::IndexOutput *out,
                                                    std::vector<int32_t> &common_prefix_lengths,
                                                    int32_t count, int32_t sorted_dim,
                                                    const IntFunction<BytesRef *> &packed_values,
                                                    int32_t prefix_len_sum,
                                                    bool low_cardinality) {
        if (prefix_len_sum == packed_bytes_length_) {
            out->writeByte(int8_t(-1));
        } else {
            int32_t compressed_byte_offset = sorted_dim * bytes_per_dim_ + common_prefix_lengths[sorted_dim];
            if (low_cardinality) {
                out->writeByte(int8_t(-2));
                write_low_cardinality_leaf_block_packed_values(out, common_prefix_lengths, count, packed_values);
            } else {
                out->writeByte((uint8_t) sorted_dim);
                write_high_cardinality_leaf_block_packed_values(out, common_prefix_lengths, count, sorted_dim, packed_values, compressed_byte_offset);
            }
        }
    }

    int32_t bkd_writer::split(std::vector<uint8_t> &min_packed_value, std::vector<uint8_t> &max_packed_value, std::vector<int32_t> &parent_splits) {
        int32_t max_num_splits = 0;
        max_num_splits = *(std::max_element(parent_splits.begin(), parent_splits.end()));
        for (int32_t dim = 0; dim < num_index_dims_; ++dim) {
            int32_t offset = dim * bytes_per_dim_;
            if (parent_splits[dim] < max_num_splits / 2 && FutureArrays::CompareUnsigned(min_packed_value, offset, offset + bytes_per_dim_, max_packed_value, offset, offset + bytes_per_dim_) != 0)
                return dim;
        }
        int32_t split_dim = -1;
        for (int32_t dim = 0; dim < num_index_dims_; dim++) {
            NumericUtils::subtract(bytes_per_dim_, dim, max_packed_value, min_packed_value, scratch_diff_);
            if (split_dim == -1 || FutureArrays::CompareUnsigned(scratch_diff_, 0, bytes_per_dim_, scratch1_, 0, bytes_per_dim_) > 0) {
                std::copy(scratch_diff_.begin(), scratch_diff_.begin() + bytes_per_dim_, scratch1_.begin());
                split_dim = dim;
            }
        }
        return split_dim;
    }

    const uint8_t *bkd_writer::mark_right_tree(int64_t rightCount, int32_t splitDim, std::shared_ptr<path_slice> &source, const std::shared_ptr<LongBitSet> &ordBitSet) const {
        std::shared_ptr<point_reader> reader = source->writer_->get_reader(source->start_ + source->count_ - rightCount, rightCount);
        bool result = reader->next();
        assert(result);
        if (reader != nullptr) {
            if (num_index_dims_ > 1 && ordBitSet != nullptr) {
                assert(ordBitSet->Get(reader->ord()) == false);
                ordBitSet->Set(reader->ord());
                reader->mark_ords(rightCount - 1, ordBitSet);
            }
            return reader->packed_value_raw() + splitDim * bytes_per_dim_;
        }
        return nullptr;
    }

    std::shared_ptr<point_writer> bkd_writer::get_point_writer(int64_t count, std::string &desc) {
        return std::make_shared<heap_point_writer>(std::min(count, int64_t(max_points_sort_in_heap_)), std::min(count, int64_t(max_points_sort_in_heap_)), packed_bytes_length_, long_ords_, single_value_per_doc_);
    }

    bool bkd_writer::leaf_node_is_low_cardinality(std::vector<int32_t> &common_prefix_lengths,
                                                  int32_t count, int32_t sorted_dim,
                                                  const IntFunction<BytesRef *> &packed_values,
                                                  const std::vector<int> &leaf_cardinality,
                                                  int32_t prefix_len_sum) const {
        int32_t compressed_byte_offset = sorted_dim * bytes_per_dim_ + common_prefix_lengths[sorted_dim];
        int highCardinalityCost;
        int lowCardinalityCost;
        if (count == leaf_cardinality.size()) {
            highCardinalityCost = 0;
            lowCardinalityCost = 1;
        } else {
            int numRunLens = 0;
            for (int i = 0; i < count;) {
                int32_t run_length = run_len(packed_values, i, std::min(i + 0xff, count), compressed_byte_offset);
                assert(run_length <= 0xff);
                numRunLens++;
                i += run_length;
            }
            highCardinalityCost = count * (num_data_dims_ * bytes_per_dim_ - prefix_len_sum - 1) + 2 * numRunLens;
            lowCardinalityCost = leaf_cardinality.size() * (num_data_dims_ * bytes_per_dim_ - prefix_len_sum + 1);
        }
        return (lowCardinalityCost <= highCardinalityCost);
    }

    void bkd_writer::build(int32_t nodeID, int32_t leafNodeOffset,
                           std::vector<std::shared_ptr<path_slice>> &slices,
                           const std::shared_ptr<LongBitSet> &ordBitSet,
                           store::IndexOutput *out,
                           std::vector<uint8_t> &minPackedValue,
                           std::vector<uint8_t> &maxPackedValue,
                           std::vector<int32_t> &parentSplits,
                           std::vector<uint8_t> &splitPackedValues,
                           std::vector<int64_t> &leafBlockFPs,
                           const std::vector<std::shared_ptr<point_reader>> &toCloseHeroically) {
        if (nodeID >= leafNodeOffset) {

            int32_t sorted_dim = 0;
            int32_t sorted_dim_cardinality = std::numeric_limits<int32_t>::max();

            for (int32_t dim = 0; dim < num_index_dims_; dim++) {
                bool created = false;
                if (slices[dim] == nullptr) {
                    create_path_slice(slices, dim);
                    created = true;
                }
                if (std::dynamic_pointer_cast<heap_point_writer>(slices[dim]->writer_) == nullptr) {
                    PathSlicePtr slice = slices[dim];
                    slices[dim] = switch_to_heap(slices[dim], toCloseHeroically);
                    if (created) {
                        slice->writer_->destroy();
                    }
                }

                PathSlicePtr source = slices[dim];

                std::shared_ptr<heap_point_writer> heap_source = std::dynamic_pointer_cast<heap_point_writer>(source->writer_);

                heap_source->read_packed_value(source->start_, scratch1_);
                heap_source->read_packed_value(source->start_ + source->count_ - 1, scratch2_);
                int32_t offset = dim * bytes_per_dim_;
                common_prefix_lengths_[dim] = FutureArrays::Mismatch(scratch1_, offset, offset + bytes_per_dim_, scratch2_, offset, offset + bytes_per_dim_);

                if (common_prefix_lengths_[dim] == -1)
                    common_prefix_lengths_[dim] = bytes_per_dim_;
                sorted_dim = dim;
            }

            PathSlicePtr data_dim_path_slice = nullptr;

            if (num_data_dims_ != num_index_dims_) {
                std::shared_ptr<heap_point_writer> heap_source = std::dynamic_pointer_cast<heap_point_writer>(slices[0]->writer_);
                auto from = (int32_t) slices[0]->start_;
                int32_t to = from + (int32_t) slices[0]->count_;
                std::fill(common_prefix_lengths_.begin() + num_index_dims_,
                          common_prefix_lengths_.begin() + num_index_dims_ + num_data_dims_,
                          bytes_per_dim_);
                heap_source->read_packed_value(from, scratch1_);
                for (int32_t i = from + 1; i < to; ++i) {
                    heap_source->read_packed_value(i, scratch2_);
                    for (int32_t dim = num_index_dims_; dim < num_data_dims_; dim++) {
                        int32_t offset = dim * bytes_per_dim_;
                        int32_t dimension_prefix_length = common_prefix_lengths_[dim];
                        common_prefix_lengths_[dim] = FutureArrays::Mismatch(scratch1_, offset, offset + dimension_prefix_length,
                                                                             scratch2_, offset, offset + dimension_prefix_length);
                        if (common_prefix_lengths_[dim] == -1) {
                            common_prefix_lengths_[dim] = dimension_prefix_length;
                        }
                    }
                }
                if (common_prefix_lengths_[sorted_dim] == bytes_per_dim_) {
                    for (int32_t dim = num_index_dims_; dim < num_data_dims_; ++dim) {
                        if (common_prefix_lengths_[dim] != bytes_per_dim_) {
                            sorted_dim = dim;
                            data_dim_path_slice = switch_to_heap(slices[0], toCloseHeroically);
                            std::shared_ptr<heap_point_writer> heap_writer = std::dynamic_pointer_cast<heap_point_writer>(data_dim_path_slice->writer_);
                            sort_heap_point_writer(heap_writer, (int32_t) data_dim_path_slice->count_, sorted_dim);
                            break;
                        }
                    }
                }
            }

            PathSlicePtr source = (data_dim_path_slice != nullptr) ? data_dim_path_slice : slices[sorted_dim];

            std::shared_ptr<heap_point_writer> heap_source = std::dynamic_pointer_cast<heap_point_writer>(source->writer_);
            auto from = (int32_t) slices[0]->start_;
            int32_t to = from + (int32_t) slices[0]->count_;
            auto leaf_cardinality = heap_source->compute_cardinality(from, to, num_data_dims_, bytes_per_dim_, common_prefix_lengths_);

            leafBlockFPs[nodeID - leafNodeOffset] = out->getFilePointer();
            int32_t count = source->count_;
            scratch_bytes_ref2.length = packed_bytes_length_;
            std::function<BytesRef *(int32_t)> packed_values = [&](int32_t i) -> BytesRef * {
                heap_source->get_packed_value_slice((source->start_ + i), scratch_bytes_ref2);
                return &scratch_bytes_ref2;
            };
            assert(count > 0);
            int32_t prefix_len_sum = std::accumulate(common_prefix_lengths_.begin(), common_prefix_lengths_.end(), 0);
            bool low_cardinal = false;
            if (prefix_len_sum == packed_bytes_length_) {
                write_leaf_block_docs_bitmap(out, heap_source->doc_IDs_, source->start_, count);
            } else {
                low_cardinal = leaf_node_is_low_cardinality(common_prefix_lengths_, count, sorted_dim, packed_values, leaf_cardinality, prefix_len_sum);
                write_leaf_block_docs(out, heap_source->doc_IDs_, source->start_, count);
            }

            write_common_prefixes(out, common_prefix_lengths_, scratch1_);

            write_leaf_block_packed_values(out, common_prefix_lengths_, count, sorted_dim, packed_values, prefix_len_sum, low_cardinal);
        } else {
            int32_t split_dim;
            if (num_index_dims_ > 1) {
                //TODO
            } else {
                split_dim = 0;
            }

            bool delete_split_dim = false;
            if (slices[split_dim] == nullptr) {
                create_path_slice(slices, split_dim);
                delete_split_dim = true;
            }
            PathSlicePtr source = slices[split_dim];

            assert(nodeID < (int32_t) splitPackedValues.size());

            int64_t right_count = source->count_ / 2;
            int64_t left_count = source->count_ - right_count;

            int32_t dim_to_clear = num_index_dims_ - 1;
            while (dim_to_clear >= 0) {
                if (slices[dim_to_clear] != nullptr && split_dim != dim_to_clear) {
                    break;
                }
                dim_to_clear--;
            }

            auto split_value = (dim_to_clear == -1) ? mark_right_tree(right_count, split_dim, source, nullptr) : mark_right_tree(right_count, split_dim, source, ordBitSet);
            if (split_value == nullptr) {
                _CLTHROWA(CL_ERR_NullPointer, "split value pointer is null.");
            }
            int32_t address = nodeID * (1 + bytes_per_dim_);
            splitPackedValues[address] = (uint8_t) split_dim;
            std::copy(split_value, split_value + bytes_per_dim_, splitPackedValues.begin() + address + 1);

            std::vector<PathSlicePtr> left_slices(num_index_dims_);
            std::vector<PathSlicePtr> right_slices(num_index_dims_);

            std::vector<uint8_t> min_split_packed_value(packed_index_bytes_length_);
            std::copy(minPackedValue.begin(), minPackedValue.begin() + packed_index_bytes_length_, min_split_packed_value.begin());

            std::vector<uint8_t> max_split_packed_value(packed_index_bytes_length_);
            std::copy(maxPackedValue.begin(), maxPackedValue.begin() + packed_index_bytes_length_, max_split_packed_value.begin());


            for (int32_t dim = 0; dim < num_index_dims_; dim++) {
                if (slices[dim] == nullptr) {
                    continue;
                }
                if (dim == split_dim) {
                    left_slices[dim] = std::make_shared<path_slice>(source->writer_, source->start_, left_count);
                    right_slices[dim] = std::make_shared<path_slice>(source->writer_, source->start_ + left_count, right_count);
                    std::copy(split_value, split_value + bytes_per_dim_, min_split_packed_value.begin() + dim * bytes_per_dim_);
                    std::copy(split_value, split_value + bytes_per_dim_, max_split_packed_value.begin() + dim * bytes_per_dim_);
                    continue;
                }

                std::shared_ptr<point_reader> reader = slices[dim]->writer_->get_shared_reader(slices[dim]->start_, slices[dim]->count_, toCloseHeroically);
                std::string desc = "left" + std::to_string(dim);
                std::shared_ptr<point_writer> left_point_writer = get_point_writer(left_count, desc);
                if (left_point_writer != nullptr) {
                    desc = "right" + std::to_string(dim);
                    std::shared_ptr<point_writer> right_point_writer = get_point_writer(source->count_ - left_count, desc);

                    int64_t next_right_count = reader->split(source->count_, ordBitSet, left_point_writer, right_point_writer, dim == dim_to_clear);
                    if (right_count != next_right_count)
                        _CLTHROWA(CL_ERR_IllegalState, ("wrong number of points in split: expected=" + std::to_string(right_count) + " but actual=" + std::to_string(next_right_count)).c_str());

                    left_slices[dim] = std::make_shared<path_slice>(left_point_writer, 0, left_count);
                    right_slices[dim] = std::make_shared<path_slice>(right_point_writer, 0, right_count);
                    left_point_writer->close();
                    right_point_writer->close();
                }
            }

            parentSplits[split_dim]++;
            build(2 * nodeID, leafNodeOffset, left_slices,
                  ordBitSet, out,
                  minPackedValue, max_split_packed_value, parentSplits,
                  splitPackedValues, leafBlockFPs, toCloseHeroically);
            for (int32_t dim = 0; dim < num_index_dims_; dim++) {
                if (dim != split_dim && slices[dim] != nullptr) {
                    left_slices[dim]->writer_->destroy();
                }
            }

            build(2 * nodeID + 1, leafNodeOffset, right_slices,
                  ordBitSet, out,
                  min_split_packed_value, maxPackedValue, parentSplits,
                  splitPackedValues, leafBlockFPs, toCloseHeroically);
            for (int32_t dim = 0; dim < num_index_dims_; dim++) {
                if (dim != split_dim && slices[dim] != nullptr) {
                    right_slices[dim]->writer_->destroy();
                }
            }
            parentSplits[split_dim]--;
            if (delete_split_dim) {
                slices[split_dim]->writer_->destroy();
            }
        }
    }
    void bkd_writer::create_path_slice(std::vector<std::shared_ptr<path_slice>> &slices, int32_t dim) {
        assert(slices[dim] == nullptr);
        std::shared_ptr<path_slice> current = nullptr;
        for (const std::shared_ptr<path_slice> &slice: slices) {
            if (slice != nullptr) {
                current = slice;
                break;
            }
        }
        if (current == nullptr) {
            slices[dim] = std::make_shared<path_slice>(sort(dim), 0, point_count_);
        } else {
            slices[dim] = std::make_shared<path_slice>(sort(dim, current->writer_, current->start_, current->count_), 0, current->count_);
        }
    }

    std::shared_ptr<heap_point_writer> bkd_writer::create_heap_point_writer_copy(const std::shared_ptr<heap_point_writer> &writer, int64_t start, int64_t count) {
        int32_t size = count;
        std::shared_ptr<heap_point_writer> point_writer = std::make_shared<heap_point_writer>(size, size, packed_bytes_length_, long_ords_, single_value_per_doc_);
        std::shared_ptr<point_reader> reader = writer->get_reader(start, count);
        if (point_writer != nullptr && reader != nullptr) {
            for (int64_t i = 0; i < count; i++) {
                reader->next();
                point_writer->append(reader->packed_value_raw(), packed_bytes_length_, reader->ord(), reader->docid());
            }
            return point_writer;
        }
        _CLTHROWA(CL_ERR_CorruptIndex, "point index corrupt.");
    }


    std::shared_ptr<point_writer> bkd_writer::sort(int32_t dim, const std::shared_ptr<point_writer> &writer, int64_t start, int64_t point_count) {
        assert(dim >= 0 && dim < num_data_dims_);
        auto w = std::dynamic_pointer_cast<heap_point_writer>(writer);
        if (w != nullptr) {
            std::shared_ptr<heap_point_writer> heap_point_writer = create_heap_point_writer_copy(w, start, point_count);
            sort_heap_point_writer(heap_point_writer, point_count, dim);
            return heap_point_writer;
        }
        return nullptr;
    }

    // radix sort by value
    std::shared_ptr<point_writer> bkd_writer::sort(int32_t dim) {
        assert(dim >= 0 && dim < num_data_dims_);
        if (heap_point_writer_ != nullptr) {
            std::shared_ptr<heap_point_writer> sorted = std::move(heap_point_writer_);
            sort_heap_point_writer(sorted, point_count_, dim);
            sorted->close();
            heap_point_writer_ = nullptr;
            return std::dynamic_pointer_cast<point_writer>(sorted);
        }
        return nullptr;
    }

    void bkd_writer::sort_heap_point_writer(std::shared_ptr<heap_point_writer> &writer, int32_t pointCount, int32_t dim) {
        auto sorter = std::make_shared<bkd_msb_radix_sorter>(shared_from_this().get(), writer.get(), dim, bytes_per_dim_ + 4);
        sorter->sort(0, pointCount);
    }

}// namespace bkd
CL_NS_END
