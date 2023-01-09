#pragma once

#include "CLucene/StdHeader.h"

#include "CLucene/store/IndexInput.h"
#include "CLucene/store/IndexOutput.h"
#include "CLucene/util/FixedBitSet.h"
#include "CLucene/util/LongBitSet.h"
#include "CLucene/util/OfflineSorter.h"

#include "CLucene/store/Directory.h"
#include "CLucene/store/IndexOutput.h"

#include "heap_point_writer.h"

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

//namespace lucene { namespace store{ class RAMOutputStream; } }
CL_CLASS_DEF(store, RAMOutputStream)
CL_NS_DEF(util)

namespace bkd {
    template<typename T>
    using IntFunction = std::function<T(int)>;

    class bkd_writer : public std::enable_shared_from_this<bkd_writer> {
    public:
        static const int MAX_ARRAY_LENGTH = std::numeric_limits<std::int32_t>::max() - 1024;
        static const int MAX_DIMS = 8;
        static const std::wstring CODEC_NAME;
        static const int VERSION_START = 0;
        static const int VERSION_COMPRESSED_DOC_IDS = 1;
        static const int VERSION_COMPRESSED_VALUES = 2;
        static const int VERSION_IMPLICIT_SPLIT_DIM_1D = 3;
        static const int VERSION_PACKED_INDEX = 4;
        static const int VERSION_LEAF_STORES_BOUNDS = 5;
        static const int VERSION_SELECTIVE_INDEXING = 6;
        static const int VERSION_CURRENT = VERSION_SELECTIVE_INDEXING;

    public:
        int32_t bytes_per_doc_;
        int32_t num_data_dims_;
        /** How many dimensions we are indexing in the internal nodes */
        int32_t num_index_dims_;
        /** How many bytes each value in each dimension takes. */
        int32_t bytes_per_dim_;
        /** numDataDims * bytesPerDim */
        int32_t packed_bytes_length_;
        /** numIndexDims * bytesPerDim */
        int32_t packed_index_bytes_length_;
        std::unique_ptr<store::Directory> temp_dir_;
        double max_mb_sort_in_heap_;
        int32_t max_depth_;
        int32_t max_points_in_leaf_node_;
        int32_t max_points_sort_in_heap_;
        /** Minimum per-dim values, packed */
        std::vector<uint8_t> min_packed_value_;
        /** Maximum per-dim values, packed */
        std::vector<uint8_t> max_packed_value_;
        int64_t point_count_ = 0;
        /** true if we have so many values that we must write ords using long (8 bytes) instead of int (4 bytes) */
        bool long_ords_;
        /** An upper bound on how many points the caller will add (includes deletions) */
        int64_t total_point_count_;
        /** True if every document has at most one value.  We specialize this case by not bothering to store the ord since it's redundant with docid.  */
        bool single_value_per_doc_;
        int32_t max_doc_;
        std::vector<uint8_t> scratch_diff_;
        std::vector<uint8_t> scratch1_;
        std::vector<uint8_t> scratch2_;
        BytesRef scratch_bytes_ref2 = BytesRef();
        std::vector<int32_t> common_prefix_lengths_;
        uint32_t docs_seen_;
        std::shared_ptr<heap_point_writer> heap_point_writer_;

    public:
        class path_slice;

    public:
        bkd_writer(int32_t maxDoc, int32_t numDataDims,
                   int32_t numIndexDims, int32_t bytesPerDim, int32_t maxPointsInLeafNode, double maxMBSortInHeap,
                   int64_t totalPointCount, bool singleValuePerDoc, int32_t maxDepth=8);
        bkd_writer(int32_t maxDoc, int32_t numDataDims,
                   int32_t numIndexDims, int32_t bytesPerDim, int32_t maxPointsInLeafNode, double maxMBSortInHeap,
                   int64_t totalPointCount, bool singleValuePerDoc, bool longOrds, int32_t maxDepth=8);


        static void verify_params(int32_t numDataDims, int32_t numIndexDims, int32_t maxPointsInLeafNode,
                                  double maxMBSortInHeap, int64_t totalPointCount);
        void check_max_leaf_node_count(int32_t num_leaves) const;
        void add(std::vector<uint8_t> &packedValue, int32_t docid);
        void add(const uint8_t *packed_value, uint32_t value_len, int32_t doc_id);
        int64_t finish(store::IndexOutput *out, store::IndexOutput *index_out);
        void build(int32_t nodeID, int32_t leafNodeOffset,
                   std::vector<std::shared_ptr<path_slice>> &slices,
                   const std::shared_ptr<LongBitSet> &ordBitSet,
                   store::IndexOutput *out,
                   std::vector<uint8_t> &minPackedValue, std::vector<uint8_t> &maxPackedValue,
                   std::vector<int32_t> &parentSplits,
                   std::vector<uint8_t> &splitPackedValues,
                   std::vector<int64_t> &leafBlockFPs,
                   const std::vector<std::shared_ptr<point_reader>> &toCloseHeroically);
        void create_path_slice(std::vector<std::shared_ptr<path_slice>> &slices, int32_t dim);
        std::shared_ptr<point_writer> sort(int32_t dim);
        std::shared_ptr<point_writer> sort(int32_t dim, const std::shared_ptr<point_writer> &writer, int64_t start, int64_t point_count);
        void sort_heap_point_writer(std::shared_ptr<heap_point_writer> &writer, int32_t pointCount, int32_t dim);
        std::shared_ptr<heap_point_writer> create_heap_point_writer_copy(const std::shared_ptr<heap_point_writer> &writer, int64_t start, int64_t count);
        std::shared_ptr<path_slice> switch_to_heap(const std::shared_ptr<path_slice> &source, const std::vector<std::shared_ptr<point_reader>> &toCloseHeroically);
        static void write_leaf_block_docs(store::IndexOutput *out, std::vector<int32_t> &docids, int32_t start, int32_t count);
        static void write_leaf_block_docs_bitmap(store::IndexOutput *out, std::vector<int32_t> &docids, int32_t start, int32_t count);
        void write_common_prefixes(store::IndexOutput *out, std::vector<int32_t> &commonPrefixes, std::vector<uint8_t> &packedValue) const;
        void write_common_prefixes(store::IndexOutput *out, std::vector<int32_t> &commonPrefixes, std::vector<uint8_t> &packedValue, int offset) const;
        void write_index(store::IndexOutput *out, int32_t countPerLeaf, std::vector<int64_t> &leafBlockFPs,
                         std::vector<uint8_t> &splitPackedValues);
        void write_index(store::IndexOutput *out, int32_t countPerLeaf, int32_t numLeaves, std::vector<uint8_t> &packedIndex);
        int32_t recurse_pack_index(const std::shared_ptr<store::RAMOutputStream> &writeBuffer, std::vector<int64_t> &leafBlockFPs,
                                   std::vector<uint8_t> &splitPackedValues, int64_t minBlockFP,
                                   ByteArrayList &blocks, int32_t nodeID, std::vector<uint8_t> &lastSplitValues,
                                   std::vector<bool> &negativeDeltas, bool isLeft) const;
        std::shared_ptr<std::vector<uint8_t>> pack_index(std::vector<int64_t> &leaf_block_fps,
                                                         std::vector<uint8_t> &split_packed_values) const;
        static int32_t append_block(const std::shared_ptr<store::RAMOutputStream> &writeBuffer, ByteArrayList &blocks);
        static int64_t get_left_most_leaf_blockFP(std::vector<int64_t> &leafBlockFPs, int32_t nodeID);
        void write_leaf_block_packed_values(store::IndexOutput *out,
                                            std::vector<int32_t> &common_prefix_lengths,
                                            int32_t count, int32_t sorted_dim,
                                            const IntFunction<BytesRef *> &packed_values,
                                            int32_t prefix_len_sum,
                                            bool low_cardinality);
        void write_high_cardinality_leaf_block_packed_values(store::IndexOutput *out, std::vector<int32_t> &common_prefix_lengths,
                                                             int32_t count, int32_t sorted_dim, const IntFunction<BytesRef *> &packed_values,
                                                             int32_t compressed_byte_offset) const;
        void write_low_cardinality_leaf_block_packed_values(store::IndexOutput *out, std::vector<int32_t> &common_prefix_lengths,
                                                            int32_t count, const IntFunction<BytesRef *> &packed_values);
        static int32_t run_len(const IntFunction<BytesRef *> &packedValues, int32_t start, int32_t end, int32_t byteOffset);
        void write_leaf_block_packed_values_range(store::IndexOutput *out, std::vector<int32_t> &commonPrefixLengths,
                                                  int32_t start, int32_t end, const IntFunction<BytesRef *> &packedValues) const;
        int32_t split(std::vector<uint8_t> &min_packed_value, std::vector<uint8_t> &max_packed_value, std::vector<int32_t> &parent_splits);
        const uint8_t *mark_right_tree(int64_t rightCount, int32_t splitDim, std::shared_ptr<path_slice> &source, const std::shared_ptr<LongBitSet> &ordBitSet) const;
        std::shared_ptr<point_writer> get_point_writer(int64_t count, std::string &desc);
        static void meta_finish(store::IndexOutput *out, int64_t fPointer, int32_t type);
        bool leaf_node_is_low_cardinality(std::vector<int32_t> &common_prefix_lengths,
                                          int32_t count, int32_t sorted_dim,
                                          const IntFunction<BytesRef *> &packed_values,
                                          const std::vector<int> &leaf_cardinality,
                                          int32_t prefix_len_sum) const;
    };

    class bkd_writer::path_slice {
    public:
        path_slice(std::shared_ptr<point_writer> writer, int64_t start, int64_t count) : writer_(std::move(writer)),
                                                                                         start_(start),
                                                                                         count_(count) {}

    public:
        std::shared_ptr<point_writer> writer_;
        int64_t start_;
        int64_t count_;
    };
    using PathSlicePtr = std::shared_ptr<bkd_writer::path_slice>;
}// namespace bkd
CL_NS_END
