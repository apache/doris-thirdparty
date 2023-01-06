#pragma once

#include "CLucene/StdHeader.h"

#include "CLucene/store/IndexInput.h"
#include "CLucene/util/croaring/roaring.hh"
#include "bkd_docid_iterator.h"
#include "index_tree.h"

#include <memory>
#include <vector>

CL_NS_DEF2(util, bkd)

enum class relation {
    /** Return this if the cell is fully contained by the query */
    CELL_INSIDE_QUERY,
    /** Return this if the cell and query do not overlap */
    CELL_OUTSIDE_QUERY,
    /** Return this if the cell partially overlaps the query */
    CELL_CROSSES_QUERY
};


class bkd_reader : public std::enable_shared_from_this<bkd_reader> {

public:
    int32_t leaf_node_offset_{};
    int32_t num_data_dims_{};
    int32_t num_index_dims_{};
    int32_t bytes_per_dim_{};
    int32_t num_leaves_{};
    std::unique_ptr<store::IndexInput> in_;
    int32_t max_points_in_leaf_node_{};
    std::vector<uint8_t> min_packed_value_;
    std::vector<uint8_t> max_packed_value_;
    int64_t point_count_{};
    int32_t doc_count_{};
    int32_t version_{};
    std::shared_ptr<std::vector<uint8_t>> packed_index_;
    std::shared_ptr<store::IndexInput> clone_index_input;
    int32_t bytes_per_index_entry_{};
    std::vector<int64_t> leaf_block_fps_;

    int32_t packed_bytes_length_{};
    int32_t packed_index_bytes_length_{};
    std::vector<uint8_t> split_packed_values_;
    int32_t type{};
    int64_t metaOffset{};
    int64_t indexFP{};

public:
    class intersect_visitor {
    public:
        virtual void visit(int docID) = 0;

        /** Called for all documents in a leaf cell that crosses the query.  The
         *  consumer should scrutinize the packedValue to decide whether to accept
         *  it.  In the 1D case, values are visited in increasing order, and in the
         *  case of ties, in increasing docID order.
         */
        virtual void visit(int docID, std::vector<uint8_t> &packedValue) = 0;
        virtual void visit(Roaring &docID) = 0;
        virtual void visit(Roaring &&docID) = 0;
        virtual void visit(bkd_docID_set_iterator *iter, std::vector<uint8_t> &packedValue) = 0;
        virtual void visit(Roaring *docID, std::vector<uint8_t> &packedValue) = 0;
        virtual void visit(std::vector<char>& docID, std::vector<uint8_t> &packedValue) = 0;

        /** Called for non-leaf cells to test how the cell relates to the query, to
         *  determine how to further recurse down the tree. */
        virtual relation compare(std::vector<uint8_t> &minPackedValue,
                                 std::vector<uint8_t> &maxPackedValue) = 0;
        void grow(int count){};

        virtual void inc_hits(int count) {}

        virtual bool only_hits() { return false; }
    };
    class intersect_state final {
    public:
        intersect_state(store::IndexInput *in,
                        int32_t numDims,
                        int32_t packedBytesLength,
                        int32_t packedIndexBytesLength,
                        int32_t maxPointsInLeafNode,
                        bkd_reader::intersect_visitor *visitor,
                        const std::shared_ptr<index_tree> &indexVisitor);

    public:
        std::shared_ptr<store::IndexInput> in_;
        std::unique_ptr<bkd_docID_set_iterator> docID_set_iterator;
        std::vector<int32_t> scratch_doc_ids_;
        std::vector<uint8_t> scratch_data_packed_value_;
        std::vector<uint8_t> scratch_min_index_packed_value_;
        std::vector<uint8_t> scratch_max_index_packed_value_;
        std::vector<int32_t> common_prefix_lengths_;

        bkd_reader::intersect_visitor *visitor_;
        std::shared_ptr<index_tree> index_;
    };

public:
    int32_t get_tree_depth() const;
    void add_all(const std::shared_ptr<intersect_state> &state, bool grown);
    void visit_doc_ids(store::IndexInput *in, int64_t blockFP, bkd_reader::intersect_visitor *visitor) const;
    int32_t read_doc_ids(store::IndexInput *in, int64_t blockFP, bkd_docID_set_iterator *iter) const;
    //int32_t VisitDocValues(const std::shared_ptr<store::IndexInput>& in, int64_t blockFP, std::vector<int32_t>& docIDs);
    void visit_doc_values(std::vector<int32_t> &commonPrefixLengths,
                          std::vector<uint8_t> &scratchDataPackedValue,
                          const std::vector<uint8_t> &scratchMinIndexPackedValue,
                          const std::vector<uint8_t> &scratchMaxIndexPackedValue,
                          store::IndexInput *in, bkd_docID_set_iterator *iter,
                          int32_t count, bkd_reader::intersect_visitor *visitor);
    void read_common_prefixes(std::vector<int32_t> &commonPrefixLengths,
                              std::vector<uint8_t> &scratchPackedValue, store::IndexInput *in) const;

    void read_min_max(const std::vector<int32_t> &commonPrefixLengths, std::vector<uint8_t> &minPackedValue,
                      std::vector<uint8_t> &maxPackedValue, store::IndexInput *in) const;
    int32_t read_compressed_dim(store::IndexInput *in) const;
    void visit_compressed_doc_values(std::vector<int32_t> &commonPrefixLengths,
                                     std::vector<uint8_t> &scratchPackedValue,
                                     store::IndexInput *in,
                                     bkd_docID_set_iterator *iter,
                                     int32_t count,
                                     bkd_reader::intersect_visitor *visitor,
                                     int32_t compressedDim) const;
    void visit_raw_doc_values(const std::vector<int32_t> &commonPrefixLengths,
                              std::vector<uint8_t> &scratchPackedValue,
                              store::IndexInput *in,
                              bkd_docID_set_iterator *iter,
                              int32_t count,
                              bkd_reader::intersect_visitor *visitor) const;
    void visit_unique_raw_doc_values(std::vector<uint8_t> &scratchPackedValue,
                                     bkd_docID_set_iterator *iter,
                                     int32_t count,
                                     bkd_reader::intersect_visitor *visitor) const;
    void visit_sparse_raw_doc_values(const std::vector<int32_t> &commonPrefixLengths,
                                     std::vector<uint8_t> &scratchPackedValue,
                                     store::IndexInput *in,
                                     bkd_docID_set_iterator *iter,
                                     int32_t count,
                                     bkd_reader::intersect_visitor *visitor) const;

public:
    bkd_reader() = default;
    void read_index(store::IndexInput* index_in);
    int read_meta(store::IndexInput* meta_in);
    explicit bkd_reader(store::IndexInput *in);
    int64_t estimate_point_count(bkd_reader::intersect_visitor *visitor);
    int64_t estimate_point_count(const std::shared_ptr<intersect_state> &s,
                                 std::vector<uint8_t> &cellMinPacked,
                                 std::vector<uint8_t> &cellMaxPacked);
    void intersect(bkd_reader::intersect_visitor *visitor);
    void intersect(const std::shared_ptr<intersect_state> &s,
                   std::vector<uint8_t> &cellMinPacked,
                   std::vector<uint8_t> &cellMaxPacked);
    std::shared_ptr<intersect_state> get_intersect_state(bkd_reader::intersect_visitor *visitor);

private:
    int64_t ram_bytes_used();

public:
    struct reader_stats {
    private:
        uint64_t visit_doc_values_time_duration_ms{0};
        uint64_t visit_uniq_doc_values_time_duration_ms{0};
        uint64_t visit_sparse_doc_values_time_duration_ms{0};
        uint64_t visit_compress_doc_values_time_duration_ms{0};
        uint64_t visit_doc_id_time_duration_ms{0};
        uint64_t read_doc_id_time_duration_ms{0};
        uint64_t visit_compare_time_duration_ms{0};

    public:
        void set_doc_value_visit_time_duration(uint64_t time_duration) { visit_doc_values_time_duration_ms = time_duration; }
        void add_doc_value_visit_time_duration(uint64_t time_duration) { visit_doc_values_time_duration_ms += time_duration; }
        uint64_t get_doc_value_visit_time_duration() const { return visit_doc_values_time_duration_ms; }

        void set_uniq_doc_value_visit_time_duration(uint64_t time_duration) { visit_uniq_doc_values_time_duration_ms = time_duration; }
        void add_uniq_doc_value_visit_time_duration(uint64_t time_duration) { visit_uniq_doc_values_time_duration_ms += time_duration; }
        uint64_t get_uniq_doc_value_visit_time_duration() const { return visit_uniq_doc_values_time_duration_ms; }

        void set_sparse_doc_value_visit_time_duration(uint64_t time_duration) { visit_sparse_doc_values_time_duration_ms = time_duration; }
        void add_sparse_doc_value_visit_time_duration(uint64_t time_duration) { visit_sparse_doc_values_time_duration_ms += time_duration; }
        uint64_t get_sparse_doc_value_visit_time_duration() const { return visit_sparse_doc_values_time_duration_ms; }

        void set_compress_doc_value_visit_time_duration(uint64_t time_duration) { visit_compress_doc_values_time_duration_ms = time_duration; }
        void add_compress_doc_value_visit_time_duration(uint64_t time_duration) { visit_compress_doc_values_time_duration_ms += time_duration; }
        uint64_t get_compress_doc_value_visit_time_duration() const { return visit_compress_doc_values_time_duration_ms; }

        void set_doc_id_visit_time_duration(uint64_t time_duration) { visit_doc_id_time_duration_ms = time_duration; }
        void add_doc_id_visit_time_duration(uint64_t time_duration) { visit_doc_id_time_duration_ms += time_duration; }
        uint64_t get_doc_id_visit_time_duration() const { return visit_doc_id_time_duration_ms; }

        void set_read_doc_id_time_duration(uint64_t time_duration) { read_doc_id_time_duration_ms = time_duration; }
        void add_read_doc_id_time_duration(uint64_t time_duration) { read_doc_id_time_duration_ms += time_duration; }
        uint64_t get_read_doc_id_time_duration() const { return read_doc_id_time_duration_ms; }

        void set_visit_compare_time_duration(uint64_t time_duration) { visit_compare_time_duration_ms = time_duration; }
        void add_visit_compare_time_duration(uint64_t time_duration) { visit_compare_time_duration_ms += time_duration; }
        uint64_t get_visit_compare_time_duration() const { return visit_compare_time_duration_ms; }

        std::string to_string() const {
            return "| visit compare time: " + std::to_string(visit_compare_time_duration_ms) + "ms " +
                   "| read doc id time: " + std::to_string(read_doc_id_time_duration_ms) + "ms " +
                   "| visit doc id time: " + std::to_string(visit_doc_id_time_duration_ms) + "ms " +
                   "| visit doc value time: " + std::to_string(visit_doc_values_time_duration_ms) + "ms " +
                   "| visit unique doc value time: " + std::to_string(visit_uniq_doc_values_time_duration_ms) + "ms " +
                   "| visit sparse doc value time: " + std::to_string(visit_sparse_doc_values_time_duration_ms) + "ms " +
                   "| visit compress doc value time: " + std::to_string(visit_compress_doc_values_time_duration_ms) + "ms "
                    ;
        }
    };
    reader_stats stats;
};
CL_NS_END2
