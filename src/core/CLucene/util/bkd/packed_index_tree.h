#pragma once
#include "CLucene/_ApiHeader.h"

#include "index_tree.h"
#include "CLucene/store/IndexInput.h"

#include <memory>
#include <vector>

CL_CLASS_DEF(store, ByteArrayDataInput)
CL_NS_DEF2(util,bkd)
class packed_index_tree : public index_tree {
public:
    explicit packed_index_tree(std::shared_ptr<bkd_reader>&& reader);

    std::shared_ptr<index_tree> clone() override;
    void push_left() override;
    void push_right() override;
    void pop() override;
    int64_t get_leaf_blockFP() override;
    std::shared_ptr<BytesRef> get_split_dim_value() override;
    std::vector<uint8_t>& get_split_1dim_value() override;

private:
    void read_node_data(bool isLeft);

private:
    std::unique_ptr<store::ByteArrayDataInput> in_;
    std::vector<int64_t> leaf_block_fp_stack_;
    std::vector<int32_t> left_node_positions_;
    std::vector<int32_t> right_node_positions_;
    std::vector<int32_t> split_dims_;
    std::vector<bool> negative_deltas_;
    std::vector<std::vector<uint8_t>> split_values_stack_;
    std::shared_ptr<BytesRef> scratch_;
};

CL_NS_END2