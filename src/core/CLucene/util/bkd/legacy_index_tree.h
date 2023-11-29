#pragma once

#include "index_tree.h"

#include <memory>
#include <vector>

CL_NS_DEF2(util,bkd)
class legacy_index_tree : public index_tree {
public:
    explicit legacy_index_tree(std::shared_ptr<bkd_reader>&& reader);

    std::shared_ptr<index_tree> clone() override;
    void push_left() override;
    void push_right() override;
    void pop() override;
    int64_t get_leaf_blockFP() override;
    std::shared_ptr<BytesRef> get_split_dim_value() override;
    std::vector<uint8_t>& get_split_1dim_value() override;

private:
    void set_node_data();

private:
    int64_t leaf_block_fp_ = 0;
    std::shared_ptr<std::vector<uint8_t>> split_dim_value_;
    std::shared_ptr<BytesRef> scratch_{};
};

CL_NS_END2