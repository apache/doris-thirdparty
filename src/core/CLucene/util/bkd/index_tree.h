#pragma once

#include "CLucene/util/BytesRef.h"

#include <memory>
#include <vector>

CL_NS_DEF2(util,bkd)
class bkd_reader;
class index_tree {
protected:
    explicit index_tree(std::shared_ptr<bkd_reader>& reader);

public:
    virtual ~index_tree() = default;
    virtual void push_left();
    virtual void push_right();
    virtual void pop();
    virtual bool is_leaf_node();
    virtual bool node_exists();
    virtual int32_t get_node_id();
    virtual std::vector<uint8_t>&get_split_packed_value();
    virtual int32_t get_split_dim();
    virtual int32_t get_num_leaves();
    virtual std::shared_ptr<index_tree> clone() = 0;
    virtual std::shared_ptr<BytesRef> get_split_dim_value() = 0;
    virtual std::vector<uint8_t>& get_split_1dim_value() = 0;
    virtual int64_t get_leaf_blockFP() = 0;
private:
    int32_t GetNumLeavesSlow(int32_t node);

protected:
    int32_t node_id_ = 0;
    int32_t level_ = 0;
    int32_t split_dim_ = 0;
    std::vector<std::vector<uint8_t>> split_packed_value_stack_;

protected:
    std::shared_ptr<bkd_reader> reader;
};

CL_NS_END2