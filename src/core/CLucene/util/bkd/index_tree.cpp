#include "bkd_reader.h"
#include "index_tree.h"

CL_NS_DEF2(util,bkd)

index_tree::index_tree(std::shared_ptr<bkd_reader>& reader)
    : reader(reader) {
    int32_t treeDepth = reader->get_tree_depth();
    split_packed_value_stack_.resize(treeDepth + 1);
    node_id_ = 1;
    level_ = 1;
    split_packed_value_stack_[level_].resize(reader->packed_index_bytes_length_);
}

void index_tree::push_left() {
    node_id_ *= 2;
    level_++;
    if (split_packed_value_stack_[level_].empty()) {
        split_packed_value_stack_[level_].resize(reader->packed_index_bytes_length_);
    }
}

void index_tree::push_right() {
    node_id_ = node_id_ * 2 + 1;
    level_++;
    if (split_packed_value_stack_[level_].empty()) {
        split_packed_value_stack_[level_].resize(reader->packed_index_bytes_length_);
    }
}

void index_tree::pop() {
    node_id_ /= 2;
    level_--;
    split_dim_ = -1;
}

bool index_tree::is_leaf_node() {
    return node_id_ >= reader->leaf_node_offset_;
}

bool index_tree::node_exists() {
    return node_id_ - reader->leaf_node_offset_ < reader->leaf_node_offset_;
}

int32_t index_tree::get_node_id() {
    return node_id_;
}

std::vector<uint8_t>&index_tree::get_split_packed_value() {
    assert(is_leaf_node() == false);
    assert(!split_packed_value_stack_[level_].empty());
    return split_packed_value_stack_[level_];
}

int32_t index_tree::get_split_dim() {
    assert(is_leaf_node() == false);
    return split_dim_;
}

int32_t index_tree::get_num_leaves() {
    int32_t leftMostLeafNode = node_id_;
    while (leftMostLeafNode < reader->leaf_node_offset_) {
        leftMostLeafNode = leftMostLeafNode * 2;
    }
    int32_t rightMostLeafNode = node_id_;
    while (rightMostLeafNode < reader->leaf_node_offset_) {
        rightMostLeafNode = rightMostLeafNode * 2 + 1;
    }
    int32_t numLeaves;
    if (rightMostLeafNode >= leftMostLeafNode) {
        numLeaves = rightMostLeafNode - leftMostLeafNode + 1;
    } else {
        numLeaves = rightMostLeafNode - leftMostLeafNode + 1 + reader->leaf_node_offset_;
    }
    assert(numLeaves == GetNumLeavesSlow(node_id_));
    return numLeaves;
}

int32_t index_tree::GetNumLeavesSlow(int32_t node) {
    if (node >= 2 * reader->leaf_node_offset_) {
        return 0;
    } else if (node >= reader->leaf_node_offset_) {
        return 1;
    } else {
        int32_t leftCount = GetNumLeavesSlow(node * 2);
        int32_t rightCount = GetNumLeavesSlow(node * 2 + 1);
        return leftCount + rightCount;
    }
}

CL_NS_END2