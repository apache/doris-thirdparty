#include "bkd_reader.h"
#include "bkd_writer.h"
#include "legacy_index_tree.h"

CL_NS_DEF2(util,bkd)
legacy_index_tree::legacy_index_tree(std::shared_ptr<bkd_reader>&& reader)
    : index_tree(reader) {
    split_dim_value_ = std::make_shared<std::vector<uint8_t>>(reader->bytes_per_dim_);
    scratch_ = std::make_shared<BytesRef>();
    set_node_data();
    scratch_->bytes = *split_dim_value_;
    scratch_->length = reader->bytes_per_dim_;
}

std::shared_ptr<index_tree> legacy_index_tree::clone() {
    auto index = std::make_shared<legacy_index_tree>(std::move(reader));
    index->node_id_ = node_id_;
    index->level_ = level_;
    index->split_dim_ = split_dim_;
    index->leaf_block_fp_ = leaf_block_fp_;
    index->split_packed_value_stack_[index->level_] = split_packed_value_stack_[index->level_];

    return index;
}

void legacy_index_tree::push_left() {
    index_tree::push_left();
    set_node_data();
}

void legacy_index_tree::push_right() {
    index_tree::push_right();
    set_node_data();
}

int64_t legacy_index_tree::get_leaf_blockFP() {
    assert(is_leaf_node());
    return leaf_block_fp_;
}

std::shared_ptr<BytesRef> legacy_index_tree::get_split_dim_value() {
    assert(is_leaf_node() == false);
    return scratch_;
}

std::vector<uint8_t>& legacy_index_tree::get_split_1dim_value() {
    assert(is_leaf_node() == false);
    return scratch_->bytes;
}

void legacy_index_tree::pop() {
    index_tree::pop();
    leaf_block_fp_ = -1;
}

void legacy_index_tree::set_node_data() {
    if (is_leaf_node()) {
        leaf_block_fp_ = reader->leaf_block_fps_[node_id_ - reader->leaf_node_offset_];
        split_dim_ = -1;
    } else {
        leaf_block_fp_ = -1;
        int32_t address = node_id_ * reader->bytes_per_index_entry_;
        if (reader->num_index_dims_ == 1) {
            split_dim_ = 0;
            if (reader->version_ < bkd_writer::VERSION_IMPLICIT_SPLIT_DIM_1D) {
                assert(reader->split_packed_values_[address] == 0);
                address++;
            }
        } else {
            split_dim_ = reader->split_packed_values_[address++] & 0xff;
        }
        std::copy(reader->split_packed_values_.begin() + address,
                  reader->split_packed_values_.begin() + address + reader->bytes_per_dim_,
                  split_dim_value_->begin());
    }
}


CL_NS_END2