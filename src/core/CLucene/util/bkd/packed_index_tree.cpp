#include "bkd_reader.h"
#include "packed_index_tree.h"

CL_NS_DEF2(util,bkd)

packed_index_tree::packed_index_tree(std::shared_ptr<bkd_reader>&& reader)
    : index_tree(reader) {
    int32_t treeDepth = reader->get_tree_depth();
    leaf_block_fp_stack_ = std::vector<int64_t>(treeDepth + 1);
    left_node_positions_ = std::vector<int32_t>(treeDepth + 1);
    right_node_positions_ = std::vector<int32_t>(treeDepth + 1);
    split_values_stack_ = std::vector<std::shared_ptr<std::vector<uint8_t>>>(treeDepth + 1);
    split_dims_ = std::vector<int32_t>(treeDepth + 1);
    negative_deltas_ = std::vector<bool>(reader->num_index_dims_ * (treeDepth + 1));

    //in_ = std::make_shared<store::ByteArrayDataInput>(reader->packed_index_);
    //in_ = std::move(reader->clone_index_input);
    in_ = reader->clone_index_input;
    split_values_stack_[0] = std::make_shared<std::vector<uint8_t>>(reader->packed_index_bytes_length_);
    read_node_data(false);
    scratch_ = std::make_shared<BytesRef>();
    scratch_->length = reader->bytes_per_dim_;
}

std::shared_ptr<index_tree> packed_index_tree::clone() {
    std::shared_ptr<packed_index_tree> index = std::make_shared<packed_index_tree>(std::move(reader));
    index->node_id_ = node_id_;
    index->level_ = level_;
    index->split_dim_ = split_dim_;
    index->leaf_block_fp_stack_[level_] = leaf_block_fp_stack_[level_];
    index->left_node_positions_[level_] = left_node_positions_[level_];
    index->right_node_positions_[level_] = right_node_positions_[level_];
    index->split_values_stack_[index->level_] = split_values_stack_[index->level_];
    std::copy(negative_deltas_.begin() + level_ * reader->num_index_dims_,
              negative_deltas_.begin() + level_ * reader->num_index_dims_ + reader->num_index_dims_,
              index->negative_deltas_.begin() + level_ * reader->num_index_dims_);
    index->split_dims_[level_] = split_dims_[level_];
    return index;
}

void packed_index_tree::push_left() {
    int32_t nodePosition = left_node_positions_[level_];
    index_tree::push_left();
    std::copy(negative_deltas_.begin() + (level_ - 1) * reader->num_index_dims_,
              negative_deltas_.begin() + (level_ - 1) * reader->num_index_dims_ + reader->num_index_dims_,
              negative_deltas_.begin() + level_ * reader->num_index_dims_);
    assert(split_dim_ != -1);
    negative_deltas_[level_ * reader->num_index_dims_ + split_dim_] = true;
    //in_->setPosition(nodePosition);
    in_->seek(nodePosition);
    read_node_data(true);
}

void packed_index_tree::push_right() {
    int32_t nodePosition = right_node_positions_[level_];
    index_tree::push_right();
    std::copy(negative_deltas_.begin() + (level_ - 1) * reader->num_index_dims_,
              negative_deltas_.begin() + (level_ - 1) * reader->num_index_dims_ + reader->num_index_dims_,
              negative_deltas_.begin() + level_ * reader->num_index_dims_);
    assert(split_dim_ != -1);
    negative_deltas_[level_ * reader->num_index_dims_ + split_dim_] = false;
    //in_->setPosition(nodePosition);
    in_->seek(nodePosition);
    read_node_data(false);
}

void packed_index_tree::pop() {
    index_tree::pop();
    split_dim_ = split_dims_[level_];
}

int64_t packed_index_tree::get_leaf_blockFP() {
    assert(is_leaf_node());
    return leaf_block_fp_stack_[level_];
}

std::shared_ptr<BytesRef> packed_index_tree::get_split_dim_value() {
    assert(is_leaf_node() == false);
    // TODO: double free warning here, need too figure it out.
    scratch_->bytes = *split_values_stack_[level_];
    scratch_->offset = split_dim_ * reader->bytes_per_dim_;
    return scratch_;
}

void packed_index_tree::read_node_data(bool isLeft) {
    leaf_block_fp_stack_[level_] = leaf_block_fp_stack_[level_ - 1];

    if (!isLeft) {
        leaf_block_fp_stack_[level_] += in_->readVLong();
    }

    if (is_leaf_node()) {
        split_dim_ = -1;
    } else {
        int32_t code = in_->readVInt();
        split_dim_ = code % reader->num_index_dims_;
        split_dims_[level_] = split_dim_;
        code /= reader->num_index_dims_;
        int32_t prefix = code % (1 + reader->bytes_per_dim_);
        int32_t suffix = reader->bytes_per_dim_ - prefix;

        if (split_values_stack_[level_]==nullptr) {
            split_values_stack_[level_] = std::make_shared<std::vector<uint8_t>>(reader->packed_index_bytes_length_);
        }
        std::copy(split_values_stack_[level_ - 1]->begin(),
                  split_values_stack_[level_ - 1]->begin() + reader->packed_index_bytes_length_,
                  split_values_stack_[level_]->begin());
        if (suffix > 0) {
            int32_t firstDiffByteDelta = code / (1 + reader->bytes_per_dim_);
            if (negative_deltas_[level_ * reader->num_index_dims_ + split_dim_]) {
                firstDiffByteDelta = -firstDiffByteDelta;
            }
            int32_t oldByte = (*split_values_stack_[level_])[split_dim_ * reader->bytes_per_dim_ + prefix] & 0xFF;
            (*split_values_stack_[level_])[split_dim_ * reader->bytes_per_dim_ + prefix] = static_cast<uint8_t>(oldByte + firstDiffByteDelta);
            in_->readBytes((split_values_stack_[level_])->data(),
                           suffix - 1,
                           split_dim_ * reader->bytes_per_dim_ + prefix + 1);
        } else {
            // our split value is == last split value in this dim, which can happen when there are many duplicate values
        }

        int32_t leftNumBytes;
        if (node_id_ * 2 < reader->leaf_node_offset_) {
            leftNumBytes = in_->readVInt();
        } else {
            leftNumBytes = 0;
        }

        //left_node_positions_[level_] = in_->getPosition();
        left_node_positions_[level_] = in_->getFilePointer();

        right_node_positions_[level_] = left_node_positions_[level_] + leftNumBytes;
    }
}

CL_NS_END2