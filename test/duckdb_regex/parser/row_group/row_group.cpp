#include "../include/row_group/row_group.hpp"

RowGroup::RowGroup(uint64_t row_start, uint64_t tuple_count) : row_start_(row_start), tuple_count_(tuple_count) {}

void RowGroup::ReadDataBlocks(uint64_t num_data_blocks, Reader& reader) {
    for (uint64_t i = 0; i < num_data_blocks; i++) {
        MetadataBlock block = MetadataBlock::Deserialize(reader);
        // printf("block_id=%llu, block_index=%llu, block_offset=%llu\n", block.GetBlockId(), block.GetBlockIndex(), block.GetBlockOffset());
        data_blocks_.push_back(block);
    }
}

uint64_t RowGroup::GetDataPointerCount() {
    return data_blocks_.size();
}

MetadataBlock RowGroup::GetMetaBlock(idx_t i) {
    return data_blocks_[i];
}