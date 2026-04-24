#pragma once
#include "../../common.hpp"
#include "../block/metadata_block.hpp"

class RowGroup {
public:
    RowGroup(uint64_t, uint64_t);
    void ReadDataBlocks(uint64_t, Reader&);
    uint64_t GetDataPointerCount();
    MetadataBlock GetMetaBlock(idx_t i);
private:
    uint64_t row_start_;
    uint64_t tuple_count_;
    vector<MetadataBlock> data_blocks_;
};
