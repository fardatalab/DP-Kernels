#pragma once
#include "../../common.hpp"
#include "../reader/reader.hpp"

class MetadataBlock {
public:
    MetadataBlock() = default;
    MetadataBlock(idx_t, idx_t);
    static MetadataBlock Deserialize(field_id_t, Reader&);
    static MetadataBlock Deserialize(Reader&);
    idx_t GetBlockId();
    idx_t GetBlockIndex();
    idx_t GetBlockOffset();
private:
    idx_t block_id;
    idx_t block_index;
    idx_t block_offset;
};