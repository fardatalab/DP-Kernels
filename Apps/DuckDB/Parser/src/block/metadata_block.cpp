#include "../include/block/metadata_block.hpp"

MetadataBlock::MetadataBlock(idx_t ptr, idx_t offset) : block_offset(offset) {
    block_id = (ptr & ~((idx_t)(0xff) << 56ull));
    block_index = (ptr >> 56ull);
}

idx_t MetadataBlock::GetBlockId() {
    return block_id;
}

idx_t MetadataBlock::GetBlockIndex() {
    return block_index;
}

idx_t MetadataBlock::GetBlockOffset() {
    return block_offset;
}

MetadataBlock MetadataBlock::Deserialize(field_id_t field_id, Reader& reader) {
    assert(reader.Read<field_id_t>() == field_id);
    return Deserialize(reader);
}

MetadataBlock MetadataBlock::Deserialize(Reader& reader) {
    idx_t block_ptr, block_offset;
    if (reader.ReadEncoded<idx_t>(100, &block_ptr)) {
        // set to default pointer
        block_ptr = static_cast<idx_t>(-1);
    }

    reader.ReadEncoded<idx_t>(101, &block_offset);
    MetadataBlock block;
    block.block_id = (block_ptr & ~((idx_t)(0xff) << 56ull));
    block.block_index = (block_ptr >> 56ull);
    block.block_offset = block_offset;
    auto val = reader.Read<field_id_t>();
//    printf("[MetadataBlock::Deserialize] val=%d\n", val);
    assert(val == OBJECT_END);
    return block;
}
