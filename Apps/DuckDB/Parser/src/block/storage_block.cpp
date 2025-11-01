#include "../include/block/storage_block.hpp"

StorageBlock::StorageBlock(idx_t block_id, idx_t block_offset) : block_id_(block_id), block_offset_(block_offset) {
}

StorageBlock StorageBlock::Deserialize(field_id_t field_id, Reader& reader) {
    auto actual_field_id = reader.Read<field_id_t>();
//    printf("\033[31m[StorageBlock::Deserialize] actual_field_id=%llu, expected_field_id=%llu\033[0m\n", actual_field_id, field_id);
    assert(actual_field_id == field_id);
    StorageBlock block;
    block.block_id_ = reader.ReadEncoded<idx_t>(100);
    block.block_offset_ = reader.ReadEncoded<idx_t>(101);
    assert(reader.Read<field_id_t>() == OBJECT_END);
    return block;
}

//StorageBlock StorageBlock::Deserialize(field_id_t field_id, LinkedListReader& reader) {
//    auto actual_field_id = reader.Read<field_id_t>();
//    // printf("actual_field_id=%llu, expected_field_id=%llu\n", actual_field_id, field_id);
//    assert(actual_field_id == field_id);
//    StorageBlock block;
//    block.block_id_ = reader.ReadEncoded<idx_t>(100);
//    block.block_offset_ = reader.ReadEncoded<idx_t>(101);
//    assert(reader.Read<field_id_t>() == OBJECT_END);
//    return block;
//}

void StorageBlock::operator=(StorageBlock& other) {
    block_id_ = other.block_id_;
    block_offset_ = other.block_offset_;
}

idx_t StorageBlock::GetBlockId() {
    return block_id_;
}

idx_t StorageBlock::GetBlockOffset() {
    return block_offset_;
}
