#pragma once
#include "../../common.hpp"
#include "../reader/reader.hpp"

class StorageBlock {
public:
    StorageBlock() = default;
    StorageBlock(idx_t, idx_t);
    static StorageBlock Deserialize(field_id_t, Reader&);
//    static StorageBlock Deserialize(field_id_t, LinkedListReader&);
    void operator=(StorageBlock&);
    idx_t GetBlockId();
    idx_t GetBlockOffset();
private:
    idx_t block_id_;
    idx_t block_offset_;
};
