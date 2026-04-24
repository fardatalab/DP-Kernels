#pragma once
#include "../../common.hpp"
#include "../reader/reader.hpp"

class DatabaseHeader {
public:
    DatabaseHeader() = default;
    // DatabaseHeader(ifstream&, uint8_t);
    DatabaseHeader(uint8_t);
    idx_t GetMetaBlockId();
    idx_t GetMetaBlockIndex();
    idx_t GetFreeListBlockId();
    idx_t GetFreeListBlockIndex();
    void operator=(const DatabaseHeader&);
    bool operator>(const DatabaseHeader&);
    void Print();
    ~DatabaseHeader() = default;
private:
    uint64_t iteration;
    idx_t meta_block;
    idx_t free_list;
    uint64_t block_count;
    uint64_t block_alloc_size;
    uint64_t vector_size;
};