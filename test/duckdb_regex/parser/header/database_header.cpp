#include "../include/header/database_header.hpp"

DatabaseHeader::DatabaseHeader(uint8_t which) {
    off_t file_offset = which * DEFAULT_HEADER_SIZE;
    byte_t* block = binary_file + file_offset;
    Reader reader(block);
    uint64_t fields[6];
    reader.Read<uint64_t>(fields, 6);
    iteration = fields[0];
    meta_block = fields[1];
    free_list = fields[2];
    block_count = fields[3];
    block_alloc_size = fields[4];
    vector_size = fields[5];
}

void DatabaseHeader::operator=(const DatabaseHeader& other) {
    iteration = other.iteration;
    meta_block = other.meta_block;
    free_list = other.free_list;
    block_count = other.block_count;
    block_alloc_size = other.block_alloc_size;
    vector_size = other.vector_size;
}


bool DatabaseHeader::operator>(const DatabaseHeader& other) {
    return iteration > other.iteration;
}

void DatabaseHeader::Print() {
    std::cout << "[DatabaseHeader]"
              << "\niteration: " << iteration
              << "\nmetablock ID: " << GetMetaBlockId()
              << "\nmetablock index: " << GetMetaBlockIndex()
              << "\nblock count: " << block_count
              << "\nblock alloc size: " << block_alloc_size
              << "\nvector size: " << vector_size << '\n';
}

idx_t DatabaseHeader::GetFreeListBlockId() {
    return (free_list & ~((idx_t)(0xff) << 56ull));
}

idx_t DatabaseHeader::GetFreeListBlockIndex() {
    return free_list >> 56ull;
}

idx_t DatabaseHeader::GetMetaBlockId() {
    return (meta_block & ~((idx_t)(0xff) << 56ull));
}

idx_t DatabaseHeader::GetMetaBlockIndex() {
    return meta_block >> 56ull;
}