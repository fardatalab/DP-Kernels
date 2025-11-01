#pragma once
#include "../../common.hpp"
#include "../decoder/decoder.hpp"

#define IS_NEXT_BLOCK(offset) (((offset) % METADATA_BLOCK_SIZE) == 0)

class Reader {
public:
    Reader(byte_t* cursor) : cursor_(cursor), offset_(0) {}
    //
    // normal read operations
    //
    //
    void Reassign(ifstream& file, uint64_t file_size, idx_t next_id, idx_t next_index) {
        auto fileoff = DEFAULT_HEADER_SIZE * 3 + DEFAULT_BLOCK_SIZE * next_id + CHECKSUM_SIZE;
        file.seekg(fileoff, ios::beg);
        file.read(reinterpret_cast<char *>(cursor_), GET_READ_SIZE(file, file_size));
        cursor_ += METADATA_BLOCK_SIZE * next_index;
        offset_ = 0;
    }

    template <typename T>
    void Read(T* dest, size_t n) {
        uint64_t i = 0;
        auto read_size = n * sizeof(T);
        byte_t buffer[1024];
        while (i < read_size) {
            if (IS_NEXT_BLOCK(offset_)) {
                offset_ += 8;
            }
            buffer[i++] = cursor_[offset_++];
        }
        memcpy(dest, buffer, sizeof(T) * n);
    }

    template <typename T>
    T Read() {
        uint64_t i = 0;
        byte_t buffer[sizeof(T)];
        while (i < sizeof(T)) {
            if (IS_NEXT_BLOCK(offset_)) {
                offset_ += 8;
            }
            buffer[i++] = cursor_[offset_++];
        }
        T val;
        memcpy(&val, buffer, sizeof(T));
        return val;
    }

    template <typename T>
    T Read(field_id_t field_id) {
        uint64_t tmp_offset = offset_;
        field_id_t actual_field_id = PeekFieldId(&tmp_offset);
        constexpr bool is_string = is_same<T, string>::value;
        if (field_id != actual_field_id) {
            if constexpr (is_string) {
                return "";
            } else {
                return static_cast<T>(0);
            }
        }
        offset_ = tmp_offset;
        if constexpr (is_string) {
            if (IS_NEXT_BLOCK(offset_)) offset_ += 8;
            auto size = cursor_[offset_++];
            char buffer[size];
            Read<char>(buffer, size);
            string result(buffer, size);
            return result;
        } else {
            return Read<T>();
        }
    }

    //
    // decode + read
    //
    //
    template <typename T>
    T ReadEncoded() {
        byte_t bytes[16] = {0};
        Peek(bytes, 16);
        T val;
        size_t size;
        if (is_unsigned<T>::value) {
            size = unsigned_decode(bytes, reinterpret_cast<uint64_t *>(&val));
        } else {
            size = signed_decode(bytes, reinterpret_cast<int64_t *>(&val));
        }
        Advance(size);
        return val;
    }

    template <typename T>
    T ReadEncoded(field_id_t field_id) {
        // 
        // peek the next field id
        // 
        // 
        uint64_t tmp_offset = offset_;
        field_id_t actual_field_id = PeekFieldId(&tmp_offset);
        if (actual_field_id != field_id) {
            return static_cast<T>(0);
        }
        offset_ = tmp_offset;

        // 
        // read and decode
        //
        //
        byte_t bytes[16];
        Peek(bytes, 16);
        T val;
        size_t size;
        if (is_unsigned<T>::value) {
            size = unsigned_decode(bytes, reinterpret_cast<uint64_t *>(&val));
        } else {
            size = signed_decode(bytes, reinterpret_cast<int64_t *>(&val));
        }
        Advance(size);
        return val;
    }

    template <typename T>
    bool ReadEncoded(field_id_t field_id, T* val) {
        *val = ReadEncoded<T>(field_id);
        return static_cast<uint64_t>(*val) == 0;
    }

    idx_t ReadMetaBlockPtr() {
        assert(IS_NEXT_BLOCK(offset_));
        //
        // DON'T ADVANCE!
        //
        //
        idx_t* ptr = reinterpret_cast<idx_t *>(cursor_ + offset_);
        return *ptr;
    }

    void Advance(size_t nbytes) {
        uint64_t total = 0;
        while (total < nbytes) {
            if (IS_NEXT_BLOCK(offset_)) {
                offset_ += CHECKSUM_SIZE;
            }
            total++;
            offset_++;
        }
    }

    void UnalignedAdvance(size_t nbytes) {
        offset_ += nbytes;
        // for (auto i = offset_; i < offset_ + 16; i++) {
        //     printf("%02x ", (uint8_t)cursor_[i]);
        // }
        // printf("\n");
    }

    uint64_t CurrentPosition() {
        return offset_;
    }


private:
    byte_t* cursor_;
    uint64_t offset_;

    field_id_t PeekFieldId(uint64_t* tmp_offset) {
        byte_t actual_field_id_buffer[sizeof(field_id_t)];
        uint64_t i = 0;
        while (i < sizeof(field_id_t)) {
            if (IS_NEXT_BLOCK(*tmp_offset)) {
                *tmp_offset += 8;
            }
            actual_field_id_buffer[i++] = cursor_[(*tmp_offset)++];
        }
        field_id_t actual_field_id;
        memcpy(&actual_field_id, actual_field_id_buffer, sizeof(field_id_t));
        return actual_field_id;
    }

    void Peek(byte_t* dest, uint64_t nbytes) {
        uint64_t i = 0;
        auto tmp_offset = offset_;
        while (i < nbytes) {
            if (IS_NEXT_BLOCK(tmp_offset)) {
                tmp_offset += 8;
            }
            dest[i++] = cursor_[tmp_offset++];
        }
    }
};

class DataReader {
public:
    DataReader(byte_t* cursor) : cursor_(cursor) {}
    template <typename T>
    void Read(T* dest, uint64_t n) {
        T* ptr = reinterpret_cast<T *>(cursor_);
        memcpy(dest, ptr, sizeof(T) * n);
        cursor_ += sizeof(T) * n;
    }
    template <typename T>
    T Read(vector<T>& dest, uint64_t n) {
        T* ptr = reinterpret_cast<T *>(cursor_);
        copy(ptr, ptr + n, back_insert_iterator(dest));
        cursor_ += sizeof(T) * n;
        return dest[dest.size()-1];
    }
    template <typename T> T Read() {
        T* ptr = reinterpret_cast<T *>(cursor_);
        T val = *ptr;
        cursor_ += sizeof(T);
        return val;
    }
private:
    byte_t* cursor_;
};

class LinkedListReader {
public:
    LinkedListReader(idx_t block_id, idx_t block_index) : offset_(POINTER_SIZE) {
        // assert((file_ = ifstream(filepath, ifstream::binary)));
        uint64_t meta_offset = METADATA_BLOCK_SIZE * block_index;
        uint64_t offset = DEFAULT_HEADER_SIZE * 3 + DEFAULT_BLOCK_SIZE * block_id + CHECKSUM_SIZE + meta_offset;
        // file_.seekg(offset, ios::beg);
        // file_.read(reinterpret_cast<char *>(cursor_), METADATA_BLOCK_SIZE);
        cursor_ = binary_file + offset;
    }

    uint64_t GetCurrentOffset() {
        return offset_;
    }

    idx_t GetPointerToNextMetaBlock() {
        idx_t* ptr = reinterpret_cast<idx_t *>(cursor_);
        return ptr[0];
    }

    void Advance(size_t nbytes) {
        uint64_t remaining_size = METADATA_BLOCK_SIZE - offset_;
        if (remaining_size >= nbytes) {
            offset_ += nbytes;
        } else {
            remaining_size = nbytes - remaining_size;
            // memcpy(cursor_, tmp_cursor_, METADATA_BLOCK_SIZE);
            cursor_ = tmp_cursor_;
            offset_ = POINTER_SIZE + remaining_size;
        }
    }

    template <typename T> void Peek(T* dest, size_t n) {
        uint64_t remaining_size = METADATA_BLOCK_SIZE - offset_;
        uint64_t read_size = n * sizeof(T);
        if (remaining_size >= read_size) {
            memcpy(dest, cursor_ + offset_, read_size);
        } else {
            byte_t* dest_casted = reinterpret_cast<byte_t *>(dest);
            memcpy(dest_casted, cursor_ + offset_, remaining_size);
            uint64_t to_read = read_size - remaining_size;
            idx_t next_metadata_ptr = GetPointerToNextMetaBlock();
            idx_t next_block_id = (next_metadata_ptr & ~((idx_t)(0xff) << 56ull));
            idx_t next_block_index = (next_metadata_ptr >> 56ull);
            uint64_t next_meta_offset = METADATA_BLOCK_SIZE * next_block_index;
            uint64_t next_offset = DEFAULT_HEADER_SIZE * 3 + DEFAULT_BLOCK_SIZE * next_block_id + CHECKSUM_SIZE + next_meta_offset;

            idx_t tmp_offset = POINTER_SIZE;
            // file_.seekg(next_offset, ios::beg);
            // file_.read(reinterpret_cast<char *>(tmp_cursor_), METADATA_BLOCK_SIZE);
            tmp_cursor_ = binary_file + next_offset;
            memcpy(dest_casted + remaining_size, tmp_cursor_ + tmp_offset, to_read);
        }
    }

    template <typename T> T Peek() {
        T val;
        Peek(&val, 1);
        return val;
    }

    template <typename T> void Read(T* dest, size_t n) {
        uint64_t read_size = sizeof(T) * n;
        uint64_t remaining_size = METADATA_BLOCK_SIZE - offset_;
        T* ptr = reinterpret_cast<T *>(cursor_ + offset_);
        if (remaining_size >= read_size) {
            memcpy(dest, ptr, read_size);
            offset_ += read_size;
        } else {
            byte_t* dest_casted = reinterpret_cast<byte_t *>(dest);
            memcpy(dest_casted, ptr, remaining_size);
            uint64_t to_read = read_size - remaining_size;
            idx_t next_metadata_ptr = GetPointerToNextMetaBlock();
            idx_t next_block_id = (next_metadata_ptr & ~((idx_t)(0xff) << 56ull));
            idx_t next_block_index = (next_metadata_ptr >> 56ull);
            uint64_t next_meta_offset = METADATA_BLOCK_SIZE * next_block_index;
            uint64_t next_offset = DEFAULT_HEADER_SIZE * 3 + DEFAULT_BLOCK_SIZE * next_block_id + CHECKSUM_SIZE + next_meta_offset;
            offset_ = POINTER_SIZE;
            // file_.seekg(next_offset, ios::beg);
            // file_.read(reinterpret_cast<char *>(cursor_), METADATA_BLOCK_SIZE);
            cursor_ = binary_file + next_offset;
            memcpy(dest_casted + remaining_size, cursor_ + offset_, to_read);
            offset_ += to_read;
        }
    }

    template <typename T> T Read() {
        T val;
        Read<T>(&val, 1);
        return val;
    }

    template <typename T> T Read(field_id_t field_id) {
        field_id_t actual_field_id = Peek<field_id_t>();
        constexpr bool is_string = is_same<T, string>::value;
        if (field_id != actual_field_id){
            if constexpr (is_string) {
                return "";
            } else {
                return static_cast<T>(0);
            }
        }        
        Advance(sizeof(field_id_t));
        if constexpr (is_string) {
            uint8_t size = Read<uint8_t>();
            char buffer[size];
            Read<char>(buffer, size);
            string result(buffer, size);
            return result;
        } else {
            return Read<T>();
        }
    }

    template <typename T> T ReadEncoded(field_id_t field_id) {
        // printf("[reader.hpp -> ReadEncoded<T>(field_id)] before peeking: offset_=%llu\n", offset_);
        field_id_t actual_field_id = Peek<field_id_t>();
        // printf("[reader.hpp -> ReadEncoded<T>(field_id)] after peeking: offset_=%llu\n", offset_);
        if (actual_field_id != field_id) {
            return static_cast<T>(0);
        }
        // printf("[reader.hpp -> ReadEncoded<T>(field_id)] before advancing: offset_=%llu\n", offset_);
        Advance(sizeof(field_id_t));
        // printf("[reader.hpp -> ReadEncoded<T>(field_id)] after advancing: offset_=%llu\n", offset_);
        
        byte_t bytes[16];
        Peek<byte_t>(bytes, 16);
        T val;
        size_t size;
        if (is_unsigned<T>::value) {
            size = unsigned_decode(bytes, reinterpret_cast<uint64_t *>(&val));
        } else {
            size = signed_decode(bytes, reinterpret_cast<int64_t *>(&val));
        }
        // printf("[reader.hpp -> ReadEncoded<T>(field_id)] before advancing: offset_=%llu\n", offset_);
        Advance(size);
        // printf("[reader.hpp -> ReadEncoded<T>(field_id)] after advancing: offset_=%llu\n\n", offset_);
        return val;
    }

private:
    // ifstream file_;
    // Always stores a complete metadata block, the start of which encodes the
    // pointer to the next metadata block
    // byte_t cursor_[METADATA_BLOCK_SIZE];
    byte_t* cursor_;
    byte_t* tmp_cursor_;
    // byte_t tmp_cursor[METADATA_BLOCK_SIZE];
    uint64_t offset_;
};