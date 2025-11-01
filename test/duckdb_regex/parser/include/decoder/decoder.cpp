#include "decoder.hpp"

size_t unsigned_decode(byte_t* cursor, uint64_t* value) {
    *value = 0;
    size_t varint_size;
    uint8_t buffer[16] = {0};
    for (varint_size = 0; varint_size < 16; varint_size++) {
        buffer[varint_size] = cursor[varint_size];
        // printf("%02x ", buffer[varint_size]);
        if (!(buffer[varint_size] & 0x80)) {
            varint_size++;
            break;
        }
    }
    // printf("varint_size=%llu\n", varint_size);
    uint64_t shift = 0;
    uint64_t offset = 0;
    uint8_t byte;
    do {
        byte = buffer[offset++];
        *value |= static_cast<uint64_t>(byte & 0x7F) << shift;
        shift += 7;
    } while (byte & 0x80);
    assert(offset == varint_size);

    return offset;
}

size_t signed_decode(byte_t* cursor, int64_t* value) {
    byte_t bytes[16] = {0};
    memcpy(bytes, cursor, 16);
    *value = 0;
    size_t varint_size;
    uint8_t buffer[16] = {0};
    for (varint_size = 0; varint_size < 16; varint_size++) {
        memcpy(buffer + varint_size, bytes + varint_size, 1);
        if (!(buffer[varint_size] & 0x80)) {
            varint_size++;
            break;
        }
    }
    uint64_t shift = 0;
    uint64_t offset = 0;
    uint8_t byte;
    do {
        byte = buffer[offset++];
        *value |= (uint64_t)(byte & 0x7f) << shift;
        shift += 7;
    } while (byte & 0x80);
    if (shift < sizeof(int64_t) * 8 && (byte & 0x40)) {
        *value |= -((uint64_t)(1) << shift);
    }
    assert(offset == varint_size);
    return offset;
}