#pragma once
#include "../../common.hpp"
#include "../reader/reader.hpp"

#define HLL_P 12
#define HLL_HDR_SIZE 17
#define HLL_BITS 6
#define HLL_REGISTERS (1<<HLL_P)
#define HLL_DENSE_SIZE (HLL_HDR_SIZE+((HLL_REGISTERS*HLL_BITS+7)/8))

class HLLHDR {
private:
    char magic[4];      /* "HYLL" */
    uint8_t encoding;   /* HLL_DENSE or HLL_SPARSE. */
    uint8_t notused[3]; /* Reserved for future use, must be zero. */
    uint8_t card[8];    /* Cached cardinality, little endian. */
    uint8_t registers[1]; /* Data bytes. */
public:
    HLLHDR() = default;
    static void Deserialize(Reader& reader) {
        reader.Advance(HLL_DENSE_SIZE);
    }
};

