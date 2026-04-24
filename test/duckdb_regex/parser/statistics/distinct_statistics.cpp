#include "../include/statistics/distinct_statistics.hpp"

void DistinctStatistics::Deserialize(field_id_t field_id, Reader& reader) {
    assert(reader.Read<field_id_t>() == field_id);
    reader.Advance(1);
    // sample count
    (void)reader.ReadEncoded<uint64_t>(100);
    // total count
    (void)reader.ReadEncoded<uint64_t>(101);
    (void)reader.Read<uint8_t>(102);
    // hyperloglog type
    (void)reader.Read<uint8_t>(100);
    // hyperloglog size
    auto size = reader.ReadEncoded<uint64_t>(101);
    assert(size == HLL_DENSE_SIZE);
    HLLHDR::Deserialize(reader);
    
    auto val = reader.Read<field_id_t>();
    assert(val == OBJECT_END);

    assert(reader.Read<field_id_t>() == OBJECT_END);  // ???
    assert(reader.Read<field_id_t>() == OBJECT_END);  // ???
    
    if (reader.Read<field_id_t>() == 101) {
        // table sample
        assert(reader.Read<uint8_t>() == 1);
        auto val = reader.Read<field_id_t>();
        // block sample
        if (val == 100) {
            assert(reader.Read<uint8_t>() == 1);
            // block reservior sample
            val = reader.Read<field_id_t>();
            // printf("val=%llu\n", val);
            if (val == 100) {
                (void)reader.Read<idx_t>();
                (void)reader.Read<idx_t>(101);
                (void)reader.Read<idx_t>(102);
                (void)reader.Read<idx_t>(103);
                (void)reader.Read<idx_t>(104);
                assert(reader.Read<field_id_t>() == OBJECT_END);
            } else {
                (void)reader.Read<idx_t>();
                (void)reader.Read<idx_t>(102);
                (void)reader.Read<idx_t>(103);
                (void)reader.Read<idx_t>(104);
                assert(reader.Read<field_id_t>() == OBJECT_END);
            }
            val = reader.Read<field_id_t>();
        }
        assert(val == 101);
        (void)reader.Read<uint8_t>();
        (void)reader.ReadEncoded<idx_t>(200);
        assert(reader.Read<field_id_t>() == OBJECT_END);
        assert(reader.Read<field_id_t>() == OBJECT_END);
    }
}