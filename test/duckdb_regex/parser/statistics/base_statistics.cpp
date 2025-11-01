
#include "../include/statistics/base_statistics.hpp"

void BaseStatistics::Deserialize(field_id_t field_id, Reader& reader) {
    assert(reader.Read<field_id_t>() == field_id);
    (void)reader.Read<bool>(100);  // has_null
	(void)reader.Read<bool>(101);  // has_no_null
	(void)reader.ReadEncoded<idx_t>(102);  // distinct_count

    assert(reader.Read<field_id_t>() == 103);  // about to read string stats

    (void)reader.Read<string>(200);
    (void)reader.Read<string>(201);
	(void)reader.Read<bool>(202);
	(void)reader.Read<bool>(203);
	(void)reader.ReadEncoded<uint64_t>(204);
}

void BaseStatistics::Deserialize(field_id_t field_id, LinkedListReader& reader) {
    // printf("[base_statistics.cpp -> Deserialize] (@start) offset=%llu\n", reader.GetCurrentOffset());
    // printf("next metadata ptr: %llx\n", reader.GetPointerToNextMetaBlock());
    field_id_t actual_field_id = reader.Read<field_id_t>();
    // printf("[base_statistics.cpp -> Deserialize] (after peeking field id) offset=%llu\n", reader.GetCurrentOffset());
    // printf("[base_statistics.cpp -> Deserialize] field_id=%llu\n", field_id);
    // printf("[base_statistics.cpp -> BaseStatistics::Deserialize] field_id=%llu, actual_field_id=%llu\n", field_id, actual_field_id);
    assert(actual_field_id == field_id);

    (void)reader.Read<bool>(100);  // has_null
	(void)reader.Read<bool>(101);  // has_no_null
	(void)reader.ReadEncoded<idx_t>(102);  // distinct_count
    // auto val = reader.Read<field_id_t>();
    // printf("val=%llx\n", val);
    assert(reader.Read<field_id_t>() == 103);  // about to read string stats

    (void)reader.Read<string>(200);
    (void)reader.Read<string>(201);
	(void)reader.Read<bool>(202);
	(void)reader.Read<bool>(203);
	(void)reader.ReadEncoded<uint64_t>(204);
    // reader.Print();
    field_id_t val = reader.Read<field_id_t>();
    // printf("after reading field id: offset=%llu\n", reader.GetCurrentOffset());
    // reader.Print();
    // printf("val=%llu\n", val);
    assert(val == OBJECT_END);
}