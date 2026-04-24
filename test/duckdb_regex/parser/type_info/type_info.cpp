#include "../include/type_info/type_info.hpp"

LogicalType LogicalType::Deserialize(field_id_t field_id, Reader& reader) {
    assert(reader.Read<field_id_t>() == field_id);
    uint8_t id = reader.Read<uint8_t>(100);
    assert(reader.Read<field_id_t>() == OBJECT_END);
    return LogicalType(id);
}

ColumnType ColumnType::Deserialize(field_id_t field_id, Reader& reader) {
    assert(reader.Read<field_id_t>() == field_id);
    uint8_t id = reader.Read<uint8_t>();
    return ColumnType(id);
}

CompressionType CompressionType::Deserialize(field_id_t field_id, Reader& reader) {
    assert(reader.Read<field_id_t>() == field_id);
    uint8_t id = reader.Read<uint8_t>();
    return CompressionType(id);
}

CompressionType CompressionType::Deserialize(field_id_t field_id, LinkedListReader& reader) {
    assert(reader.Read<field_id_t>() == field_id);
    uint8_t id = reader.Read<uint8_t>();
    return CompressionType(id);
}