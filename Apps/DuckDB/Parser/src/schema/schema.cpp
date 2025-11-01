#include "../include/schema/schema.hpp"

Schema::Schema(CatalogType type, string& catalog_name, string& schema_name, bool temporary,
                bool internal, uint8_t on_conflict, string& sql) :
    type_(type), catalog_name_(catalog_name), schema_name_(schema_name), temporary_(temporary),
    internal_(internal), on_conflict_(on_conflict), sql_(sql) {
}

void Schema::Deserialize(Reader& reader, Schema* dest) {
    uint8_t count = reader.Read<uint8_t>(100);
    assert(count == 1);
    
    dest->type_ = (CatalogType)reader.Read<uint8_t>(100);
    dest->catalog_name_ = reader.Read<string>(101);
    dest->schema_name_ = reader.Read<string>(102);
    dest->temporary_ = reader.Read<bool>(103);
    dest->internal_ = reader.Read<bool>(104);
    dest->on_conflict_ = reader.Read<uint8_t>(105);
    dest->sql_ = reader.Read<string>(106);
    
    assert(reader.Read<field_id_t>() == OBJECT_END);
    assert(reader.Read<field_id_t>() == OBJECT_END);
}

void Schema::Print() {
    std::cout << "[Schema]"
              << "\ncatalog name: " << (catalog_name_.size() == 0 ? "(empty)" : catalog_name_)
              << "\nschema name: " << (schema_name_.size() == 0 ? "(empty)" : schema_name_)
              << "\ntemporary: " << (temporary_ ? "yes" : "no")
              << "\ninternal: " << (internal_ ? "yes" : "no")
              << "\non conflict: " << on_conflict_
              << "\nsql: " << (sql_.size() == 0 ? "(empty)" : sql_) << '\n';
}