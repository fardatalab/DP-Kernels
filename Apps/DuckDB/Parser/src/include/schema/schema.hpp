#pragma once
#include "../type_info/type_info.hpp"
#include "../reader/reader.hpp"

class Schema {
public:
    Schema() = default;
    Schema(CatalogType type, string& catalog_name, string& schema_name,
            bool temporary, bool internal, uint8_t on_conflict, string&);
    void Print();
    static void Deserialize(Reader&, Schema*);
    // static void Deserialize(Reader&, Schema&);
private:
    CatalogType type_;
    string catalog_name_;
    string schema_name_;
    bool temporary_;
    bool internal_;
    uint8_t on_conflict_;
    string sql_;
};