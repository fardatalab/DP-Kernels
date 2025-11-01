#pragma once
#include "../../common.hpp"
#include "../header/database_header.hpp"
#include "../block/storage_block.hpp"
#include "../statistics/base_statistics.hpp"
#include "../statistics/distinct_statistics.hpp"
#include "../header/main_header.hpp"
#include "../schema/schema.hpp"
#include "../table/table.hpp"
#include "../type_info/type_info.hpp"


class Database {
public:
    Database(const char*);
    void LoadExistingDatabase();
    void ScanTable(Table*);
    Table* GetTable(idx_t);
private:
    ifstream file;
    const char* file_path;
    fsize_t file_size;
    vector<Schema*> schemas;
    vector<Table*> tables;
    MainHeader main_header;
    DatabaseHeader db_header;

    void LoadListEntries();
    void LoadRowGroups(Table*);
    void LoadColumnDataPointers(Table*);
    void LoadColumnDataPointersUtil(Table*, MetadataBlock&);
};