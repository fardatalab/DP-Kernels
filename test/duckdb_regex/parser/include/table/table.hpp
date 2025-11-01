#pragma once
#include "../block/metadata_block.hpp"
#include "../block/storage_block.hpp"
#include "../reader/reader.hpp"
#include "../type_info/type_info.hpp"
#include "../row_group/row_group.hpp"

class ColumnInfo {
public:
    ColumnInfo();
    static void Deserialize(ColumnInfo&, Reader&);
private:
    string name;
    LogicalType logical_type;
    ColumnType column_type;
    CompressionType compression_type;
};

class ColumnDataPointer {
public:
    ColumnDataPointer(uint64_t, uint64_t, StorageBlock&);
    uint64_t GetRowStart();
    uint64_t GetTupleCount();
    uint64_t GetBlockId();
    uint64_t GetBlockOffset();
private:
    [[maybe_unused]] uint64_t row_start_;
    uint64_t tuple_count_;
    StorageBlock pointer_;
};
    

class Table {
public:
    Table() = default;
    Table(CatalogType, string, string, uint8_t, bool, bool, string, string, uint8_t, uint64_t, uint64_t);
    void LoadData();
    static void Deserialize(Reader&, Table*);
    idx_t GetTableStartBlockId();
    idx_t GetTableStartBlockIndex();
    idx_t GetTableStartBlockOffset();
    uint64_t GetRowCount();
    void AdvanceCursors();
    void SetRowGroupCount(uint64_t);
    void AddRowGroup(RowGroup*);
    void AddColumnDataPointer(uint64_t, uint64_t, StorageBlock);
    uint64_t GetRowGroupCount();
    RowGroup* GetRowGroup(idx_t i);
    void Clear();
    char* GetNextString();
    uint8_t GetCurrentThreadCursor();
    idx_t GetCurrentPerThreadCursor();
    char* GetString(idx_t i);
    char* GetString(uint8_t, idx_t);
    uint64_t GetCountPerThread(uint8_t);
    char** GetStringsPerThread(uint8_t);
private:
    CatalogType type_;
    string catalog_name_;
    string schema_name_;
    uint8_t on_conflict_;
    bool temporary_;
    bool internal_;
    string sql_;
    // table-specific fields
    string table_name_;
    uint8_t ncols_;
    uint64_t nrows_;
    uint64_t row_group_count_;
    vector<RowGroup*> row_groups_;
    vector<ColumnInfo> columns_meta_;
    vector<ColumnDataPointer> data_pointers_;
    MetadataBlock table_start_;

    char*** partial_strings;
    uint64_t* partial_counts;
    
    uint8_t thread_cursor;
    idx_t per_thread_cursor;

    void LoadTableColumns(field_id_t, Reader&);
    void ReadRowCount(field_id_t, Reader&);
    void ReadIndexPointers(field_id_t, Reader&);
};