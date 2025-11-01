#include "../include/table/table.hpp"

//
// ColumnDataPointer class methods
//
//
ColumnDataPointer::ColumnDataPointer(uint64_t row_start, uint64_t tuple_count, StorageBlock& other) {
    row_start_ = row_start;
    tuple_count_ = tuple_count;
    pointer_ = other;
}

uint64_t ColumnDataPointer::GetRowStart() {
    return row_start_;
}

uint64_t ColumnDataPointer::GetTupleCount() {
    return tuple_count_;
}

uint64_t ColumnDataPointer::GetBlockId() {
    return pointer_.GetBlockId();
}

uint64_t ColumnDataPointer::GetBlockOffset() {
    return pointer_.GetBlockOffset();
}

// string Table::GetString(idx_t i) {
//     assert(i < data.size());
//     return data[i];
// }

Table::Table(CatalogType type, string catalog, string schema, uint8_t on_conflict,
            bool temporary, bool internal, string sql, string table, uint8_t ncols,
            uint64_t nrows, uint64_t row_group_count) :
            type_(type), catalog_name_(catalog), schema_name_(schema), on_conflict_(on_conflict),
            temporary_(temporary), internal_(internal), sql_(sql), table_name_(table), ncols_(ncols),
            nrows_(nrows), row_group_count_(row_group_count), thread_cursor(0), per_thread_cursor(0) {
}

void Table::Clear() {
    for (uint8_t i = 0; i < NTHREADS; i++) {
        uint64_t count = partial_counts[i];
        for (uint64_t j = 0; j < count; j++) {
            delete [] partial_strings[i][j];
        }
        delete [] partial_strings[i];
    }
    delete [] partial_strings;
    delete [] partial_counts;
}

ColumnInfo::ColumnInfo() {}

void load_data_worker(vector<ColumnDataPointer>& pointers, uint64_t start, uint64_t end,
                        char*** partial_strings, uint64_t* partial_count) {
    uint64_t count = 0;
    for (uint64_t k = start; k < end; k++) {
        count += pointers[k].GetTupleCount();
    }
    uint64_t idx = 0;
    *partial_strings = new char*[count];

    for (uint64_t k = start; k < end; k++) {
        ColumnDataPointer pointer = pointers[k];
        uint64_t block_id = pointer.GetBlockId();
        uint64_t block_offset = pointer.GetBlockOffset();
        uint64_t tuple_count = pointer.GetTupleCount();
        uint64_t block_start = HEADER_SIZE * 3 + block_id * DEFAULT_BLOCK_SIZE;
        byte_t* block = binary_file + block_start;
        byte_t* cursor = block + CHECKSUM_SIZE + block_offset + sizeof(uint32_t);

        DataReader offset_reader(cursor);
        uint32_t dict_end_offset = offset_reader.Read<uint32_t>();

        uint32_t total_length = *reinterpret_cast<uint32_t *>(cursor + tuple_count * sizeof(uint32_t));

        char* schar_array = reinterpret_cast<char *>(block + CHECKSUM_SIZE + block_offset + dict_end_offset - total_length);
        idx_t schar_offset = total_length;

        uint32_t prev = 0;
        uint32_t curr = 0;
        for (uint64_t i = 0; i < tuple_count; i++) {
            curr = offset_reader.Read<uint32_t>();
            uint32_t length = curr - prev;
            schar_offset -= length;
            (*partial_strings)[idx] = new char[length + 1];
            memcpy((*partial_strings)[idx], schar_array + schar_offset, length);
            (*partial_strings)[idx++][length] = 0;
            // printf("%s\n", (*partial_strings)[i]);
            prev = curr;
        }
        assert(schar_offset == 0);
    }
    *partial_count = count;
}

void Table::LoadData() {
    auto n_data_pointers = data_pointers_.size();
    auto n_data_pointers_per_thread = n_data_pointers / NTHREADS;
    vector<thread> threads;
    // char** partial_strings[NTHREADS];
    // uint64_t partial_counts[NTHREADS];
    partial_strings = new char**[NTHREADS];
    partial_counts = new uint64_t[NTHREADS];

    for (unsigned i = 0; i < NTHREADS; i++) {
        auto start = i * n_data_pointers_per_thread;
        auto end = (i == NTHREADS-1) ? n_data_pointers : (i + 1) * n_data_pointers_per_thread;
        threads.emplace_back(
            load_data_worker, ref(data_pointers_), start, end, &partial_strings[i], &partial_counts[i]
        );
    }
    for (auto& t : threads) {
        t.join();
    }
    // for (unsigned i = 0; i < NTHREADS; i++) {
    //     auto count = partial_counts[i];
    //     char** strings = partial_strings[i];
    //     for (auto j = 0; j < count; j++) {
    //         printf("%s\n", strings[j]);
    //     }
    // }
}

void Table::LoadTableColumns(field_id_t field_id, Reader& reader) {
    assert(reader.Read<field_id_t>() == field_id);
    ncols_ = reader.Read<uint8_t>(100);
    for (auto i = 0; i < ncols_; i++) {
        ColumnInfo column_info;
        ColumnInfo::Deserialize(column_info, reader);
        columns_meta_.push_back(column_info);
        assert(reader.Read<field_id_t>() == OBJECT_END);
    }
    assert(reader.Read<field_id_t>() == OBJECT_END);
}

idx_t Table::GetTableStartBlockId() {
    return table_start_.GetBlockId();
}

idx_t Table::GetTableStartBlockIndex() {
    return table_start_.GetBlockIndex();
}

idx_t Table::GetTableStartBlockOffset() {
    return table_start_.GetBlockOffset();
}

uint64_t Table::GetRowCount() {
    return nrows_;
}

void Table::SetRowGroupCount(uint64_t count) {
    row_group_count_ = count;
}

void Table::ReadRowCount(field_id_t field_id, Reader& reader) {
    nrows_ = reader.ReadEncoded<uint64_t>(field_id);
}

void Table::ReadIndexPointers(field_id_t field_id, Reader& reader) {
    auto count = reader.ReadEncoded<uint64_t>(field_id);
    assert(count == 0);
}

// char* Table::GetString(idx_t i) {
//     assert(i < nrows_);
//     auto thread_id = i / NTHREADS;
//     auto thread_off = i % NTHREADS;
//     return partial_strings[thread_id][thread_off];
// }

// char* Table::GetString(uint8_t thread_id, idx_t thread_off) {
//     return partial_strings[thread_id][thread_off];
// }

uint8_t Table::GetCurrentThreadCursor() {
    return thread_cursor;
}
idx_t Table::GetCurrentPerThreadCursor() {
    return per_thread_cursor;
}

char* Table::GetNextString() {
    bool is_last = (thread_cursor == NTHREADS);  // because of how AdvanceCursors works
    if (is_last) {
        return nullptr;
    }
    return partial_strings[thread_cursor][per_thread_cursor];
}

void Table::AdvanceCursors() {
    uint64_t count = partial_counts[thread_cursor];
    per_thread_cursor++;
    if (per_thread_cursor == count) {
        per_thread_cursor = 0;
        thread_cursor++;
    }
}

uint64_t Table::GetCountPerThread(uint8_t thread_id) {
    return partial_counts[thread_id];
}

char** Table::GetStringsPerThread(uint8_t thread_id) {
    return partial_strings[thread_id];
}

void ColumnInfo::Deserialize(ColumnInfo& info, Reader& reader) {
    info.name = reader.Read<string>(100);
    info.logical_type = LogicalType::Deserialize(101, reader);
    info.column_type = ColumnType::Deserialize(103, reader);
    info.compression_type = CompressionType::Deserialize(104, reader);
}

void Table::Deserialize(Reader& reader, Table* table) {
    uint8_t count = reader.Read<uint8_t>(100);
    assert(count == 1);

    // read in table info
    table->type_ = (CatalogType)reader.Read<uint8_t>(100);
    table->catalog_name_ = reader.Read<string>(101);
    table->catalog_name_ = reader.Read<string>(102);
    table->temporary_ = reader.Read<bool>(103);
    table->internal_ = reader.Read<bool>(104);
    table->on_conflict_ = reader.Read<uint8_t>(105);
    table->sql_ = reader.Read<string>(106);
    
    table->table_name_ = reader.Read<string>(200);
    // printf("catalog_name=%s\nschema_name=%s\ntable_name=%s\n", table->catalog_name_.c_str(), table->catalog_name_.c_str(), table->table_name_.c_str());
    table->LoadTableColumns(201, reader);
    assert(reader.Read<field_id_t>() == OBJECT_END);
    
    // printf("about to load table_start_...\n");
    table->table_start_ = MetadataBlock::Deserialize(101, reader);

    // printf("about to load row count...\n");
    table->ReadRowCount(102, reader);
    table->ReadIndexPointers(103, reader);
}

void Table::AddRowGroup(RowGroup* row_group) {
    row_groups_.push_back(row_group);
}

void Table::AddColumnDataPointer(uint64_t row_start, uint64_t tuple_count, StorageBlock block) {
    data_pointers_.push_back(ColumnDataPointer(row_start, tuple_count, block));
}

uint64_t Table::GetRowGroupCount() {
    return row_group_count_;
}

RowGroup* Table::GetRowGroup(idx_t i) {
    assert(i < row_group_count_);
    return row_groups_[i];
}