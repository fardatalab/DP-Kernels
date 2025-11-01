#include "../include/database/database.hpp"

byte_t* binary_file = nullptr;

//
// Database class methods
//
//
Database::Database(const char* filepath) {
    assert((file = ifstream(filepath, ifstream::binary)));
    file_size = filesystem::file_size(filesystem::path(filepath));
    file_path = filepath;

    assert((binary_file = new byte_t[file_size]) != nullptr);
    file.read(reinterpret_cast<char *>(binary_file), file_size);
}

void Database::LoadExistingDatabase() {
    std::chrono::duration<double> list_entries_info_elapsed;
    std::chrono::duration<double> row_group_elapsed;
    std::chrono::duration<double> data_pointer_elapsed;

    //
    // load metadata about each schema and table
    //
    //
    auto start = chrono::high_resolution_clock::now();
    main_header = MainHeader(0);  // 0 is unused
    DatabaseHeader db_header_1(1);
    DatabaseHeader db_header_2(2);
    db_header = (db_header_1 > db_header_2) ? db_header_1 : db_header_2;
    LoadListEntries();
    auto end = chrono::high_resolution_clock::now();
    list_entries_info_elapsed = end - start;

    //
    // load metadata for row groups
    //
    //
    start = chrono::high_resolution_clock::now();
    for (auto& table : tables) {
        LoadRowGroups(table);
    }
    end = chrono::high_resolution_clock::now();
    row_group_elapsed = end - start;

    //
    // load pointers to actual data
    //
    //
    start = chrono::high_resolution_clock::now();
    for (auto& table : tables) {
        LoadColumnDataPointers(table);
    }
    end = chrono::high_resolution_clock::now();
    data_pointer_elapsed = end - start;
    
    file.close();
    // std::cout << "list entries metadata load time: " << list_entries_info_elapsed.count() << " sec\n"
    //           << "row groups load time: " << row_group_elapsed.count() << " sec\n"
    //           << "data pointers load time: " << data_pointer_elapsed.count() << " sec\n"
    //           << "data load time: " << data_elapsed.count() << " sec\n";
    // file.close();
}

Table* Database::GetTable(idx_t i) {
    assert(i < tables.size());
    return tables[i];
}

void Database::ScanTable(Table* table) {
    table->LoadData();
}

void Database::LoadListEntries() {
    idx_t metablock_id = db_header.GetMetaBlockId();
    idx_t metablock_index = db_header.GetMetaBlockIndex();
    // byte_t block[DEFAULT_BLOCK_SIZE];
    // file.seekg(DEFAULT_HEADER_SIZE * 3 + DEFAULT_BLOCK_SIZE * metablock_id, ios::beg);
    // file.read(reinterpret_cast<char *>(block), GET_READ_SIZE(file, file_size));
    byte_t* block = binary_file + DEFAULT_HEADER_SIZE * 3 + DEFAULT_BLOCK_SIZE * metablock_id;
    byte_t* cursor = block + CHECKSUM_SIZE + METADATA_BLOCK_SIZE * metablock_index;
    Reader reader(cursor);
    
    uint8_t size = reader.Read<uint8_t>(100);
    for (auto i = 0; i < size; i++) {
        uint8_t type = reader.Read<uint8_t>(99);
        if ((CatalogType)type == CatalogType::CATALOG_SCHEMA_ENTRY) {
            Schema* schema = new Schema;
            Schema::Deserialize(reader, schema);
            schemas.push_back(schema);
        } else if ((CatalogType)type == CatalogType::CATALOG_TABLE_ENTRY) {
            Table* table = new Table;
            Table::Deserialize(reader, table);
            tables.push_back(ref(table));
        } else {
            (void)reader.Read<field_id_t>();
        }
    }
}

void Database::LoadRowGroups(Table* table) {
    idx_t block_id = table->GetTableStartBlockId();
    idx_t block_index = table->GetTableStartBlockIndex();
    idx_t block_offset = table->GetTableStartBlockOffset();
    // byte_t block[DEFAULT_BLOCK_SIZE];
    // file.seekg(HEADER_SIZE * 3 + block_id * DEFAULT_BLOCK_SIZE + CHECKSUM_SIZE, ios::beg);
    // file.read(reinterpret_cast<char *>(block), GET_READ_SIZE(file, file_size));

    byte_t* block = binary_file + HEADER_SIZE * 3 + block_id * DEFAULT_BLOCK_SIZE + CHECKSUM_SIZE;
    Reader reader(block);
    reader.UnalignedAdvance(METADATA_BLOCK_SIZE * block_index + block_offset);
    assert(reader.ReadEncoded<uint64_t>(100) == 1);
    (void)reader.Read<uint8_t>();
    BaseStatistics::Deserialize(100, reader);
    assert(reader.Read<field_id_t>() == OBJECT_END);  // string stats end
    assert(reader.Read<field_id_t>() == OBJECT_END);  // base stats end
    DistinctStatistics::Deserialize(101, reader);
    uint64_t n_row_groups = reader.Read<uint64_t>();
    table->SetRowGroupCount(n_row_groups);

    // now load the actual row groups
    for (uint64_t i = 0; i < n_row_groups; i++) {
        // printf("===== row group %llu =====\n", i);
        auto row_start = reader.ReadEncoded<uint64_t>(100);
        auto tuple_count = reader.ReadEncoded<uint64_t>(101);
        auto n_data_blocks = reader.ReadEncoded<uint64_t>(102);
        // printf("row_start=%llu, tuple_count=%llu, n_data_pointers=%llu\n",
        //         row_start, tuple_count, n_data_blocks);
        RowGroup* row_group = new RowGroup(row_start, tuple_count);
        row_group->ReadDataBlocks(n_data_blocks, reader);
        (void)reader.Read<uint8_t>(103);
        auto val = reader.Read<field_id_t>();
        assert(val == OBJECT_END);
        table->AddRowGroup(row_group);
        // printf("=== row group %llu done ===\n", i);
    }
}

void Database::LoadColumnDataPointers(Table* table) {
    uint64_t n_row_groups = table->GetRowGroupCount();
    for (uint64_t i = 0; i < n_row_groups; i++) {
        RowGroup* row_group = table->GetRowGroup(i);
        auto n_data_pointers = row_group->GetDataPointerCount();
        for (uint64_t j = 0; j < n_data_pointers; j++) {
            MetadataBlock data_block = row_group->GetMetaBlock(j);
            LoadColumnDataPointersUtil(table, data_block);
        }
    }
    // printf("data_pointers.size()=%llu\n", data_pointers.size());
    // assert(data.size() == table->GetRowCount());
}

void Database::LoadColumnDataPointersUtil(Table* table, MetadataBlock& meta_block){
    // printf("[database.cpp -> Database::LoadColumnDataPointersUtil] called\n");
    idx_t block_id = meta_block.GetBlockId();
    idx_t block_index = meta_block.GetBlockIndex();
    idx_t block_offset = meta_block.GetBlockOffset();

    // printf("block_offset=%llu\n", block_offset);
    // LinkedListReader reader(file_path, block_id, block_index);
    LinkedListReader reader(block_id, block_index);
    reader.Advance(block_offset - POINTER_SIZE);
    auto n_data_pointers = reader.ReadEncoded<uint64_t>(100);
    // printf("n_data_pointers=%llu\n", n_data_pointers);
    for (uint64_t i = 0; i < n_data_pointers; i++) {
        auto row_start = reader.ReadEncoded<uint64_t>(100);
        auto tuple_count = reader.ReadEncoded<uint64_t>(101);
        auto data_block = StorageBlock::Deserialize(102, reader);
        auto compression = CompressionType::Deserialize(103, reader);
        BaseStatistics::Deserialize(104, reader);
        (void)compression;
        assert(reader.Read<field_id_t>() == OBJECT_END);  // base stats end
        assert(reader.Read<field_id_t>() == OBJECT_END);
        // printf("row_start=%llu, tuple_count=%llu, block_id=%llu, offset=%llu\n",
        //         row_start, tuple_count, data_block.GetBlockId(), data_block.GetBlockOffset());
        // ColumnDataPointer data_pointer(row_start, tuple_count, data_block);
        table->AddColumnDataPointer(row_start, tuple_count, data_block);
    }
}