#include "../include/table/table.hpp"
#include "../common.hpp"

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

Table::Table(CatalogType type, string catalog, string schema, uint8_t on_conflict,
            bool temporary, bool internal, string sql, string table, uint8_t ncols,
            uint64_t nrows, uint64_t row_group_count) :
            type_(type), catalog_name_(catalog), schema_name_(schema), on_conflict_(on_conflict), temporary_(temporary), internal_(internal), sql_(sql), table_name_(table), ncols_(ncols), nrows_(nrows), row_group_count_(row_group_count), cp_cursor(0), str_cursor(0), cached_block(nullptr) {
}

void Table::Clear() {
    // for (uint8_t i = 0; i < NTHREADS; i++) {
    //     delete [] partial_strings[i];
    //     // delete [] partial_lengths[i];
    // }
}

ColumnInfo::ColumnInfo() {}

void load_data_worker(char* path, vector<ColumnDataPointer*>& pointers, uint64_t start, uint64_t end,
                    char** partial_strings, vector<uint8_t>& partial_lengths, uint8_t tid) {
    ifstream file(path, ios::binary);
    assert(file);
        
    string partial_strings_obj;
    // vector<uint8_t> partial_lengths_vec;

    for (uint64_t k = start; k < end; k++) {
        ColumnDataPointer* pointer = pointers[k];
        uint64_t block_id = pointer->GetBlockId();
        uint64_t block_offset = pointer->GetBlockOffset();
        uint64_t tuple_count = pointer->GetTupleCount();
        delete pointer;
        uint64_t block_start = HEADER_SIZE * 3 + block_id * DEFAULT_BLOCK_SIZE;
        byte_t block[DEFAULT_BLOCK_SIZE];
	    read_from(file, block, block_start, DEFAULT_BLOCK_SIZE); 
        byte_t* cursor = block + CHECKSUM_SIZE + block_offset + sizeof(uint32_t);

        DataReader offset_reader(cursor);
        uint32_t dict_end_offset = offset_reader.Read<uint32_t>();

        uint32_t total_length = *reinterpret_cast<uint32_t *>(cursor + tuple_count * sizeof(uint32_t));

        char* schar_array = reinterpret_cast<char *>(block + CHECKSUM_SIZE + block_offset + dict_end_offset - total_length);
        partial_strings_obj += string(schar_array, total_length);

        uint32_t* offset_arr =  reinterpret_cast<uint32_t *>(block + CHECKSUM_SIZE + block_offset + sizeof(uint64_t));
        for (int64_t i = tuple_count-1; i >= 0; i--) {
            uint8_t length = (i == 0) ? offset_arr[i] : offset_arr[i] - offset_arr[i-1];
            partial_lengths.push_back(length);
        }
    }

    size_t len = partial_strings_obj.size();
    *partial_strings = new char[len + 1];
    memcpy(*partial_strings, partial_strings_obj.c_str(), len);
    (*partial_strings)[len] = 0;

    // printf("partial_strings=%s\n", partial_strings);
    // std::cout << "[load_data_worker] partial_lengths.size()=" << partial_lengths.size() << '\n';
    
    file.close();
}

// void Table::LoadData(char* path) {
//     auto n_data_pointers = data_pointers_.size();
//     auto n_data_pointers_per_thread = n_data_pointers / NTHREADS;
//     vector<thread> threads;

//     for (unsigned i = 0; i < NTHREADS; i++) {
//         auto start = i * n_data_pointers_per_thread;
//         auto end = (i == NTHREADS-1) ? n_data_pointers : (i + 1) * n_data_pointers_per_thread;
//         threads.emplace_back(load_data_worker, path, ref(data_pointers_), start, end, &partial_strings[i], ref(partial_lengths[i]), i);
//     }
//     for (auto& t : threads) {
//         t.join();
//     }
// }

size_t Table::LoadData(char* path, size_t size, char* in_buf, uint8_t* lengths) {
    // printf("data_pointers_.size()=%u\n", data_pointers_.size());
	ifstream file(path, ios::binary);
    
	size_t size_aggregated = 0;
    size_t n_strs = 0;
	size_t n_cps = data_pointers_.size();

    ColumnDataPointer* cp;
    size_t block_id;
    size_t block_offset;
    size_t tuple_count;
    size_t file_offset;
	
    if (cached_block == nullptr) {
        // then cache it
        cached_block = new char[DEFAULT_BLOCK_SIZE];
        cp = data_pointers_[0];
        block_id = cp->GetBlockId();
        block_offset = cp->GetBlockOffset();
        tuple_count = cp->GetTupleCount();
        file_offset = DEFAULT_HEADER_SIZE * 3 + block_id * DEFAULT_BLOCK_SIZE;
        read_from(file, reinterpret_cast<uint8_t*>(cached_block), file_offset, DEFAULT_BLOCK_SIZE);
    }
    
	while (cp_cursor < n_cps && size_aggregated < size) {
        // printf("while...\n");
		cp = data_pointers_[cp_cursor];
		block_id = cp->GetBlockId();
		block_offset = cp->GetBlockOffset();
		tuple_count = cp->GetTupleCount();
        // printf("block_id=%u, block_offset=%u, tuple_count=%u\n", block_id, block_offset, tuple_count);
		if (str_cursor == tuple_count) {
            // printf("about to move to next block and cache it...\n");
			// move to next block and cache it
			delete cp;
			cp_cursor++;
			str_cursor = 0;
			if (cp_cursor == n_cps) break;
			cp = data_pointers_[cp_cursor];
			block_id = cp->GetBlockId();
			block_offset = cp->GetBlockOffset();
			tuple_count = cp->GetTupleCount();
			file_offset = DEFAULT_HEADER_SIZE * 3 + block_id * DEFAULT_BLOCK_SIZE;
			read_from(file, reinterpret_cast<uint8_t*>(cached_block), file_offset, DEFAULT_BLOCK_SIZE);
            // printf("next block read: cp_cursor=%u, str_cursor=%u\n", cp_cursor, str_cursor);
		}
        
	    char* cursor = cached_block + CHECKSUM_SIZE + block_offset + sizeof(uint32_t);

		uint32_t dict_end_offset = *reinterpret_cast<uint32_t*>(cursor);
        uint32_t total_length = *reinterpret_cast<uint32_t*>(cursor + tuple_count * sizeof(uint32_t));
        char* str_chunk = reinterpret_cast<char*>(cached_block + CHECKSUM_SIZE + block_offset + dict_end_offset - total_length);
        uint32_t* offset_arr = reinterpret_cast<uint32_t*>(cached_block + CHECKSUM_SIZE + block_offset + sizeof(uint64_t));
		uint8_t strlen = (str_cursor == 0) ? offset_arr[0] : offset_arr[str_cursor] - offset_arr[str_cursor-1];
		if (size_aggregated + strlen > size) {
			break;
		} else {
			size_t str_chunk_offset = total_length - offset_arr[str_cursor];
			// char* src = str_chunk + str_chunk_offset;
            string src(str_chunk + str_chunk_offset, strlen);
            // src += "\n";  // as separator between strings
			memcpy(in_buf + size_aggregated, src.c_str(), src.size());
            
            lengths[n_strs++] = src.size();
			size_aggregated += src.size();
			str_cursor++;
		}
	}
	// printf("done\n");
	file.close();
    return n_strs;
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
//    printf("catalog_name=%s\nschema_name=%s\ntable_name=%s\n", table->catalog_name_.c_str(), table->catalog_name_.c_str(), table->table_name_.c_str());
    table->LoadTableColumns(201, reader);
    assert(reader.Read<field_id_t>() == OBJECT_END);
    
//    printf("about to load table_start_...\n");
    table->table_start_ = MetadataBlock::Deserialize(101, reader);

//    printf("about to load row count...\n");
    table->ReadRowCount(102, reader);
    table->ReadIndexPointers(103, reader);
}

void Table::AddRowGroup(RowGroup* row_group) {
    row_groups_.push_back(row_group);
}

void Table::AddColumnDataPointer(uint64_t row_start, uint64_t tuple_count, StorageBlock block) {
    ColumnDataPointer* pointer = new ColumnDataPointer(row_start, tuple_count, block);
    data_pointers_.push_back(pointer);
}

uint64_t Table::GetRowGroupCount() {
    return row_group_count_;
}

RowGroup* Table::GetRowGroup(idx_t i) {
    assert(i < row_group_count_);
    return row_groups_[i];
}
