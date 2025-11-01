#pragma once
#include "../../common.hpp"
#include "../reader/reader.hpp"

enum class CatalogType : uint8_t {
    CATALOG_INVALID = 0,
    CATALOG_TABLE_ENTRY = 1,
    CATALOG_SCHEMA_ENTRY = 2,
    CATALOG_VIEW_ENTRY = 3,
    CATALOG_INDEX_ENTRY = 4,
    CATALOG_PREPARED_STATEMENT = 5,
    CATALOG_SEQUENCE_ENTRY = 6,
    CATALOG_COLLATION_ENTRY = 7,
    CATALOG_TYPE_ENTRY = 8,
    CATALOG_DATABASE_ENTRY = 9,

    /* functions */
    CATALOG_TABLE_FUNCTION_ENTRY = 25,
    CATALOG_SCALAR_FUNCTION_ENTRY = 26,
    CATALOG_AGGREGATE_FUNCTION_ENTRY = 27,
    CATALOG_PRAGMA_FUNCTION_ENTRY = 28,
    CATALOG_COPY_FUNCTION_ENTRY = 29,
    CATALOG_MACRO_ENTRY = 30,
    CATALOG_TABLE_MACRO_ENTRY = 31,

    /* version info */
    CATALOG_DELETED_ENTRY = 51,
    CATALOG_RENAMED_ENTRY = 52,

    /* secrets */
    CATALOG_SECRET_ENTRY = 71,
    CATALOG_SECRET_TYPE_ENTRY = 72,
    CATALOG_SECRET_FUNCTION_ENTRY = 73,

    /* dependency info */
    CATALOG_DEPENDENCY_ENTRY = 100
};

enum class LogicalTypeId : uint8_t {
	INVALID = 0,
	SQLNULL = 1, /* NULL type, used for constant NULL */
	UNKNOWN = 2, /* unknown type, used for parameter expressions */
	ANY = 3,     /* ANY type, used for functions that accept any type as parameter */
	USER = 4,    /* A User Defined Type (e.g., ENUMs before the binder) */
	BOOLEAN = 10,
	TINYINT = 11,
	SMALLINT = 12,
	INTEGER = 13,
	BIGINT = 14,
	DATE = 15,
	TIME = 16,
	TIMESTAMP_SEC = 17,
	TIMESTAMP_MS = 18,
	TIMESTAMP = 19, //! us
	TIMESTAMP_NS = 20,
	DECIMAL = 21,
	FLOAT = 22,
	DOUBLE = 23,
	CHAR = 24,
	VARCHAR = 25,
	BLOB = 26,
	INTERVAL = 27,
	UTINYINT = 28,
	USMALLINT = 29,
	UINTEGER = 30,
	UBIGINT = 31,
	TIMESTAMP_TZ = 32,
	TIME_TZ = 34,
	BIT = 36,
	STRING_LITERAL = 37,
	INTEGER_LITERAL = 38,
	VARINT = 39,
	UHUGEINT = 49,
	HUGEINT = 50,
	POINTER = 51,
	VALIDITY = 53,
	UUID = 54,
	STRUCT = 100,
	LIST = 101,
	MAP = 102,
    /* complex types */
	TABLE = 103,
	ENUM = 104,
	AGGREGATE_STATE = 105,
	LAMBDA = 106,
	UNION = 107,
	ARRAY = 108
};

class LogicalType {
public:
	LogicalType() = default;
	explicit LogicalType(uint8_t id) {
		this->id = static_cast<LogicalTypeId>(id);
	}
	static LogicalType Deserialize(field_id_t, Reader&);
private:
	LogicalTypeId id;
};

enum class ColumnTypeId : uint8_t {
    STANDARD = 0,
    GENERATED = 1
};

class ColumnType {
public:
	ColumnType() = default;
	explicit ColumnType(uint8_t id) {
		this->id = static_cast<ColumnTypeId>(id);
	}
	static ColumnType Deserialize(field_id_t, Reader&);
private:
	ColumnTypeId id;
};

enum class CompressionTypeId : uint8_t {
	COMPRESSION_AUTO = 0,
	COMPRESSION_UNCOMPRESSED = 1,
	COMPRESSION_CONSTANT = 2, // internal only
	COMPRESSION_RLE = 3,
	COMPRESSION_DICTIONARY = 4,
	COMPRESSION_PFOR_DELTA = 5,
	COMPRESSION_BITPACKING = 6,
	COMPRESSION_FSST = 7,
	COMPRESSION_CHIMP = 8,
	COMPRESSION_PATAS = 9,
	COMPRESSION_ALP = 10,
	COMPRESSION_ALPRD = 11,
	COMPRESSION_ZSTD = 12,
	COMPRESSION_ROARING = 13,
	COMPRESSION_EMPTY = 14, // internal only
	COMPRESSION_COUNT       // This has to stay the last entry of the type!
};

class CompressionType {
public:
	CompressionType() = default;
	explicit CompressionType(uint8_t id) {
		this->id = static_cast<CompressionTypeId>(id);
	}
	static CompressionType Deserialize(field_id_t, Reader&);
	//static CompressionType Deserialize(field_id_t, LinkedListReader&);
private:
	CompressionTypeId id;
};

enum ExtraInfoType : uint8_t {
	INVALID_TYPE_INFO = 0,
	GENERIC_TYPE_INFO = 1,
	DECIMAL_TYPE_INFO = 2,
	STRING_TYPE_INFO = 3,
	LIST_TYPE_INFO = 4,
	STRUCT_TYPE_INFO = 5,
	ENUM_TYPE_INFO = 6,
	USER_TYPE_INFO = 7,
	AGGREGATE_STATE_TYPE_INFO = 8,
	ARRAY_TYPE_INFO = 9,
	ANY_TYPE_INFO = 10,
	INTEGER_LITERAL_TYPE_INFO = 11
};

enum class ConstraintType : uint8_t {
	INVALID_CONST = 0,     // invalid constraint type
	NOT_NULL = 1,    // NOT NULL constraint
	CHECK = 2,       // CHECK constraint
	UNIQUE = 3,      // UNIQUE constraint
	FOREIGN_KEY = 4, // FOREIGN KEY constraint
};

typedef struct {
    uint8_t width;
    uint8_t scale;
} DecimalInfoType ;
