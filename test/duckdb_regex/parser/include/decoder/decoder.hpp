#pragma once
#include "../../common.hpp"

size_t unsigned_decode(byte_t* cursor, uint64_t* value);
size_t signed_decode(byte_t* cursor, int64_t* value);