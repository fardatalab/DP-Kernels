#pragma once
#include "faster_bench_common.hpp"
// #include "faster.h"
// using namespace FASTER::core;

#include <cstdint>
#include <memory>
#include <string>
extern uint64_t kInitCount;

bool read_file(const char *filename, uint8_t **buffer, size_t *buffer_size);

extern aligned_unique_ptr_t<uint64_t> init_keys_; // array of keys loaded from file

size_t load_keys(const std::string &load_filename, bool vanilla = false);