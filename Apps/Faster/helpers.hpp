#pragma once
// #include "common.hpp"
#include "auto_ptr.h"
using namespace FASTER::core;

#include <cstdint>
#include <string>
extern uint64_t kInitCount;
extern aligned_unique_ptr_t<uint64_t> init_keys_; // array of keys loaded from file

size_t load_keys(const std::string &load_filename, bool vanilla = false);