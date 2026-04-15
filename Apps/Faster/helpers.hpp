#include "FASTER/cc/benchmark-dir/file.h"
#include "common.hpp"

extern constexpr uint64_t kInitCount = 1000;
extern aligned_unique_ptr_t<uint64_t> init_keys_; // array of keys loaded from file

size_t load_keys(const std::string &load_filename, bool vanilla = false);