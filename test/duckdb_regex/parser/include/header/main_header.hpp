#pragma once

#include "../../common.hpp"
#include "../reader/reader.hpp"

class MainHeader {
public:
    MainHeader() = default;
    MainHeader([[maybe_unused]] int _unused);
    // MainHeader(ifstream& file);
    // MainHeader(int placeholder);
    void Print();
    void operator=(const MainHeader& other);
    // ~MainHeader() = default;
private:
    char magic_bytes[4];
	uint64_t version;
};