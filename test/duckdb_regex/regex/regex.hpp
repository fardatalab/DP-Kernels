#pragma once
#include "duckdb_regex/regex.hpp"
#include "re2_regex/re2/re2.h"
#include <string>

struct RegexMatcher {
    static bool Matches(const char* str, const re2::RE2& pattern) {
        return re2::RE2::FullMatch(string(str), pattern);
    }

    static bool Matches(const char* sdata, uint64_t slen, const char* pdata, uint64_t plen) {
        return like(sdata, slen, pdata, plen);
    }
};