#pragma once
#include <unistd.h>
#include <string>

using namespace std;


static inline bool IsCharacter(char c) {
	return (c & 0xc0) != 0x80;
}

struct StandardCharacterReader {
	static void NextCharacter(const char* sdata, uint64_t slen, uint64_t &sidx) {
		sidx++;
		while (sidx < slen && !IsCharacter(sdata[sidx])) {
			sidx++;
		}
	}

	static char Operation(const char* data, uint64_t pos) {
		return data[pos];
	}
};

bool like(const char* sdata, uint64_t slen, const char* pdata, uint64_t plen);
