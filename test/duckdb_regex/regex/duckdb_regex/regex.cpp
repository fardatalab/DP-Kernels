#include "regex.hpp"

bool like(const char* sdata, uint64_t slen, const char* pdata, uint64_t plen) {
	uint64_t pidx = 0;
	uint64_t sidx = 0;
	for (; pidx < plen && sidx < slen; pidx++) {
		char pchar = StandardCharacterReader::Operation(pdata, pidx);
		char schar = StandardCharacterReader::Operation(sdata, sidx);
		if (pchar == '_') {
			StandardCharacterReader::NextCharacter(sdata, slen, sidx);
		} else if (pchar == '%') {
			pidx++;
			while (pidx < plen && pdata[pidx] == '%') {
				pidx++;
			}
			if (pidx == plen) {
				return true; /* tail is acceptable */
			}
			for (; sidx < slen; sidx++) {
				if (like(sdata + sidx, slen - sidx, pdata + pidx, plen - pidx)) {
					return true;
				}
			}
			return false;
		} else if (pchar == schar) {
			sidx++;
		} else {
			return false;
		}
	}
	while (pidx < plen && pdata[pidx] == '%') {
		pidx++;
	}
	return pidx == plen && sidx == slen;
}
