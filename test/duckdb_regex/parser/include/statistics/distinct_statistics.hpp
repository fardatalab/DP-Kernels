#include "../../common.hpp"
#include "../reader/reader.hpp"
#include "../hyperloglog/hyperloglog.hpp"

class DistinctStatistics {
public:
    static void Deserialize(field_id_t, Reader&);
};