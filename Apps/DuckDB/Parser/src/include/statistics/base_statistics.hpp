#include "../../common.hpp"
#include "../reader/reader.hpp"

// don't care about what BaseStatistics encapsulates,
// only need to ignore it for now
class BaseStatistics {
public:
    static void Deserialize(field_id_t, Reader&);
//    static void Deserialize(field_id_t, LinkedListReader&);
};
