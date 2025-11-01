#include "regduck_api.hpp"

#define DBFILE "/home/ubuntu/RegDuck/db-files/comment_sf1.db"

int main() {
    char buf[MAX_BATCH_SIZE + 1];
    auto start = chrono::high_resolution_clock::now();
    while (load_strings_in_batch(buf, MAX_BATCH_SIZE)) {
        buf[MAX_BATCH_SIZE] = 0;
        // printf("%s", buf);
        memset(buf, 0, MAX_BATCH_SIZE + 1);
    }
    auto end = chrono::high_resolution_clock::now();
    chrono::duration<double> elapsed = end - start;
    cout << "elapsed: " << elapsed.count() << " sec\n";
    return 0;
}
