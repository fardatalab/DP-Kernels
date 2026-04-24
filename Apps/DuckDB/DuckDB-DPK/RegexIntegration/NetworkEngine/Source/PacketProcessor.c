#include "PacketProcessor.h"
#include "/home/ubuntu/dpk-duckdb/Apps/DuckDB/bf3_regex_app.h"
#include "/home/ubuntu/dpk-duckdb/Apps/DuckDB/re2_regex_app.h"

#define RUN_DPKERNEL

#define DUCKDB_DB_FILE_PATH "/home/ubuntu/dbfiles/sf1.db"
#define PIPELINE_DEPTH (0)
#define NUM_THREADS (1)

int ProcessPacketMsg(char *msg, int msg_len) {
    // FIXME: maybe add msg processing logic?
    int ret;
#ifdef RUN_DPKERNEL
    printf("executing regex query using DPK\n");
    ret = !bf3_regex_app_driver(contig_strings, n_bufs, NUM_THREADS, PIPELINE_DEPTH); 
#else
    printf("falling back to RE2 on SoC cores\n");
    ret = !re2_regex_app_driver(DUCKDB_DB_FILE_PATH, contig_strings, n_bufs, NUM_THREADS);
#endif
    return ret;
}
