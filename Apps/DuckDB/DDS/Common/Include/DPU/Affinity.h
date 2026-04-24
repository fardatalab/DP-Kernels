#pragma once

#include <sched.h>
#include <pthread.h>

static int BindToCore(size_t core_id) {
    cpu_set_t cpuset;
    
    CPU_ZERO(&cpuset);
    CPU_SET(core_id, &cpuset);
    int rc = pthread_setaffinity_np(
        pthread_self(),
        sizeof(cpu_set_t),
        &cpuset
    );

    return rc;
}
