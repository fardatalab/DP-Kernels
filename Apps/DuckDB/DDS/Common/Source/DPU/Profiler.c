#include "Profiler.h"

#include <stdio.h>

#define BILLION  1000000000L

void InitProfiler(struct Profiler* Prof, size_t Ops) {
    Prof->Operations = Ops;
    Prof->StartTs.tv_sec = 0;
    Prof->StartTs.tv_nsec = 0;
    Prof->StopTs.tv_sec = 0;
    Prof->StopTs.tv_nsec = 0;
}
void StartProfiler(struct Profiler* Prof) {
    if(clock_gettime(CLOCK_REALTIME, &Prof->StartTs) == -1) {
        perror("Unable to take the start timestamp");
    }
}

void StopProfiler(struct Profiler* Prof) {
    if(clock_gettime(CLOCK_REALTIME, &Prof->StopTs) == -1) {
        perror("Unable to take the stop timestamp");
    }
}

double GetProfilerEllapsed(struct Profiler* Prof) {
    double latency = (Prof->StopTs.tv_sec - Prof->StartTs.tv_sec) * (double)BILLION
        + (double)(Prof->StopTs.tv_nsec - Prof->StartTs.tv_nsec);
    latency = latency / 1000.0;
    return latency;
}
void ReportProfiler(struct Profiler* Prof) {
    double latency = GetProfilerEllapsed(Prof);
    printf("Porfiler:\n");
	printf("-- latency: %.2lf seconds\n", latency / 1000000.0);
	printf("-- throughput: %.2lf million op/s\n", Prof->Operations * 1.0 / latency);
}