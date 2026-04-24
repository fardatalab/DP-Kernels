#pragma once

#include <time.h>

struct Profiler {
    struct timespec StartTs;
    struct timespec StopTs;
    size_t Operations;
};

void InitProfiler(struct Profiler* Prof, size_t Ops);
void StartProfiler(struct Profiler* Prof);
void StopProfiler(struct Profiler* Prof);
double GetProfilerEllapsed(struct Profiler* Prof);
void ReportProfiler(struct Profiler* Prof);
