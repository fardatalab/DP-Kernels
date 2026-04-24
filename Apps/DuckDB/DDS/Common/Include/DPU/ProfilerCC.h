#pragma once

#include <ctime>
#include <chrono>

using std::chrono::steady_clock;
using std::chrono::duration_cast;
using std::chrono::microseconds;

/**
* A profiler that counts elapsed time and computes opertions/s.
*
*/
class Profiler {
public:
	Profiler(size_t _operations);
	void Start();
	void Stop();
	double Ellapsed();
	void Report();
	void ReportLatency();
	void ReportThroughput();

private:
	steady_clock::time_point m_start_time;
	steady_clock::time_point m_end_time;
	size_t m_operations;
};
