#include <iostream>

#include "ProfilerCC.h"

using std::cout;
using std::endl;

Profiler::Profiler(size_t _operations) {
	m_operations = _operations;
}

void Profiler::Start() {
	m_start_time = steady_clock::now();
}

void Profiler::Stop() {
	m_end_time = steady_clock::now();
}

double Profiler::Ellapsed() {
	auto dur = duration_cast<microseconds>(m_end_time - m_start_time);
	return (double)dur.count();
}

void Profiler::Report() {
	auto dur = duration_cast<microseconds>(m_end_time - m_start_time);
	cout << "Porfiler:" << endl;
	cout << "-- latency: " << dur.count() / 1000000.0 << " seconds" << endl;
	cout << "-- throughput: " << m_operations * 1.0 / dur.count() << " million op/s" << endl;
}

void Profiler::ReportLatency() {
	auto dur = duration_cast<microseconds>(m_end_time - m_start_time);
	cout << "Porfiler:" << endl;
	cout << "-- latency: " << dur.count() / 1000000.0 << " seconds" << endl;
}

void Profiler::ReportThroughput() {
	auto dur = duration_cast<microseconds>(m_end_time - m_start_time);
	cout << "Porfiler:" << endl;
	cout << "-- throughput: " << m_operations * 1.0 / dur.count() << " million op/s" << endl;
}
