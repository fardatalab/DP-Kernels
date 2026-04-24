#include "duckdb/main/query_profiler.hpp"

#include "duckdb/common/fstream.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/tree_renderer/text_tree_renderer.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/operator/helper/physical_execute.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/storage/buffer/buffer_pool.hpp"
#include "yyjson.hpp"

#include <algorithm>
#include <cstdio>
#include <mutex>
#include <thread>
#include <utility>

using namespace duckdb_yyjson; // NOLINT

namespace duckdb {

namespace {
// Reserve a large per-thread buffer to minimize per-call allocations for tail latency tracking.
static constexpr idx_t DDS_PREAD_LATENCY_RESERVE_BYTES = 128ULL * 1024ULL * 1024ULL;
static constexpr idx_t DDS_PREAD_LATENCY_RESERVE_COUNT =
    DDS_PREAD_LATENCY_RESERVE_BYTES / static_cast<idx_t>(sizeof(uint64_t));
// Number of per-thread latency buffer slots to track at most per query.
static constexpr idx_t DDS_PREAD_LATENCY_BUFFER_SLOTS = 4096;
// Thread-local nesting depth for table-scan expression-filter evaluation scopes.
static thread_local idx_t table_filter_expression_scope_depth = 0;
// Thread-local nesting depth for attributing read I/O to string constant-comparison predicates.
static thread_local idx_t table_scan_string_constant_comparison_io_scope_depth = 0;
// Thread-local nesting depth for attributing read I/O to LIKE-related predicates.
static thread_local idx_t table_scan_string_like_operator_io_scope_depth = 0;

enum class Pread2PerTableValueType : uint8_t { THREAD_COUNT, BYTES, TIME_NS };

// Normalizes a table/file path into a stable basename for per-table pread2 metrics.
static string NormalizePread2PerTablePath(const string &table_path) {
	const auto last_sep = table_path.find_last_of("/\\");
	if (last_sep == string::npos) {
		return table_path;
	}
	return table_path.substr(last_sep + 1);
}

// Formats per-table pread2 aggregate metrics as "table_a=V,table_b=V".
static string FormatPread2PerTableMetric(
    const unordered_map<string, QueryMetrics::DDSPosixPread2PerTableStats> &stats,
    Pread2PerTableValueType value_type) {
	if (stats.empty()) {
		return string();
	}
	vector<string> table_names;
	table_names.reserve(stats.size());
	for (const auto &entry : stats) {
		table_names.push_back(entry.first);
	}
	std::sort(table_names.begin(), table_names.end());

	string formatted_metric;
	for (const auto &table_name : table_names) {
		const auto stat_entry = stats.find(table_name);
		if (stat_entry == stats.end()) {
			continue;
		}
		const auto &table_stats = stat_entry->second;
		uint64_t value = 0;
		switch (value_type) {
		case Pread2PerTableValueType::THREAD_COUNT:
			value = NumericCast<uint64_t>(table_stats.thread_ids.size());
			break;
		case Pread2PerTableValueType::BYTES:
			value = table_stats.bytes;
			break;
		case Pread2PerTableValueType::TIME_NS:
			value = table_stats.time_ns;
			break;
		}
		if (!formatted_metric.empty()) {
			formatted_metric += ",";
		}
		formatted_metric += table_name + "=" + std::to_string(value);
	}
	return formatted_metric;
}
} // namespace

QueryProfiler::QueryProfiler(ClientContext &context_p)
    : context(context_p), running(false), query_requires_profiling(false), is_explain_analyze(false) {
}

bool QueryProfiler::IsEnabled() const {
	return is_explain_analyze || ClientConfig::GetConfig(context).enable_profiler;
}

bool QueryProfiler::IsDetailedEnabled() const {
	return !is_explain_analyze && ClientConfig::GetConfig(context).enable_detailed_profiling;
}

ProfilerPrintFormat QueryProfiler::GetPrintFormat(ExplainFormat format) const {
	auto print_format = ClientConfig::GetConfig(context).profiler_print_format;
	switch (format) {
	case ExplainFormat::DEFAULT:
		if (print_format != ProfilerPrintFormat::NO_OUTPUT) {
			return print_format;
		}
		DUCKDB_EXPLICIT_FALLTHROUGH;
	case ExplainFormat::TEXT:
		return ProfilerPrintFormat::QUERY_TREE;
	case ExplainFormat::JSON:
		return ProfilerPrintFormat::JSON;
	case ExplainFormat::HTML:
		return ProfilerPrintFormat::HTML;
	case ExplainFormat::GRAPHVIZ:
		return ProfilerPrintFormat::GRAPHVIZ;
	default:
		throw NotImplementedException("No mapping from ExplainFormat::%s to ProfilerPrintFormat",
		                              EnumUtil::ToString(format));
	}
}

ExplainFormat QueryProfiler::GetExplainFormat(ProfilerPrintFormat format) const {
	switch (format) {
	case ProfilerPrintFormat::QUERY_TREE:
	case ProfilerPrintFormat::QUERY_TREE_OPTIMIZER:
		return ExplainFormat::TEXT;
	case ProfilerPrintFormat::JSON:
		return ExplainFormat::JSON;
	case ProfilerPrintFormat::HTML:
		return ExplainFormat::HTML;
	case ProfilerPrintFormat::GRAPHVIZ:
		return ExplainFormat::GRAPHVIZ;
	case ProfilerPrintFormat::NO_OUTPUT:
		throw InternalException("Should not attempt to get ExplainFormat for ProfilerPrintFormat::NO_OUTPUT");
	default:
		throw NotImplementedException("No mapping from ProfilePrintFormat::%s to ExplainFormat",
		                              EnumUtil::ToString(format));
	}
}

bool QueryProfiler::PrintOptimizerOutput() const {
	return GetPrintFormat() == ProfilerPrintFormat::QUERY_TREE_OPTIMIZER || IsDetailedEnabled();
}

string QueryProfiler::GetSaveLocation() const {
	return is_explain_analyze ? string() : ClientConfig::GetConfig(context).profiler_save_location;
}

QueryProfiler &QueryProfiler::Get(ClientContext &context) {
	return *ClientData::Get(context).profiler;
}

void QueryProfiler::Start(const string &query) {
	Reset();
	running = true;
	query_metrics.query = query;
	query_metrics.latency.Start();
}

// Resets query-level profiler state and counters for a fresh query.
void QueryProfiler::Reset() {
	tree_map.clear();
	root = nullptr;
	phase_timings.clear();
	phase_stack.clear();
	running = false;
	query_metrics.query = "";
	query_metrics.total_bytes_read = 0;
	query_metrics.total_bytes_written = 0;
	query_metrics.parquet_decrypt_time_ns = 0;
	query_metrics.parquet_decrypt_call_count = 0;
	query_metrics.parquet_decompress_time_ns = 0;
	query_metrics.parquet_decompress_call_count = 0;
	query_metrics.dds_pread_time_ns = 0;
	query_metrics.dds_pread2_time_ns = 0;
	query_metrics.dds_pread_bytes = 0;
	query_metrics.dds_pread_call_count = 0;
	query_metrics.dds_pread_in_flight = 0;
	query_metrics.dds_pread_max_in_flight = 0;
	query_metrics.dds_pread_wall_start_ns = 0;
	query_metrics.dds_pread_wall_end_ns = 0;
	query_metrics.offload_parquet_read_time_ns = 0;
	query_metrics.offload_parquet_read_call_count = 0;
	query_metrics.offload_parquet_decrypt_time_ns = 0;
	query_metrics.offload_parquet_decrypt_call_count = 0;
	query_metrics.offload_parquet_decompress_time_ns = 0;
	query_metrics.offload_parquet_decompress_call_count = 0;
	query_metrics.table_scan_string_constant_comparison_time_ns = 0;
	query_metrics.table_scan_string_constant_comparison_count = 0;
	query_metrics.table_scan_string_like_operator_time_ns = 0;
	query_metrics.table_scan_string_like_operator_count = 0;
	query_metrics.table_scan_string_constant_comparison_read_io_time_ns = 0;
	query_metrics.table_scan_string_like_operator_read_io_time_ns = 0;
	// Reset per-table pread2 aggregates for the new query.
	{
		lock_guard<std::mutex> per_table_guard(query_metrics.dds_pread2_per_table_stats_lock);
		query_metrics.dds_pread2_per_table_stats.clear();
	}
	// DDS pread metrics are optional and can be compile-time disabled.
#if defined(DUCKDB_DDS_PREAD_METRICS_ENABLED) && (DUCKDB_DDS_PREAD_METRICS_ENABLED)
	// Original per-thread stats reset (mutex-based); kept for reference.
	// {
	// 	lock_guard<std::mutex> guard(dds_pread_thread_stats_mutex);
	// 	dds_pread_thread_stats.clear();
	// }
	// Reset lock-free per-thread latency buffers for the new query.
	dds_pread_latency_generation.fetch_add(1);
	dds_pread_latency_buffer_count.store(0);
	if (dds_pread_latency_buffers.empty()) {
		dds_pread_latency_buffers.resize(DDS_PREAD_LATENCY_BUFFER_SLOTS, nullptr);
	} else {
		std::fill(dds_pread_latency_buffers.begin(), dds_pread_latency_buffers.end(), nullptr);
	}
#endif
}

void QueryProfiler::StartQuery(const string &query, bool is_explain_analyze_p, bool start_at_optimizer) {
	lock_guard<std::mutex> guard(lock);
	if (is_explain_analyze_p) {
		StartExplainAnalyze();
	}
	if (!IsEnabled()) {
		return;
	}
	if (start_at_optimizer && !PrintOptimizerOutput()) {
		// This is the StartQuery call before the optimizer, but we don't have to print optimizer output
		return;
	}
	if (running) {
		// Called while already running: this should only happen when we print optimizer output
		D_ASSERT(PrintOptimizerOutput());
		return;
	}
	Start(query);
}

bool QueryProfiler::OperatorRequiresProfiling(const PhysicalOperatorType op_type) {
	const auto &config = ClientConfig::GetConfig(context);
	if (config.profiling_coverage == ProfilingCoverage::ALL) {
		return true;
	}

	switch (op_type) {
	case PhysicalOperatorType::ORDER_BY:
	case PhysicalOperatorType::RESERVOIR_SAMPLE:
	case PhysicalOperatorType::STREAMING_SAMPLE:
	case PhysicalOperatorType::LIMIT:
	case PhysicalOperatorType::LIMIT_PERCENT:
	case PhysicalOperatorType::STREAMING_LIMIT:
	case PhysicalOperatorType::TOP_N:
	case PhysicalOperatorType::WINDOW:
	case PhysicalOperatorType::UNNEST:
	case PhysicalOperatorType::UNGROUPED_AGGREGATE:
	case PhysicalOperatorType::HASH_GROUP_BY:
	case PhysicalOperatorType::FILTER:
	case PhysicalOperatorType::PROJECTION:
	case PhysicalOperatorType::COPY_TO_FILE:
	case PhysicalOperatorType::TABLE_SCAN:
	case PhysicalOperatorType::CHUNK_SCAN:
	case PhysicalOperatorType::DELIM_SCAN:
	case PhysicalOperatorType::EXPRESSION_SCAN:
	case PhysicalOperatorType::BLOCKWISE_NL_JOIN:
	case PhysicalOperatorType::NESTED_LOOP_JOIN:
	case PhysicalOperatorType::HASH_JOIN:
	case PhysicalOperatorType::CROSS_PRODUCT:
	case PhysicalOperatorType::PIECEWISE_MERGE_JOIN:
	case PhysicalOperatorType::IE_JOIN:
	case PhysicalOperatorType::LEFT_DELIM_JOIN:
	case PhysicalOperatorType::RIGHT_DELIM_JOIN:
	case PhysicalOperatorType::UNION:
	case PhysicalOperatorType::RECURSIVE_CTE:
	case PhysicalOperatorType::RECURSIVE_KEY_CTE:
	case PhysicalOperatorType::EMPTY_RESULT:
	case PhysicalOperatorType::EXTENSION:
		return true;
	default:
		return false;
	}
}

void QueryProfiler::Finalize(ProfilingNode &node) {
	for (idx_t i = 0; i < node.GetChildCount(); i++) {
		auto child = node.GetChild(i);
		Finalize(*child);

		auto &info = node.GetProfilingInfo();
		auto type = PhysicalOperatorType(info.GetMetricValue<uint8_t>(MetricsType::OPERATOR_TYPE));
		if (type == PhysicalOperatorType::UNION &&
		    info.Enabled(info.expanded_settings, MetricsType::OPERATOR_CARDINALITY)) {

			auto &child_info = child->GetProfilingInfo();
			auto value = child_info.metrics[MetricsType::OPERATOR_CARDINALITY].GetValue<idx_t>();
			info.MetricSum(MetricsType::OPERATOR_CARDINALITY, value);
		}
	}
}

void QueryProfiler::StartExplainAnalyze() {
	is_explain_analyze = true;
}

template <class METRIC_TYPE>
static void AggregateMetric(ProfilingNode &node, MetricsType aggregated_metric, MetricsType child_metric,
                            const std::function<METRIC_TYPE(const METRIC_TYPE &, const METRIC_TYPE &)> &update_fun) {
	auto &info = node.GetProfilingInfo();
	info.metrics[aggregated_metric] = info.metrics[child_metric];

	for (idx_t i = 0; i < node.GetChildCount(); i++) {
		auto child = node.GetChild(i);
		AggregateMetric<METRIC_TYPE>(*child, aggregated_metric, child_metric, update_fun);

		auto &child_info = child->GetProfilingInfo();
		auto value = child_info.GetMetricValue<METRIC_TYPE>(aggregated_metric);
		info.MetricUpdate<METRIC_TYPE>(aggregated_metric, value, update_fun);
	}
}

template <class METRIC_TYPE>
static void GetCumulativeMetric(ProfilingNode &node, MetricsType cumulative_metric, MetricsType child_metric) {
	AggregateMetric<METRIC_TYPE>(
	    node, cumulative_metric, child_metric,
	    [](const METRIC_TYPE &old_value, const METRIC_TYPE &new_value) { return old_value + new_value; });
}

Value GetCumulativeOptimizers(ProfilingNode &node) {
	auto &metrics = node.GetProfilingInfo().metrics;
	double count = 0;
	for (auto &metric : metrics) {
		if (MetricsUtils::IsOptimizerMetric(metric.first)) {
			count += metric.second.GetValue<double>();
		}
	}
	return Value::CreateValue(count);
}

// Finalizes query metrics and emits profiling output if enabled (including DDS pread call count and tail latencies).
void QueryProfiler::EndQuery() {
	unique_lock<std::mutex> guard(lock);
	if (!IsEnabled() || !running) {
		return;
	}

	query_metrics.latency.End();
	if (root) {
		auto &info = root->GetProfilingInfo();
		if (info.Enabled(info.expanded_settings, MetricsType::OPERATOR_CARDINALITY)) {
			Finalize(*root->GetChild(0));
		}
	}
	running = false;
	bool emit_output = false;

	// Print or output the query profiling after query termination.
	// EXPLAIN ANALYZE output is not written by the profiler.
	if (IsEnabled() && !is_explain_analyze) {
		if (root) {
			auto &info = root->GetProfilingInfo();
			info = ProfilingInfo(ClientConfig::GetConfig(context).profiler_settings);
			auto &child_info = root->children[0]->GetProfilingInfo();
			info.metrics[MetricsType::QUERY_NAME] = query_metrics.query;

			auto &settings = info.expanded_settings;
			for (const auto &global_info_entry : query_metrics.query_global_info.metrics) {
				info.metrics[global_info_entry.first] = global_info_entry.second;
			}
			if (info.Enabled(settings, MetricsType::LATENCY)) {
				info.metrics[MetricsType::LATENCY] = query_metrics.latency.Elapsed();
			}
			if (info.Enabled(settings, MetricsType::TOTAL_BYTES_READ)) {
				info.metrics[MetricsType::TOTAL_BYTES_READ] = Value::UBIGINT(query_metrics.total_bytes_read);
			}
			if (info.Enabled(settings, MetricsType::TOTAL_BYTES_WRITTEN)) {
				info.metrics[MetricsType::TOTAL_BYTES_WRITTEN] = Value::UBIGINT(query_metrics.total_bytes_written);
			}
			// DDS pread metrics are optional and can be compile-time disabled.
#if defined(DUCKDB_DDS_PREAD_METRICS_ENABLED) && (DUCKDB_DDS_PREAD_METRICS_ENABLED)
				if (info.Enabled(settings, MetricsType::DDS_PREAD_TIME)) {
					info.metrics[MetricsType::DDS_PREAD_TIME] =
					    Value::DOUBLE(static_cast<double>(query_metrics.dds_pread_time_ns.load()) * 1e-9);
				}
				if (info.Enabled(settings, MetricsType::DDS_PREAD2_TIME)) {
					info.metrics[MetricsType::DDS_PREAD2_TIME] =
					    Value::DOUBLE(static_cast<double>(query_metrics.dds_pread2_time_ns.load()) * 1e-9);
				}
				const bool need_pread2_per_table_metrics =
				    info.Enabled(settings, MetricsType::PREAD2_PER_TABLE_THREADS) ||
				    info.Enabled(settings, MetricsType::PREAD2_PER_TABLE_BYTES) ||
				    info.Enabled(settings, MetricsType::PREAD2_PER_TABLE_TIME);
				string per_table_threads;
				string per_table_bytes;
				string per_table_time;
				if (need_pread2_per_table_metrics) {
					lock_guard<std::mutex> per_table_guard(query_metrics.dds_pread2_per_table_stats_lock);
					per_table_threads =
					    FormatPread2PerTableMetric(query_metrics.dds_pread2_per_table_stats,
					                               Pread2PerTableValueType::THREAD_COUNT);
					per_table_bytes =
					    FormatPread2PerTableMetric(query_metrics.dds_pread2_per_table_stats,
					                               Pread2PerTableValueType::BYTES);
					per_table_time =
					    FormatPread2PerTableMetric(query_metrics.dds_pread2_per_table_stats,
					                               Pread2PerTableValueType::TIME_NS);
				}
				if (info.Enabled(settings, MetricsType::PREAD2_PER_TABLE_THREADS)) {
					info.metrics[MetricsType::PREAD2_PER_TABLE_THREADS] = Value::CreateValue(per_table_threads);
				}
				if (info.Enabled(settings, MetricsType::PREAD2_PER_TABLE_BYTES)) {
					info.metrics[MetricsType::PREAD2_PER_TABLE_BYTES] = Value::CreateValue(per_table_bytes);
				}
				if (info.Enabled(settings, MetricsType::PREAD2_PER_TABLE_TIME)) {
					info.metrics[MetricsType::PREAD2_PER_TABLE_TIME] = Value::CreateValue(per_table_time);
				}
				if (info.Enabled(settings, MetricsType::DDS_PREAD_LATENCY)) {
					double latency_seconds = 0.0;
					const auto call_count = query_metrics.dds_pread_call_count.load();
					if (call_count != 0) {
						latency_seconds = static_cast<double>(query_metrics.dds_pread_time_ns.load()) * 1e-9 /
						                  static_cast<double>(call_count);
					}
					info.metrics[MetricsType::DDS_PREAD_LATENCY] = Value::DOUBLE(latency_seconds);
				}
				if (info.Enabled(settings, MetricsType::DDS_PREAD_CALL_COUNT)) {
					info.metrics[MetricsType::DDS_PREAD_CALL_COUNT] =
					    Value::UBIGINT(query_metrics.dds_pread_call_count.load());
				}
			// Original tail-latency aggregation (min/max/p99 + ad-hoc p50 print) kept for reference.
			// const bool need_tail_latency =
			//     info.Enabled(settings, MetricsType::DDS_PREAD_MIN_LATENCY) ||
			//     info.Enabled(settings, MetricsType::DDS_PREAD_MAX_LATENCY) ||
			//     info.Enabled(settings, MetricsType::DDS_PREAD_P99_LATENCY) ||
			//     (query_metrics.dds_pread_call_count.load() > 0);
			// double min_latency_seconds = 0.0;
			// double max_latency_seconds = 0.0;
			// double p99_latency_seconds = 0.0;
			// if (need_tail_latency) {
			// 	uint64_t min_latency_ns = NumericLimits<uint64_t>::Maximum();
			// 	uint64_t max_latency_ns = 0;
			// 	idx_t total_latency_count = 0;
			// 	// Note: only registered buffers (up to DDS_PREAD_LATENCY_BUFFER_SLOTS) are aggregated here.
			// 	auto buffer_limit = MinValue<idx_t>(dds_pread_latency_buffer_count.load(),
			// 	                                    dds_pread_latency_buffers.size());
			// 	for (idx_t idx = 0; idx < buffer_limit; idx++) {
			// 		auto *buffer = dds_pread_latency_buffers[idx];
			// 		if (!buffer || buffer->latencies_ns.empty()) {
			// 			continue;
			// 		}
			// 		total_latency_count += buffer->latencies_ns.size();
			// 	}
			// 	std::vector<uint64_t> combined_latencies;
			// 	if (total_latency_count > 0) {
			// 		combined_latencies.reserve(total_latency_count);
			// 		for (idx_t idx = 0; idx < buffer_limit; idx++) {
			// 			auto *buffer = dds_pread_latency_buffers[idx];
			// 			if (!buffer || buffer->latencies_ns.empty()) {
			// 				continue;
			// 			}
			// 			auto &latencies = buffer->latencies_ns;
			// 			auto local_minmax = std::minmax_element(latencies.begin(), latencies.end());
			// 			min_latency_ns = MinValue<uint64_t>(min_latency_ns, *local_minmax.first);
			// 			max_latency_ns = MaxValue<uint64_t>(max_latency_ns, *local_minmax.second);
			// 			combined_latencies.insert(combined_latencies.end(), latencies.begin(), latencies.end());
			// 		}
			// 	}
			// 	if (!combined_latencies.empty()) {
			// 		auto p99_index = static_cast<idx_t>((combined_latencies.size() - 1) * 99 / 100);
			// 		auto p50_index = static_cast<idx_t>((combined_latencies.size() - 1) * 50 / 100);
			// 		double p50_latency_seconds = static_cast<double>(combined_latencies[p50_index]) * 1e-9;
			// 		printf("[DDS-IO] latency p50=%.6f seconds\n", p50_latency_seconds);
			// 		// also print the number of preads in total
			// 		printf("[DDS-IO] total DDSPosix::pread calls=%llu\n",
			// 		       static_cast<unsigned long long>(query_metrics.dds_pread_call_count.load()));
			//
			// 		std::nth_element(combined_latencies.begin(),
			// 		                 combined_latencies.begin() + NumericCast<idx_t>(p99_index),
			// 		                 combined_latencies.end());
			// 		p99_latency_seconds = static_cast<double>(combined_latencies[p99_index]) * 1e-9;
			// 		min_latency_seconds = static_cast<double>(min_latency_ns) * 1e-9;
			// 		max_latency_seconds = static_cast<double>(max_latency_ns) * 1e-9;
			// 	}
			// }
			// if (info.Enabled(settings, MetricsType::DDS_PREAD_MIN_LATENCY)) {
			// 	info.metrics[MetricsType::DDS_PREAD_MIN_LATENCY] = Value::DOUBLE(min_latency_seconds);
			// }
			// if (info.Enabled(settings, MetricsType::DDS_PREAD_MAX_LATENCY)) {
			// 	info.metrics[MetricsType::DDS_PREAD_MAX_LATENCY] = Value::DOUBLE(max_latency_seconds);
			// }
			// if (info.Enabled(settings, MetricsType::DDS_PREAD_P99_LATENCY)) {
			// 	info.metrics[MetricsType::DDS_PREAD_P99_LATENCY] = Value::DOUBLE(p99_latency_seconds);
			// }

			const bool need_tail_latency =
			    info.Enabled(settings, MetricsType::DDS_PREAD_MIN_LATENCY) ||
			    info.Enabled(settings, MetricsType::DDS_PREAD_MAX_LATENCY) ||
			    info.Enabled(settings, MetricsType::DDS_PREAD_P50_LATENCY) ||
			    info.Enabled(settings, MetricsType::DDS_PREAD_P99_LATENCY) ||
			    (query_metrics.dds_pread_call_count.load() > 0);
			double min_latency_seconds = 0.0;
			double max_latency_seconds = 0.0;
			double p50_latency_seconds = 0.0;
			double p99_latency_seconds = 0.0;
			if (need_tail_latency) {
				uint64_t min_latency_ns = NumericLimits<uint64_t>::Maximum();
				uint64_t max_latency_ns = 0;
				idx_t total_latency_count = 0;
				// Note: only registered buffers (up to DDS_PREAD_LATENCY_BUFFER_SLOTS) are aggregated here.
				auto buffer_limit = MinValue<idx_t>(dds_pread_latency_buffer_count.load(),
				                                    dds_pread_latency_buffers.size());
				for (idx_t idx = 0; idx < buffer_limit; idx++) {
					auto *buffer = dds_pread_latency_buffers[idx];
					if (!buffer || buffer->latencies_ns.empty()) {
						continue;
					}
					total_latency_count += buffer->latencies_ns.size();
				}
				std::vector<uint64_t> combined_latencies;
				if (total_latency_count > 0) {
					combined_latencies.reserve(total_latency_count);
					for (idx_t idx = 0; idx < buffer_limit; idx++) {
						auto *buffer = dds_pread_latency_buffers[idx];
						if (!buffer || buffer->latencies_ns.empty()) {
							continue;
						}
						auto &latencies = buffer->latencies_ns;
						auto local_minmax = std::minmax_element(latencies.begin(), latencies.end());
						min_latency_ns = MinValue<uint64_t>(min_latency_ns, *local_minmax.first);
						max_latency_ns = MaxValue<uint64_t>(max_latency_ns, *local_minmax.second);
						combined_latencies.insert(combined_latencies.end(), latencies.begin(), latencies.end());
					}
				}
				if (!combined_latencies.empty()) {
					const auto p50_index = static_cast<idx_t>((combined_latencies.size() - 1) * 50 / 100);
					const auto p99_index = static_cast<idx_t>((combined_latencies.size() - 1) * 99 / 100);
					// P50 and P99 are computed with nth_element to avoid full sort cost.
					std::nth_element(combined_latencies.begin(),
					                 combined_latencies.begin() + NumericCast<idx_t>(p50_index),
					                 combined_latencies.end());
					p50_latency_seconds = static_cast<double>(combined_latencies[p50_index]) * 1e-9;
					std::nth_element(combined_latencies.begin(),
					                 combined_latencies.begin() + NumericCast<idx_t>(p99_index),
					                 combined_latencies.end());
					p99_latency_seconds = static_cast<double>(combined_latencies[p99_index]) * 1e-9;
					min_latency_seconds = static_cast<double>(min_latency_ns) * 1e-9;
					max_latency_seconds = static_cast<double>(max_latency_ns) * 1e-9;
				}
			}
			if (info.Enabled(settings, MetricsType::DDS_PREAD_MIN_LATENCY)) {
				info.metrics[MetricsType::DDS_PREAD_MIN_LATENCY] = Value::DOUBLE(min_latency_seconds);
			}
			if (info.Enabled(settings, MetricsType::DDS_PREAD_MAX_LATENCY)) {
				info.metrics[MetricsType::DDS_PREAD_MAX_LATENCY] = Value::DOUBLE(max_latency_seconds);
			}
			if (info.Enabled(settings, MetricsType::DDS_PREAD_P50_LATENCY)) {
				info.metrics[MetricsType::DDS_PREAD_P50_LATENCY] = Value::DOUBLE(p50_latency_seconds);
			}
			if (info.Enabled(settings, MetricsType::DDS_PREAD_P99_LATENCY)) {
				info.metrics[MetricsType::DDS_PREAD_P99_LATENCY] = Value::DOUBLE(p99_latency_seconds);
			}
			const bool need_throughput = info.Enabled(settings, MetricsType::DDS_PREAD_THREAD_THROUGHPUT) ||
			                             info.Enabled(settings, MetricsType::DDS_PREAD_TOTAL_THROUGHPUT);
			double thread_throughput = 0.0;
			double aggregate_throughput = 0.0;
			if (need_throughput) {
				// Previous throughput calculation used summed pread time (not wall clock).
				// auto total_time_ns = static_cast<double>(query_metrics.dds_pread_time_ns.load());
				// auto total_bytes = static_cast<double>(query_metrics.dds_pread_bytes.load());
				// if (total_time_ns > 0.0) {
				// 	aggregate_throughput = total_bytes / total_time_ns * 1e9;
				// }
				// double throughput_sum = 0.0;
				// idx_t thread_count = 0;
				// {
				// 	lock_guard<std::mutex> guard(dds_pread_thread_stats_mutex);
				// 	for (auto &entry : dds_pread_thread_stats) {
				// 		if (entry.second.time_ns == 0) {
				// 			continue;
				// 		}
				// 		throughput_sum += static_cast<double>(entry.second.bytes) /
				// 		                  static_cast<double>(entry.second.time_ns) * 1e9;
				// 		thread_count++;
				// 	}
				// }
				// if (thread_count > 0) {
				// 	thread_throughput = throughput_sum / static_cast<double>(thread_count);
				// }

				// throughput calculation uses wall clock time.
				const auto total_bytes = static_cast<double>(query_metrics.dds_pread_bytes.load());
				const auto wall_start = query_metrics.dds_pread_wall_start_ns.load();
				const auto wall_end = query_metrics.dds_pread_wall_end_ns.load();
				if (wall_start != 0 && wall_end > wall_start) {
					const auto total_wall_ns = static_cast<double>(wall_end - wall_start);
					aggregate_throughput = total_bytes / total_wall_ns * 1e9;
				}
				double throughput_sum = 0.0;
				idx_t thread_count = 0;
				auto buffer_limit = MinValue<idx_t>(dds_pread_latency_buffer_count.load(),
				                                    dds_pread_latency_buffers.size());
				for (idx_t idx = 0; idx < buffer_limit; idx++) {
					auto *buffer = dds_pread_latency_buffers[idx];
					if (!buffer) {
						continue;
					}
					const auto thread_start = buffer->wall_start_ns;
					const auto thread_end = buffer->wall_end_ns;
					if (thread_start == 0 || thread_end <= thread_start) {
						continue;
					}
					const auto thread_wall_ns = static_cast<double>(thread_end - thread_start);
					throughput_sum += static_cast<double>(buffer->bytes) / thread_wall_ns * 1e9;
					thread_count++;
				}
				if (thread_count > 0) {
					thread_throughput = throughput_sum / static_cast<double>(thread_count);
				}
			}
			if (info.Enabled(settings, MetricsType::DDS_PREAD_THREAD_THROUGHPUT)) {
				info.metrics[MetricsType::DDS_PREAD_THREAD_THROUGHPUT] = Value::DOUBLE(thread_throughput);
			}
			if (info.Enabled(settings, MetricsType::DDS_PREAD_TOTAL_THROUGHPUT)) {
				info.metrics[MetricsType::DDS_PREAD_TOTAL_THROUGHPUT] = Value::DOUBLE(aggregate_throughput);
			}
			// Emit a debug print for the max concurrent DDS pread threads when DDS pread was used.
			if (query_metrics.dds_pread_call_count.load() > 0) {
				// Original tail-latency log (min/max/p99) kept for reference.
				// printf("[DDS-IO] latency min=%.6f max=%.6f p99=%.6f (seconds)\n", min_latency_seconds,
				//        max_latency_seconds, p99_latency_seconds);
				printf("[DDS-IO] latency min=%.6f max=%.6f p50=%.6f p99=%.6f (seconds)\n",
				       min_latency_seconds, max_latency_seconds, p50_latency_seconds, p99_latency_seconds);
				printf("[DDS-IO] max concurrent DDSPosix::pread threads=%llu\n",
				       static_cast<unsigned long long>(query_metrics.dds_pread_max_in_flight.load()));
			}
#endif
			// Added parquet crypto/codec metrics to the query-global output.
			if (info.Enabled(settings, MetricsType::PARQUET_DECRYPTION_TIME)) {
				info.metrics[MetricsType::PARQUET_DECRYPTION_TIME] =
				    Value::DOUBLE(static_cast<double>(query_metrics.parquet_decrypt_time_ns) * 1e-9);
			}
			if (info.Enabled(settings, MetricsType::PARQUET_DECRYPTION_COUNT)) {
				info.metrics[MetricsType::PARQUET_DECRYPTION_COUNT] =
				    Value::UBIGINT(query_metrics.parquet_decrypt_call_count);
			}
			if (info.Enabled(settings, MetricsType::PARQUET_DECOMPRESSION_TIME)) {
				info.metrics[MetricsType::PARQUET_DECOMPRESSION_TIME] =
				    Value::DOUBLE(static_cast<double>(query_metrics.parquet_decompress_time_ns) * 1e-9);
			}
			if (info.Enabled(settings, MetricsType::PARQUET_DECOMPRESSION_COUNT)) {
				info.metrics[MetricsType::PARQUET_DECOMPRESSION_COUNT] =
				    Value::UBIGINT(query_metrics.parquet_decompress_call_count);
			}
			if (info.Enabled(settings, MetricsType::OFFLOAD_PARQUET_READ_TIME)) {
				info.metrics[MetricsType::OFFLOAD_PARQUET_READ_TIME] =
				    Value::DOUBLE(static_cast<double>(query_metrics.offload_parquet_read_time_ns) * 1e-9);
			}
			if (info.Enabled(settings, MetricsType::OFFLOAD_PARQUET_READ_COUNT)) {
				info.metrics[MetricsType::OFFLOAD_PARQUET_READ_COUNT] =
				    Value::UBIGINT(query_metrics.offload_parquet_read_call_count);
			}
			if (info.Enabled(settings, MetricsType::OFFLOAD_PARQUET_DECRYPT_TIME)) {
				info.metrics[MetricsType::OFFLOAD_PARQUET_DECRYPT_TIME] =
				    Value::DOUBLE(static_cast<double>(query_metrics.offload_parquet_decrypt_time_ns) * 1e-9);
			}
			if (info.Enabled(settings, MetricsType::OFFLOAD_PARQUET_DECRYPT_COUNT)) {
				info.metrics[MetricsType::OFFLOAD_PARQUET_DECRYPT_COUNT] =
				    Value::UBIGINT(query_metrics.offload_parquet_decrypt_call_count);
			}
			if (info.Enabled(settings, MetricsType::OFFLOAD_PARQUET_DECOMPRESS_TIME)) {
				info.metrics[MetricsType::OFFLOAD_PARQUET_DECOMPRESS_TIME] =
				    Value::DOUBLE(static_cast<double>(query_metrics.offload_parquet_decompress_time_ns) * 1e-9);
			}
			if (info.Enabled(settings, MetricsType::OFFLOAD_PARQUET_DECOMPRESS_COUNT)) {
				info.metrics[MetricsType::OFFLOAD_PARQUET_DECOMPRESS_COUNT] =
				    Value::UBIGINT(query_metrics.offload_parquet_decompress_call_count);
			}
			if (info.Enabled(settings, MetricsType::TABLE_SCAN_STRING_CONSTANT_COMPARISON_TIME)) {
				info.metrics[MetricsType::TABLE_SCAN_STRING_CONSTANT_COMPARISON_TIME] = Value::DOUBLE(
				    static_cast<double>(query_metrics.table_scan_string_constant_comparison_time_ns.load()) * 1e-9);
			}
			if (info.Enabled(settings, MetricsType::TABLE_SCAN_STRING_CONSTANT_COMPARISON_COUNT)) {
				info.metrics[MetricsType::TABLE_SCAN_STRING_CONSTANT_COMPARISON_COUNT] =
				    Value::UBIGINT(query_metrics.table_scan_string_constant_comparison_count.load());
			}
			if (info.Enabled(settings, MetricsType::TABLE_SCAN_STRING_LIKE_OPERATOR_TIME)) {
				info.metrics[MetricsType::TABLE_SCAN_STRING_LIKE_OPERATOR_TIME] =
				    Value::DOUBLE(static_cast<double>(query_metrics.table_scan_string_like_operator_time_ns.load()) *
				                  1e-9);
			}
			if (info.Enabled(settings, MetricsType::TABLE_SCAN_STRING_LIKE_OPERATOR_COUNT)) {
				info.metrics[MetricsType::TABLE_SCAN_STRING_LIKE_OPERATOR_COUNT] =
				    Value::UBIGINT(query_metrics.table_scan_string_like_operator_count.load());
			}
			if (info.Enabled(settings, MetricsType::TABLE_SCAN_STRING_CONSTANT_COMPARISON_READ_IO_TIME)) {
				info.metrics[MetricsType::TABLE_SCAN_STRING_CONSTANT_COMPARISON_READ_IO_TIME] = Value::DOUBLE(
				    static_cast<double>(query_metrics.table_scan_string_constant_comparison_read_io_time_ns.load()) *
				    1e-9);
			}
			if (info.Enabled(settings, MetricsType::TABLE_SCAN_STRING_LIKE_OPERATOR_READ_IO_TIME)) {
				info.metrics[MetricsType::TABLE_SCAN_STRING_LIKE_OPERATOR_READ_IO_TIME] = Value::DOUBLE(
				    static_cast<double>(query_metrics.table_scan_string_like_operator_read_io_time_ns.load()) * 1e-9);
			}
			if (info.Enabled(settings, MetricsType::ROWS_RETURNED)) {
				info.metrics[MetricsType::ROWS_RETURNED] = child_info.metrics[MetricsType::OPERATOR_CARDINALITY];
			}
			if (info.Enabled(settings, MetricsType::CPU_TIME)) {
				GetCumulativeMetric<double>(*root, MetricsType::CPU_TIME, MetricsType::OPERATOR_TIMING);
			}
			if (info.Enabled(settings, MetricsType::CUMULATIVE_CARDINALITY)) {
				GetCumulativeMetric<idx_t>(*root, MetricsType::CUMULATIVE_CARDINALITY,
				                           MetricsType::OPERATOR_CARDINALITY);
			}
			if (info.Enabled(settings, MetricsType::CUMULATIVE_ROWS_SCANNED)) {
				GetCumulativeMetric<idx_t>(*root, MetricsType::CUMULATIVE_ROWS_SCANNED,
				                           MetricsType::OPERATOR_ROWS_SCANNED);
			}
			if (info.Enabled(settings, MetricsType::RESULT_SET_SIZE)) {
				info.metrics[MetricsType::RESULT_SET_SIZE] = child_info.metrics[MetricsType::RESULT_SET_SIZE];
			}

			MoveOptimizerPhasesToRoot();
			if (info.Enabled(settings, MetricsType::CUMULATIVE_OPTIMIZER_TIMING)) {
				info.metrics.at(MetricsType::CUMULATIVE_OPTIMIZER_TIMING) = GetCumulativeOptimizers(*root);
			}
		}

		if (ClientConfig::GetConfig(context).emit_profiler_output) {
			emit_output = true;
		}
	}

	is_explain_analyze = false;

	guard.unlock();

	if (emit_output) {
		string tree = ToString();
		auto save_location = GetSaveLocation();

		if (save_location.empty()) {
			Printer::Print(tree);
			Printer::Print("\n");
		} else {
			WriteToFile(save_location.c_str(), tree);
		}
	}
}

void QueryProfiler::AddBytesRead(const idx_t nr_bytes) {
	if (IsEnabled()) {
		query_metrics.total_bytes_read += nr_bytes;
	}
}

void QueryProfiler::AddBytesWritten(const idx_t nr_bytes) {
	if (IsEnabled()) {
		query_metrics.total_bytes_written += nr_bytes;
	}
}

// Records the time spent decrypting parquet data for this query.
void QueryProfiler::AddParquetDecryptionMetrics(uint64_t elapsed_ns) {
	if (IsEnabled()) {
		query_metrics.parquet_decrypt_time_ns += elapsed_ns;
		query_metrics.parquet_decrypt_call_count++;
	}
}

// Records the time spent decompressing parquet data for this query.
void QueryProfiler::AddParquetDecompressionMetrics(uint64_t elapsed_ns) {
	if (IsEnabled()) {
		query_metrics.parquet_decompress_time_ns += elapsed_ns;
		query_metrics.parquet_decompress_call_count++;
	}
}

// Records time spent in offloaded parquet stage0 reads.
void QueryProfiler::AddOffloadParquetReadMetrics(uint64_t elapsed_ns) {
	if (IsEnabled()) {
		query_metrics.offload_parquet_read_time_ns += elapsed_ns;
		query_metrics.offload_parquet_read_call_count++;
	}
}

// Records time spent in offloaded parquet stage1 decrypt operations.
void QueryProfiler::AddOffloadParquetDecryptMetrics(uint64_t elapsed_ns) {
	if (IsEnabled()) {
		query_metrics.offload_parquet_decrypt_time_ns += elapsed_ns;
		query_metrics.offload_parquet_decrypt_call_count++;
	}
}

// Records time spent in offloaded parquet stage2 decompress operations.
void QueryProfiler::AddOffloadParquetDecompressMetrics(uint64_t elapsed_ns) {
	if (IsEnabled()) {
		query_metrics.offload_parquet_decompress_time_ns += elapsed_ns;
		query_metrics.offload_parquet_decompress_call_count++;
	}
}

// Records time spent evaluating table-scan string constant-comparison predicates.
void QueryProfiler::AddTableScanStringConstantComparisonMetrics(uint64_t elapsed_ns) {
	if (IsEnabled()) {
		query_metrics.table_scan_string_constant_comparison_time_ns += elapsed_ns;
		query_metrics.table_scan_string_constant_comparison_count++;
	}
}

// Records time spent evaluating table-scan LIKE-related predicates.
void QueryProfiler::AddTableScanStringLikeOperatorMetrics(uint64_t elapsed_ns) {
	if (IsEnabled()) {
		query_metrics.table_scan_string_like_operator_time_ns += elapsed_ns;
		query_metrics.table_scan_string_like_operator_count++;
	}
}

void QueryProfiler::AddTableScanStringConstantComparisonReadIOMetrics(uint64_t elapsed_ns) {
	if (IsEnabled()) {
		query_metrics.table_scan_string_constant_comparison_read_io_time_ns += elapsed_ns;
	}
}

void QueryProfiler::AddTableScanStringLikeOperatorReadIOMetrics(uint64_t elapsed_ns) {
	if (IsEnabled()) {
		query_metrics.table_scan_string_like_operator_read_io_time_ns += elapsed_ns;
	}
}

// Aggregates DDSPosix pread timing/byte metrics for the current query.
void QueryProfiler::AddDDSPosixPreadMetrics(uint64_t elapsed_ns, uint64_t bytes, uint64_t wall_start_ns,
                                            uint64_t wall_end_ns) {
	// DDS pread metrics are optional and can be compile-time disabled.
#if defined(DUCKDB_DDS_PREAD_METRICS_ENABLED) && (DUCKDB_DDS_PREAD_METRICS_ENABLED)
	if (!IsEnabled()) {
		return;
	}
	// Previous implementation used only elapsed time/bytes, which is still needed for latency.
	// query_metrics.dds_pread_time_ns += elapsed_ns;
	// query_metrics.dds_pread_bytes += bytes;
	// query_metrics.dds_pread_call_count++;
	query_metrics.dds_pread_time_ns += elapsed_ns;
	query_metrics.dds_pread_bytes += bytes;
	query_metrics.dds_pread_call_count++;

	// Track wall clock window for the full query.
	auto current_start = query_metrics.dds_pread_wall_start_ns.load();
	if (current_start == 0 || wall_start_ns < current_start) {
		query_metrics.dds_pread_wall_start_ns.compare_exchange_weak(current_start, wall_start_ns);
	}
	auto current_end = query_metrics.dds_pread_wall_end_ns.load();
	if (wall_end_ns > current_end) {
		query_metrics.dds_pread_wall_end_ns.compare_exchange_weak(current_end, wall_end_ns);
	}

	// Original per-thread map update (mutex-based); kept for reference.
	// lock_guard<std::mutex> guard(dds_pread_thread_stats_mutex);
	// auto &stats = dds_pread_thread_stats[std::this_thread::get_id()];
	// stats.bytes += bytes;
	// stats.time_ns += elapsed_ns;
	// if (stats.wall_start_ns == 0 || wall_start_ns < stats.wall_start_ns) {
	// 	stats.wall_start_ns = wall_start_ns;
	// }
	// if (wall_end_ns > stats.wall_end_ns) {
	// 	stats.wall_end_ns = wall_end_ns;
	// }

	// Lock-free per-thread buffer update for tail latency and throughput.
	struct DDSPosixPreadThreadLocalState {
		explicit DDSPosixPreadThreadLocalState(idx_t reserve_count)
		    : buffer(reserve_count), generation(0), registered(false), registered_in_global_list(false) {
		}
		QueryProfiler::DDSPosixPreadLatencyBuffer buffer;
		uint64_t generation;
		bool registered;
		bool registered_in_global_list;
	};
	static thread_local DDSPosixPreadThreadLocalState local_state(DDS_PREAD_LATENCY_RESERVE_COUNT);
	const auto current_generation = dds_pread_latency_generation.load();
	if (local_state.generation != current_generation) {
		// New query: reset thread-local buffer without reallocating the backing store.
		local_state.buffer.Reset();
		local_state.generation = current_generation;
		local_state.registered = false;
		local_state.registered_in_global_list = false;
	}
	if (!local_state.registered) {
		// Register this thread's buffer once per query using a lock-free slot assignment.
		const auto slot = dds_pread_latency_buffer_count.fetch_add(1);
		if (slot < dds_pread_latency_buffers.size()) {
			dds_pread_latency_buffers[slot] = &local_state.buffer;
			local_state.registered_in_global_list = true;
		} else {
			// If we run out of slots, keep collecting locally but skip global aggregation.
			local_state.registered_in_global_list = false;
		}
		local_state.registered = true;
	}
	// Always record the latency sample locally; aggregation happens at EndQuery.
	local_state.buffer.latencies_ns.push_back(elapsed_ns);
	local_state.buffer.bytes += bytes;
	local_state.buffer.time_ns += elapsed_ns;
	if (local_state.buffer.wall_start_ns == 0 || wall_start_ns < local_state.buffer.wall_start_ns) {
		local_state.buffer.wall_start_ns = wall_start_ns;
	}
	if (wall_end_ns > local_state.buffer.wall_end_ns) {
		local_state.buffer.wall_end_ns = wall_end_ns;
	}
#else
	(void)elapsed_ns;
	(void)bytes;
	(void)wall_start_ns;
	(void)wall_end_ns;
#endif
}

// Aggregates DDSPosix pread2 timing and per-table metrics for the current query.
void QueryProfiler::AddDDSPosixPread2Metrics(const string &table_path, uint64_t bytes, uint64_t elapsed_ns) {
	// DDS pread metrics are optional and can be compile-time disabled.
#if defined(DUCKDB_DDS_PREAD_METRICS_ENABLED) && (DUCKDB_DDS_PREAD_METRICS_ENABLED)
	if (!IsEnabled()) {
		return;
	}
	query_metrics.dds_pread2_time_ns += elapsed_ns;
	const auto table_name = NormalizePread2PerTablePath(table_path);
	lock_guard<std::mutex> per_table_guard(query_metrics.dds_pread2_per_table_stats_lock);
	auto &table_stats = query_metrics.dds_pread2_per_table_stats[table_name];
	table_stats.bytes += bytes;
	table_stats.time_ns += elapsed_ns;
	table_stats.thread_ids.insert(std::this_thread::get_id());
#else
	(void)table_path;
	(void)bytes;
	(void)elapsed_ns;
#endif
}

// Store a query-global profiling metric for later emission.
void QueryProfiler::SetQueryGlobalMetric(MetricsType metric, Value value) {
	if (!IsEnabled()) {
		return;
	}
	lock_guard<std::mutex> guard(lock);
	query_metrics.query_global_info.metrics[metric] = std::move(value);
}

// Tracks an in-flight DDSPosix::pread call so we can compute max concurrency.
void QueryProfiler::BeginDDSPosixPread() {
#if defined(DUCKDB_DDS_PREAD_METRICS_ENABLED) && (DUCKDB_DDS_PREAD_METRICS_ENABLED)
	if (!IsEnabled()) {
		return;
	}
	const auto current = query_metrics.dds_pread_in_flight.fetch_add(1) + 1;
	auto max_seen = query_metrics.dds_pread_max_in_flight.load();
	while (current > max_seen && !query_metrics.dds_pread_max_in_flight.compare_exchange_weak(max_seen, current)) {
	}
#endif
}

// Marks the end of an in-flight DDSPosix::pread call for concurrency tracking.
void QueryProfiler::EndDDSPosixPread() {
#if defined(DUCKDB_DDS_PREAD_METRICS_ENABLED) && (DUCKDB_DDS_PREAD_METRICS_ENABLED)
	if (!IsEnabled()) {
		return;
	}
	query_metrics.dds_pread_in_flight.fetch_sub(1);
#endif
}

void QueryProfiler::PushTableFilterExpressionScope() {
	table_filter_expression_scope_depth++;
}

void QueryProfiler::PopTableFilterExpressionScope() {
	if (table_filter_expression_scope_depth > 0) {
		table_filter_expression_scope_depth--;
	}
}

bool QueryProfiler::InTableFilterExpressionScope() {
	return table_filter_expression_scope_depth > 0;
}

void QueryProfiler::PushTableScanStringPredicateIOScope(bool constant_comparison, bool like_operator) {
	if (constant_comparison) {
		table_scan_string_constant_comparison_io_scope_depth++;
	}
	if (like_operator) {
		table_scan_string_like_operator_io_scope_depth++;
	}
}

void QueryProfiler::PopTableScanStringPredicateIOScope(bool constant_comparison, bool like_operator) {
	if (constant_comparison && table_scan_string_constant_comparison_io_scope_depth > 0) {
		table_scan_string_constant_comparison_io_scope_depth--;
	}
	if (like_operator && table_scan_string_like_operator_io_scope_depth > 0) {
		table_scan_string_like_operator_io_scope_depth--;
	}
}

bool QueryProfiler::InTableScanStringConstantComparisonIOScope() {
	return table_scan_string_constant_comparison_io_scope_depth > 0;
}

bool QueryProfiler::InTableScanStringLikeOperatorIOScope() {
	return table_scan_string_like_operator_io_scope_depth > 0;
}

string QueryProfiler::ToString(ExplainFormat explain_format) const {
	return ToString(GetPrintFormat(explain_format));
}

string QueryProfiler::ToString(ProfilerPrintFormat format) const {
	if (!IsEnabled()) {
		return RenderDisabledMessage(format);
	}
	switch (format) {
	case ProfilerPrintFormat::QUERY_TREE:
	case ProfilerPrintFormat::QUERY_TREE_OPTIMIZER:
		return QueryTreeToString();
	case ProfilerPrintFormat::JSON:
		return ToJSON();
	case ProfilerPrintFormat::NO_OUTPUT:
		return "";
	case ProfilerPrintFormat::HTML:
	case ProfilerPrintFormat::GRAPHVIZ: {
		lock_guard<std::mutex> guard(lock);
		// checking the tree to ensure the query is really empty
		// the query string is empty when a logical plan is deserialized
		if (query_metrics.query.empty() && !root) {
			return "";
		}
		auto renderer = TreeRenderer::CreateRenderer(GetExplainFormat(format));
		duckdb::stringstream str;
		auto &info = root->GetProfilingInfo();
		if (info.Enabled(info.expanded_settings, MetricsType::OPERATOR_TIMING)) {
			info.metrics[MetricsType::OPERATOR_TIMING] = query_metrics.latency.Elapsed();
		}
		renderer->Render(*root, str);
		return str.str();
	}
	default:
		throw InternalException("Unknown ProfilerPrintFormat \"%s\"", EnumUtil::ToString(format));
	}
}

void QueryProfiler::StartPhase(MetricsType phase_metric) {
	lock_guard<std::mutex> guard(lock);
	if (!IsEnabled() || !running) {
		return;
	}

	// start a new phase
	phase_stack.push_back(phase_metric);
	// restart the timer
	phase_profiler.Start();
}

void QueryProfiler::EndPhase() {
	lock_guard<std::mutex> guard(lock);
	if (!IsEnabled() || !running) {
		return;
	}
	D_ASSERT(!phase_stack.empty());

	// end the timer
	phase_profiler.End();
	// add the timing to all currently active phases
	for (auto &phase : phase_stack) {
		phase_timings[phase] += phase_profiler.Elapsed();
	}
	// now remove the last added phase
	phase_stack.pop_back();

	if (!phase_stack.empty()) {
		phase_profiler.Start();
	}
}

OperatorProfiler::OperatorProfiler(ClientContext &context) : context(context) {
	enabled = QueryProfiler::Get(context).IsEnabled();
	auto &context_metrics = ClientConfig::GetConfig(context).profiler_settings;

	// Expand.
	for (const auto metric : context_metrics) {
		settings.insert(metric);
		ProfilingInfo::Expand(settings, metric);
	}

	// Reduce.
	auto root_metrics = ProfilingInfo::DefaultRootSettings();
	for (const auto metric : root_metrics) {
		settings.erase(metric);
	}
}

void OperatorProfiler::StartOperator(optional_ptr<const PhysicalOperator> phys_op) {
	if (!enabled) {
		return;
	}
	if (active_operator) {
		throw InternalException("OperatorProfiler: Attempting to call StartOperator while another operator is active");
	}
	active_operator = phys_op;

	if (!settings.empty()) {
		if (ProfilingInfo::Enabled(settings, MetricsType::EXTRA_INFO)) {
			if (!OperatorInfoIsInitialized(*active_operator)) {
				// first time calling into this operator - fetch the info
				auto &info = GetOperatorInfo(*active_operator);
				auto params = active_operator->ParamsToString();
				info.extra_info = params;
			}
		}

		// Start the timing of the current operator.
		if (ProfilingInfo::Enabled(settings, MetricsType::OPERATOR_TIMING)) {
			op.Start();
		}
	}
}

void OperatorProfiler::EndOperator(optional_ptr<DataChunk> chunk) {
	if (!enabled) {
		return;
	}
	if (!active_operator) {
		throw InternalException("OperatorProfiler: Attempting to call EndOperator while no operator is active");
	}

	if (!settings.empty()) {
		auto &info = GetOperatorInfo(*active_operator);
		if (ProfilingInfo::Enabled(settings, MetricsType::OPERATOR_TIMING)) {
			op.End();
			info.AddTime(op.Elapsed());
		}
		if (ProfilingInfo::Enabled(settings, MetricsType::OPERATOR_CARDINALITY) && chunk) {
			info.AddReturnedElements(chunk->size());
		}
		if (ProfilingInfo::Enabled(settings, MetricsType::RESULT_SET_SIZE) && chunk) {
			auto result_set_size = chunk->GetAllocationSize();
			info.AddResultSetSize(result_set_size);
		}
		if (ProfilingInfo::Enabled(settings, MetricsType::SYSTEM_PEAK_BUFFER_MEMORY)) {
			auto used_memory = BufferManager::GetBufferManager(context).GetBufferPool().GetUsedMemory(false);
			info.UpdateSystemPeakBufferManagerMemory(used_memory);
		}
		if (ProfilingInfo::Enabled(settings, MetricsType::SYSTEM_PEAK_TEMP_DIR_SIZE)) {
			auto used_swap = BufferManager::GetBufferManager(context).GetUsedSwap();
			info.UpdateSystemPeakTempDirectorySize(used_swap);
		}
	}
	active_operator = nullptr;
}

void OperatorProfiler::FinishSource(GlobalSourceState &gstate, LocalSourceState &lstate) {
	if (!enabled) {
		return;
	}
	if (!active_operator) {
		throw InternalException("OperatorProfiler: Attempting to call FinishSource while no operator is active");
	}
	if (!settings.empty()) {
		if (ProfilingInfo::Enabled(settings, MetricsType::EXTRA_INFO)) {
			// we're emitting extra info - get the extra source info
			auto &info = GetOperatorInfo(*active_operator);
			auto extra_info = active_operator->ExtraSourceParams(gstate, lstate);
			for (auto &new_info : extra_info) {
				auto entry = info.extra_info.find(new_info.first);
				if (entry != info.extra_info.end()) {
					// entry exists - override
					entry->second = std::move(new_info.second);
				} else {
					// entry does not exist yet - insert
					info.extra_info.insert(std::move(new_info));
				}
			}
		}
	}
}

bool OperatorProfiler::OperatorInfoIsInitialized(const PhysicalOperator &phys_op) {
	auto entry = operator_infos.find(phys_op);
	return entry != operator_infos.end();
}

OperatorInformation &OperatorProfiler::GetOperatorInfo(const PhysicalOperator &phys_op) {
	auto entry = operator_infos.find(phys_op);
	if (entry != operator_infos.end()) {
		return entry->second;
	}

	// Add a new entry.
	operator_infos[phys_op] = OperatorInformation();
	return operator_infos[phys_op];
}

void OperatorProfiler::Flush(const PhysicalOperator &phys_op) {
	auto entry = operator_infos.find(phys_op);
	if (entry == operator_infos.end()) {
		return;
	}

	auto &info = operator_infos.find(phys_op)->second;
	info.name = phys_op.GetName();
}

void QueryProfiler::Flush(OperatorProfiler &profiler) {
	lock_guard<std::mutex> guard(lock);
	if (!IsEnabled() || !running) {
		return;
	}
	for (auto &node : profiler.operator_infos) {
		auto &op = node.first.get();
		auto entry = tree_map.find(op);
		D_ASSERT(entry != tree_map.end());

		auto &tree_node = entry->second.get();
		auto &info = tree_node.GetProfilingInfo();

		if (ProfilingInfo::Enabled(profiler.settings, MetricsType::OPERATOR_TIMING)) {
			info.MetricSum<double>(MetricsType::OPERATOR_TIMING, node.second.time);
		}
		if (ProfilingInfo::Enabled(profiler.settings, MetricsType::OPERATOR_CARDINALITY)) {
			info.MetricSum<idx_t>(MetricsType::OPERATOR_CARDINALITY, node.second.elements_returned);
		}
		if (ProfilingInfo::Enabled(profiler.settings, MetricsType::OPERATOR_ROWS_SCANNED)) {
			if (op.type == PhysicalOperatorType::TABLE_SCAN) {
				auto &scan_op = op.Cast<PhysicalTableScan>();
				auto &bind_data = scan_op.bind_data;

				if (bind_data && scan_op.function.cardinality) {
					auto cardinality = scan_op.function.cardinality(context, &(*bind_data));
					if (cardinality && cardinality->has_estimated_cardinality) {
						info.MetricSum<idx_t>(MetricsType::OPERATOR_ROWS_SCANNED, cardinality->estimated_cardinality);
					}
				}
			}
		}
		if (ProfilingInfo::Enabled(profiler.settings, MetricsType::RESULT_SET_SIZE)) {
			info.MetricSum<idx_t>(MetricsType::RESULT_SET_SIZE, node.second.result_set_size);
		}
		if (ProfilingInfo::Enabled(profiler.settings, MetricsType::EXTRA_INFO)) {
			info.extra_info = node.second.extra_info;
		}
		if (ProfilingInfo::Enabled(profiler.settings, MetricsType::SYSTEM_PEAK_BUFFER_MEMORY)) {
			query_metrics.query_global_info.MetricMax(MetricsType::SYSTEM_PEAK_BUFFER_MEMORY,
			                                          node.second.system_peak_buffer_manager_memory);
		}
		if (ProfilingInfo::Enabled(profiler.settings, MetricsType::SYSTEM_PEAK_TEMP_DIR_SIZE)) {
			query_metrics.query_global_info.MetricMax(MetricsType::SYSTEM_PEAK_TEMP_DIR_SIZE,
			                                          node.second.system_peak_temp_directory_size);
		}
	}
	profiler.operator_infos.clear();
}

void QueryProfiler::SetInfo(const double &blocked_thread_time) {
	lock_guard<std::mutex> guard(lock);
	if (!IsEnabled() || !running) {
		return;
	}

	auto &info = root->GetProfilingInfo();
	if (info.Enabled(info.expanded_settings, MetricsType::BLOCKED_THREAD_TIME)) {
		query_metrics.query_global_info.metrics[MetricsType::BLOCKED_THREAD_TIME] = blocked_thread_time;
	}
}

string QueryProfiler::DrawPadded(const string &str, idx_t width) {
	if (str.size() > width) {
		return str.substr(0, width);
	} else {
		width -= str.size();
		auto half_spaces = width / 2;
		auto extra_left_space = NumericCast<idx_t>(width % 2 != 0 ? 1 : 0);
		return string(half_spaces + extra_left_space, ' ') + str + string(half_spaces, ' ');
	}
}

static string RenderTitleCase(string str) {
	str = StringUtil::Lower(str);
	str[0] = NumericCast<char>(toupper(str[0]));
	for (idx_t i = 0; i < str.size(); i++) {
		if (str[i] == '_') {
			str[i] = ' ';
			if (i + 1 < str.size()) {
				str[i + 1] = NumericCast<char>(toupper(str[i + 1]));
			}
		}
	}
	return str;
}

static string RenderTiming(double timing) {
	string timing_s;
	if (timing >= 1) {
		timing_s = StringUtil::Format("%.2f", timing);
	} else if (timing >= 0.1) {
		timing_s = StringUtil::Format("%.3f", timing);
	} else {
		timing_s = StringUtil::Format("%.4f", timing);
	}
	return timing_s + "s";
}

string QueryProfiler::QueryTreeToString() const {
	duckdb::stringstream str;
	QueryTreeToStream(str);
	return str.str();
}

void RenderPhaseTimings(std::ostream &ss, const pair<string, double> &head, map<string, double> &timings, idx_t width) {
	ss << "┌────────────────────────────────────────────────┐\n";
	ss << "│" + QueryProfiler::DrawPadded(RenderTitleCase(head.first) + ": " + RenderTiming(head.second), width - 2) +
	          "│\n";
	ss << "│┌──────────────────────────────────────────────┐│\n";

	for (const auto &entry : timings) {
		ss << "││" +
		          QueryProfiler::DrawPadded(RenderTitleCase(entry.first) + ": " + RenderTiming(entry.second),
		                                    width - 4) +
		          "││\n";
	}
	ss << "│└──────────────────────────────────────────────┘│\n";
	ss << "└────────────────────────────────────────────────┘\n";
}

void PrintPhaseTimingsToStream(std::ostream &ss, const ProfilingInfo &info, idx_t width) {
	map<string, double> optimizer_timings;
	map<string, double> planner_timings;
	map<string, double> physical_planner_timings;

	pair<string, double> optimizer_head;
	pair<string, double> planner_head;
	pair<string, double> physical_planner_head;

	for (const auto &entry : info.metrics) {
		if (MetricsUtils::IsOptimizerMetric(entry.first)) {
			optimizer_timings[EnumUtil::ToString(entry.first).substr(10)] = entry.second.GetValue<double>();
		} else if (MetricsUtils::IsPhaseTimingMetric(entry.first)) {
			switch (entry.first) {
			case MetricsType::CUMULATIVE_OPTIMIZER_TIMING:
				continue;
			case MetricsType::ALL_OPTIMIZERS:
				optimizer_head = {"Optimizer", entry.second.GetValue<double>()};
				break;
			case MetricsType::PHYSICAL_PLANNER:
				physical_planner_head = {"Physical Planner", entry.second.GetValue<double>()};
				break;
			case MetricsType::PLANNER:
				planner_head = {"Planner", entry.second.GetValue<double>()};
				break;
			default:
				break;
			}

			auto metric = EnumUtil::ToString(entry.first);
			if (StringUtil::StartsWith(metric, "PHYSICAL_PLANNER") && entry.first != MetricsType::PHYSICAL_PLANNER) {
				physical_planner_timings[metric.substr(17)] = entry.second.GetValue<double>();
			} else if (StringUtil::StartsWith(metric, "PLANNER") && entry.first != MetricsType::PLANNER) {
				planner_timings[metric.substr(8)] = entry.second.GetValue<double>();
			}
		}
	}

	RenderPhaseTimings(ss, optimizer_head, optimizer_timings, width);
	RenderPhaseTimings(ss, physical_planner_head, physical_planner_timings, width);
	RenderPhaseTimings(ss, planner_head, planner_timings, width);
}

void QueryProfiler::QueryTreeToStream(std::ostream &ss) const {
	lock_guard<std::mutex> guard(lock);
	ss << "┌─────────────────────────────────────┐\n";
	ss << "│┌───────────────────────────────────┐│\n";
	ss << "││    Query Profiling Information    ││\n";
	ss << "│└───────────────────────────────────┘│\n";
	ss << "└─────────────────────────────────────┘\n";
	ss << StringUtil::Replace(query_metrics.query, "\n", " ") + "\n";

	// checking the tree to ensure the query is really empty
	// the query string is empty when a logical plan is deserialized
	if (query_metrics.query.empty() && !root) {
		return;
	}

	for (auto &state : context.registered_state->States()) {
		state->WriteProfilingInformation(ss);
	}

	constexpr idx_t TOTAL_BOX_WIDTH = 50;
	ss << "┌────────────────────────────────────────────────┐\n";
	ss << "│┌──────────────────────────────────────────────┐│\n";
	string total_time = "Total Time: " + RenderTiming(query_metrics.latency.Elapsed());
	ss << "││" + DrawPadded(total_time, TOTAL_BOX_WIDTH - 4) + "││\n";
	ss << "│└──────────────────────────────────────────────┘│\n";
	ss << "└────────────────────────────────────────────────┘\n";
	// render the main operator tree
	if (root) {
		// print phase timings
		if (PrintOptimizerOutput()) {
			PrintPhaseTimingsToStream(ss, root->GetProfilingInfo(), TOTAL_BOX_WIDTH);
		}
		Render(*root, ss);
	}
}

InsertionOrderPreservingMap<string> QueryProfiler::JSONSanitize(const InsertionOrderPreservingMap<string> &input) {
	InsertionOrderPreservingMap<string> result;
	for (auto &it : input) {
		auto key = it.first;
		if (StringUtil::StartsWith(key, "__")) {
			key = StringUtil::Replace(key, "__", "");
			key = StringUtil::Replace(key, "_", " ");
			key = StringUtil::Title(key);
		}
		result[key] = it.second;
	}
	return result;
}

string QueryProfiler::JSONSanitize(const std::string &text) {
	string result;
	result.reserve(text.size());
	for (char i : text) {
		switch (i) {
		case '\b':
			result += "\\b";
			break;
		case '\f':
			result += "\\f";
			break;
		case '\n':
			result += "\\n";
			break;
		case '\r':
			result += "\\r";
			break;
		case '\t':
			result += "\\t";
			break;
		case '"':
			result += "\\\"";
			break;
		case '\\':
			result += "\\\\";
			break;
		default:
			result += i;
			break;
		}
	}
	return result;
}

static yyjson_mut_val *ToJSONRecursive(yyjson_mut_doc *doc, ProfilingNode &node) {
	auto result_obj = yyjson_mut_obj(doc);
	auto &profiling_info = node.GetProfilingInfo();
	profiling_info.extra_info = QueryProfiler::JSONSanitize(profiling_info.extra_info);
	profiling_info.WriteMetricsToJSON(doc, result_obj);

	auto children_list = yyjson_mut_arr(doc);
	for (idx_t i = 0; i < node.GetChildCount(); i++) {
		auto child = ToJSONRecursive(doc, *node.GetChild(i));
		yyjson_mut_arr_add_val(children_list, child);
	}
	yyjson_mut_obj_add_val(doc, result_obj, "children", children_list);
	return result_obj;
}

static string StringifyAndFree(yyjson_mut_doc *doc, yyjson_mut_val *object) {
	auto data = yyjson_mut_val_write_opts(object, YYJSON_WRITE_ALLOW_INF_AND_NAN | YYJSON_WRITE_PRETTY, nullptr,
	                                      nullptr, nullptr);
	if (!data) {
		yyjson_mut_doc_free(doc);
		throw InternalException("The plan could not be rendered as JSON, yyjson failed");
	}
	auto result = string(data);
	free(data);
	yyjson_mut_doc_free(doc);
	return result;
}

string QueryProfiler::ToJSON() const {
	lock_guard<std::mutex> guard(lock);
	auto doc = yyjson_mut_doc_new(nullptr);
	auto result_obj = yyjson_mut_obj(doc);
	yyjson_mut_doc_set_root(doc, result_obj);

	if (query_metrics.query.empty() && !root) {
		yyjson_mut_obj_add_str(doc, result_obj, "result", "empty");
		return StringifyAndFree(doc, result_obj);
	}
	if (!root) {
		yyjson_mut_obj_add_str(doc, result_obj, "result", "error");
		return StringifyAndFree(doc, result_obj);
	}

	auto &settings = root->GetProfilingInfo();

	settings.WriteMetricsToJSON(doc, result_obj);

	// recursively print the physical operator tree
	auto children_list = yyjson_mut_arr(doc);
	yyjson_mut_obj_add_val(doc, result_obj, "children", children_list);
	auto child = ToJSONRecursive(doc, *root->GetChild(0));
	yyjson_mut_arr_add_val(children_list, child);
	return StringifyAndFree(doc, result_obj);
}

void QueryProfiler::WriteToFile(const char *path, string &info) const {
	ofstream out(path);
	out << info;
	out.close();
	// throw an IO exception if it fails to write the file
	if (out.fail()) {
		throw IOException(strerror(errno));
	}
}

profiler_settings_t EraseQueryRootSettings(profiler_settings_t settings) {
	profiler_settings_t phase_timing_settings_to_erase;

	for (auto &setting : settings) {
		if (MetricsUtils::IsOptimizerMetric(setting) || MetricsUtils::IsPhaseTimingMetric(setting) ||
		    MetricsUtils::IsQueryGlobalMetric(setting)) {
			phase_timing_settings_to_erase.insert(setting);
		}
	}

	for (auto &setting : phase_timing_settings_to_erase) {
		settings.erase(setting);
	}

	return settings;
}

unique_ptr<ProfilingNode> QueryProfiler::CreateTree(const PhysicalOperator &root_p, const profiler_settings_t &settings,
                                                    const idx_t depth) {
	if (OperatorRequiresProfiling(root_p.type)) {
		query_requires_profiling = true;
	}

	unique_ptr<ProfilingNode> node = make_uniq<ProfilingNode>();
	auto &info = node->GetProfilingInfo();
	info = ProfilingInfo(settings, depth);
	auto child_settings = settings;
	if (depth == 0) {
		child_settings = EraseQueryRootSettings(child_settings);
	}
	node->depth = depth;

	if (depth != 0) {
		info.metrics[MetricsType::OPERATOR_NAME] = root_p.GetName();
		info.MetricSum<uint8_t>(MetricsType::OPERATOR_TYPE, static_cast<uint8_t>(root_p.type));
	}
	if (info.Enabled(info.settings, MetricsType::EXTRA_INFO)) {
		info.extra_info = root_p.ParamsToString();
	}

	tree_map.insert(make_pair(reference<const PhysicalOperator>(root_p), reference<ProfilingNode>(*node)));
	auto children = root_p.GetChildren();
	for (auto &child : children) {
		auto child_node = CreateTree(child.get(), child_settings, depth + 1);
		node->AddChild(std::move(child_node));
	}
	return node;
}

string QueryProfiler::RenderDisabledMessage(ProfilerPrintFormat format) const {
	switch (format) {
	case ProfilerPrintFormat::NO_OUTPUT:
		return "";
	case ProfilerPrintFormat::QUERY_TREE:
	case ProfilerPrintFormat::QUERY_TREE_OPTIMIZER:
		return "Query profiling is disabled. Use 'PRAGMA enable_profiling;' to enable profiling!";
	case ProfilerPrintFormat::HTML:
		return R"(
				<!DOCTYPE html>
                <html lang="en"><head/><body>
                  Query profiling is disabled. Use 'PRAGMA enable_profiling;' to enable profiling!
                </body></html>
			)";
	case ProfilerPrintFormat::GRAPHVIZ:
		return R"(
				digraph G {
				    node [shape=box, style=rounded, fontname="Courier New", fontsize=10];
				    node_0_0 [label="Query profiling is disabled. Use 'PRAGMA enable_profiling;' to enable profiling!"];
				}
			)";
	case ProfilerPrintFormat::JSON: {
		auto doc = yyjson_mut_doc_new(nullptr);
		auto result_obj = yyjson_mut_obj(doc);
		yyjson_mut_doc_set_root(doc, result_obj);

		yyjson_mut_obj_add_str(doc, result_obj, "result", "disabled");
		return StringifyAndFree(doc, result_obj);
	}
	default:
		throw InternalException("Unknown ProfilerPrintFormat \"%s\"", EnumUtil::ToString(format));
	}
}

void QueryProfiler::Initialize(const PhysicalOperator &root_op) {
	lock_guard<std::mutex> guard(lock);
	if (!IsEnabled() || !running) {
		return;
	}
	query_requires_profiling = false;
	ClientConfig &config = ClientConfig::GetConfig(context);
	root = CreateTree(root_op, config.profiler_settings, 0);
	if (!query_requires_profiling) {
		// query does not require profiling: disable profiling for this query
		running = false;
		tree_map.clear();
		root = nullptr;
		phase_timings.clear();
		phase_stack.clear();
	}
}

void QueryProfiler::Render(const ProfilingNode &node, std::ostream &ss) const {
	TextTreeRenderer renderer;
	if (IsDetailedEnabled()) {
		renderer.EnableDetailed();
	} else {
		renderer.EnableStandard();
	}
	renderer.Render(node, ss);
}

void QueryProfiler::Print() {
	Printer::Print(QueryTreeToString());
}

void QueryProfiler::MoveOptimizerPhasesToRoot() {
	auto &root_info = root->GetProfilingInfo();
	auto &root_metrics = root_info.metrics;

	for (auto &entry : phase_timings) {
		auto &phase = entry.first;
		auto &timing = entry.second;
		if (root_info.Enabled(root_info.expanded_settings, phase)) {
			root_metrics[phase] = Value::CreateValue(timing);
		}
	}
}

void QueryProfiler::Propagate(QueryProfiler &) {
}

} // namespace duckdb
