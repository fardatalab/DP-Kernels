//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/query_profiler.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/deque.hpp"
#include "duckdb/common/enums/profiler_format.hpp"
#include "duckdb/common/enums/explain_format.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/common/profiler.hpp"
#include "duckdb/common/reference_map.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/winapi.hpp"
#include "duckdb/execution/expression_executor_state.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/main/profiling_info.hpp"
#include "duckdb/main/profiling_node.hpp"

#include <stack>
#include <functional>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <unordered_set>

namespace duckdb {

class ClientContext;
class ExpressionExecutor;
class ProfilingNode;
class PhysicalOperator;
class SQLStatement;

enum class ProfilingCoverage : uint8_t { SELECT = 0, ALL = 1 };

struct OperatorInformation {
	explicit OperatorInformation() {
	}

	string name;

	double time = 0;
	idx_t elements_returned = 0;
	idx_t result_set_size = 0;
	idx_t system_peak_buffer_manager_memory = 0;
	idx_t system_peak_temp_directory_size = 0;

	InsertionOrderPreservingMap<string> extra_info;

	void AddTime(double n_time) {
		time += n_time;
	}

	void AddReturnedElements(idx_t n_elements) {
		elements_returned += n_elements;
	}

	void AddResultSetSize(idx_t n_result_set_size) {
		result_set_size += n_result_set_size;
	}

	void UpdateSystemPeakBufferManagerMemory(idx_t used_memory) {
		if (used_memory > system_peak_buffer_manager_memory) {
			system_peak_buffer_manager_memory = used_memory;
		}
	}

	void UpdateSystemPeakTempDirectorySize(idx_t used_swap) {
		if (used_swap > system_peak_temp_directory_size) {
			system_peak_temp_directory_size = used_swap;
		}
	}
};

//! The OperatorProfiler measures timings of individual operators
//! This class exists once for all operators and collects `OperatorInfo` for each operator
class OperatorProfiler {
	friend class QueryProfiler;

public:
	DUCKDB_API explicit OperatorProfiler(ClientContext &context);
	~OperatorProfiler() {
	}

public:
	DUCKDB_API void StartOperator(optional_ptr<const PhysicalOperator> phys_op);
	DUCKDB_API void EndOperator(optional_ptr<DataChunk> chunk);
	DUCKDB_API void FinishSource(GlobalSourceState &gstate, LocalSourceState &lstate);

	//! Adds the timings in the OperatorProfiler (tree) to the QueryProfiler (tree).
	DUCKDB_API void Flush(const PhysicalOperator &phys_op);
	DUCKDB_API OperatorInformation &GetOperatorInfo(const PhysicalOperator &phys_op);
	DUCKDB_API bool OperatorInfoIsInitialized(const PhysicalOperator &phys_op);
	DUCKDB_API void AddExtraInfo(InsertionOrderPreservingMap<string> extra_info);

public:
	ClientContext &context;

private:
	//! Whether or not the profiler is enabled
	bool enabled;
	//! Sub-settings for the operator profiler
	profiler_settings_t settings;

	//! The timer used to time the execution time of the individual Physical Operators
	Profiler op;
	//! The stack of Physical Operators that are currently active
	optional_ptr<const PhysicalOperator> active_operator;
	//! A mapping of physical operators to profiled operator information.
	reference_map_t<const PhysicalOperator, OperatorInformation> operator_infos;
};

//! Top level query metrics.
struct QueryMetrics {
	//! Per-table aggregate metrics for DDSPosix::pread2 calls.
	struct DDSPosixPread2PerTableStats {
		DDSPosixPread2PerTableStats() : bytes(0), time_ns(0) {
		}
		//! Total bytes processed by DDSPosix::pread2 for this table.
		uint64_t bytes;
		//! Total elapsed host-thread nanoseconds spent in DDSPosix::pread2 for this table.
		uint64_t time_ns;
		//! Set of unique execution threads that issued DDSPosix::pread2 for this table.
		std::unordered_set<std::thread::id> thread_ids;
	};

	//! Initialize query-level counters to zero.
	QueryMetrics()
	    : total_bytes_read(0), total_bytes_written(0), parquet_decrypt_time_ns(0), parquet_decrypt_call_count(0),
	      parquet_decompress_time_ns(0), parquet_decompress_call_count(0), dds_pread_time_ns(0),
	      dds_pread2_time_ns(0), dds_pread_bytes(0),
	      dds_pread_call_count(0), dds_pread_in_flight(0), dds_pread_max_in_flight(0),
	      dds_pread_wall_start_ns(0), dds_pread_wall_end_ns(0), offload_parquet_read_time_ns(0),
	      offload_parquet_read_call_count(0), offload_parquet_decrypt_time_ns(0),
	      offload_parquet_decrypt_call_count(0), offload_parquet_decompress_time_ns(0),
	      offload_parquet_decompress_call_count(0),
	      table_scan_string_constant_comparison_time_ns(0), table_scan_string_constant_comparison_count(0),
	      table_scan_string_like_operator_time_ns(0), table_scan_string_like_operator_count(0),
	      table_scan_string_constant_comparison_read_io_time_ns(0), table_scan_string_like_operator_read_io_time_ns(0) {};

	ProfilingInfo query_global_info;

	//! The SQL string of the query
	string query;
	//! The timer used to time the excution time of the entire query
	Profiler latency;
	//! The total bytes read by the file system
	atomic<idx_t> total_bytes_read;
	//! The total bytes written by the file system
	atomic<idx_t> total_bytes_written;
	//! Total nanoseconds spent decrypting parquet data in this query
	atomic<uint64_t> parquet_decrypt_time_ns;
	//! Number of parquet decryption operations in this query
	atomic<uint64_t> parquet_decrypt_call_count;
	//! Total nanoseconds spent decompressing parquet data in this query
	atomic<uint64_t> parquet_decompress_time_ns;
	//! Number of parquet decompression operations in this query
	atomic<uint64_t> parquet_decompress_call_count;
	//! Total nanoseconds spent in DDSPosix::pread reads for this query
	atomic<uint64_t> dds_pread_time_ns;
	//! Total nanoseconds spent in DDSPosix::pread2 reads for this query
	atomic<uint64_t> dds_pread2_time_ns;
	//! Total bytes read via DDSPosix::pread for this query
	atomic<uint64_t> dds_pread_bytes;
	//! Number of DDSPosix::pread calls in this query
	atomic<uint64_t> dds_pread_call_count;
	//! Number of DDSPosix::pread calls currently in flight
	atomic<uint64_t> dds_pread_in_flight;
	//! Maximum number of concurrent DDSPosix::pread calls observed
	atomic<uint64_t> dds_pread_max_in_flight;
	//! Wall clock start (steady clock, ns since epoch) for the first DDSPosix::pread in the query
	atomic<uint64_t> dds_pread_wall_start_ns;
	//! Wall clock end (steady clock, ns since epoch) for the last DDSPosix::pread in the query
	atomic<uint64_t> dds_pread_wall_end_ns;
	//! Total nanoseconds spent in offloaded parquet stage0 (read) operations.
	atomic<uint64_t> offload_parquet_read_time_ns;
	//! Number of offloaded parquet stage0 (read) operations.
	atomic<uint64_t> offload_parquet_read_call_count;
	//! Total nanoseconds spent in offloaded parquet stage1 (decrypt) operations.
	atomic<uint64_t> offload_parquet_decrypt_time_ns;
	//! Number of offloaded parquet stage1 (decrypt) operations.
	atomic<uint64_t> offload_parquet_decrypt_call_count;
	//! Total nanoseconds spent in offloaded parquet stage2 (decompress) operations.
	atomic<uint64_t> offload_parquet_decompress_time_ns;
	//! Number of offloaded parquet stage2 (decompress) operations.
	atomic<uint64_t> offload_parquet_decompress_call_count;
	//! Total nanoseconds spent evaluating table-scan string constant comparisons.
	atomic<uint64_t> table_scan_string_constant_comparison_time_ns;
	//! Number of table-scan string constant-comparison predicate evaluation calls.
	atomic<uint64_t> table_scan_string_constant_comparison_count;
	//! Total nanoseconds spent evaluating table-scan LIKE-related predicates.
	atomic<uint64_t> table_scan_string_like_operator_time_ns;
	//! Number of table-scan LIKE-related predicate evaluation calls.
	atomic<uint64_t> table_scan_string_like_operator_count;
	//! Total nanoseconds spent in read I/O while processing table-scan string constant-comparison predicates.
	atomic<uint64_t> table_scan_string_constant_comparison_read_io_time_ns;
	//! Total nanoseconds spent in read I/O while processing table-scan LIKE-related predicates.
	atomic<uint64_t> table_scan_string_like_operator_read_io_time_ns;
	//! Mutex protecting `dds_pread2_per_table_stats`.
	std::mutex dds_pread2_per_table_stats_lock;
	//! Per-table aggregate bytes/time/thread-id statistics for DDSPosix::pread2 calls.
	unordered_map<string, DDSPosixPread2PerTableStats> dds_pread2_per_table_stats;
};

//! QueryProfiler collects the profiling metrics of a query.
class QueryProfiler {
public:
	using TreeMap = reference_map_t<const PhysicalOperator, reference<ProfilingNode>>;

public:
	DUCKDB_API explicit QueryProfiler(ClientContext &context);

public:
	//! Propagate save_location, enabled, detailed_enabled and automatic_print_format.
	void Propagate(QueryProfiler &qp);

	DUCKDB_API bool IsEnabled() const;
	DUCKDB_API bool IsDetailedEnabled() const;
	DUCKDB_API ProfilerPrintFormat GetPrintFormat(ExplainFormat format = ExplainFormat::DEFAULT) const;
	DUCKDB_API bool PrintOptimizerOutput() const;
	DUCKDB_API string GetSaveLocation() const;

	DUCKDB_API static QueryProfiler &Get(ClientContext &context);

	DUCKDB_API void Start(const string &query);
	//! Reset per-query state and metric counters before a new query starts.
	//! DDS pread thread stats are reset only when DUCKDB_DDS_PREAD_METRICS_ENABLED is enabled.
	//! Modified: DDS pread lock-free per-thread latency buffers are reset on each query as well.
	DUCKDB_API void Reset();
	DUCKDB_API void StartQuery(const string &query, bool is_explain_analyze = false, bool start_at_optimizer = false);
	//! Finalize profiling metrics and emit output when profiling is enabled.
	//! DDS pread metrics (including call count and tail latencies) are emitted only when
	//! DUCKDB_DDS_PREAD_METRICS_ENABLED is enabled.
	DUCKDB_API void EndQuery();

	//! Adds nr_bytes bytes to the total bytes read.
	DUCKDB_API void AddBytesRead(const idx_t nr_bytes);
	//! Adds nr_bytes bytes to the total bytes written.
	DUCKDB_API void AddBytesWritten(const idx_t nr_bytes);
	//! Adds a parquet decryption timing in nanoseconds and increments the call counter.
	DUCKDB_API void AddParquetDecryptionMetrics(uint64_t elapsed_ns);
	//! Adds a parquet decompression timing in nanoseconds and increments the call counter.
	DUCKDB_API void AddParquetDecompressionMetrics(uint64_t elapsed_ns);
	//! Adds an offloaded parquet stage0 read timing in nanoseconds and increments the call counter.
	DUCKDB_API void AddOffloadParquetReadMetrics(uint64_t elapsed_ns);
	//! Adds an offloaded parquet stage1 decrypt timing in nanoseconds and increments the call counter.
	DUCKDB_API void AddOffloadParquetDecryptMetrics(uint64_t elapsed_ns);
	//! Adds an offloaded parquet stage2 decompress timing in nanoseconds and increments the call counter.
	DUCKDB_API void AddOffloadParquetDecompressMetrics(uint64_t elapsed_ns);
	//! Adds table-scan string constant-comparison predicate evaluation timing in nanoseconds.
	DUCKDB_API void AddTableScanStringConstantComparisonMetrics(uint64_t elapsed_ns);
	//! Adds table-scan LIKE-related predicate evaluation timing in nanoseconds.
	DUCKDB_API void AddTableScanStringLikeOperatorMetrics(uint64_t elapsed_ns);
	//! Adds read I/O timing in nanoseconds attributed to table-scan string constant-comparison predicates.
	DUCKDB_API void AddTableScanStringConstantComparisonReadIOMetrics(uint64_t elapsed_ns);
	//! Adds read I/O timing in nanoseconds attributed to table-scan LIKE-related predicates.
	DUCKDB_API void AddTableScanStringLikeOperatorReadIOMetrics(uint64_t elapsed_ns);
	//! Adds a DDSPosix::pread timing and byte count for query throughput metrics.
	//! Adds a DDSPosix::pread timing and byte count plus wall clock timing for query throughput metrics.
	//! Modified: records per-call latency samples in a lock-free, per-thread buffer for tail metrics (min/max/p99).
	//! No-op when DUCKDB_DDS_PREAD_METRICS_ENABLED is disabled.
	DUCKDB_API void AddDDSPosixPreadMetrics(uint64_t elapsed_ns, uint64_t bytes, uint64_t wall_start_ns,
	                                        uint64_t wall_end_ns);
	//! Adds DDSPosix::pread2 timing and per-table bytes/thread usage for query-level aggregate metrics.
	//! No-op when DUCKDB_DDS_PREAD_METRICS_ENABLED is disabled.
	DUCKDB_API void AddDDSPosixPread2Metrics(const string &table_path, uint64_t bytes, uint64_t elapsed_ns);
	//! Store a query-global profiling metric for later emission.
	DUCKDB_API void SetQueryGlobalMetric(MetricsType metric, Value value);
	//! Marks the start of a DDSPosix::pread call for concurrency tracking.
	//! No-op when DUCKDB_DDS_PREAD_METRICS_ENABLED is disabled.
	DUCKDB_API void BeginDDSPosixPread();
	//! Marks the end of a DDSPosix::pread call for concurrency tracking.
	//! No-op when DUCKDB_DDS_PREAD_METRICS_ENABLED is disabled.
	DUCKDB_API void EndDDSPosixPread();
	//! Enter/exit a thread-local scope indicating we are evaluating pushed-down
	//! table-scan expression filters. LIKE-related scalar functions use this to
	//! ensure only table-scan predicate work is counted.
	DUCKDB_API static void PushTableFilterExpressionScope();
	DUCKDB_API static void PopTableFilterExpressionScope();
	DUCKDB_API static bool InTableFilterExpressionScope();
	//! Enter/exit thread-local scopes used to attribute read I/O time to table-scan
	//! string predicate categories while filter columns are being read.
	DUCKDB_API static void PushTableScanStringPredicateIOScope(bool constant_comparison, bool like_operator);
	DUCKDB_API static void PopTableScanStringPredicateIOScope(bool constant_comparison, bool like_operator);
	DUCKDB_API static bool InTableScanStringConstantComparisonIOScope();
	DUCKDB_API static bool InTableScanStringLikeOperatorIOScope();

	DUCKDB_API void StartExplainAnalyze();

	//! Adds the timings gathered by an OperatorProfiler to this query profiler
	DUCKDB_API void Flush(OperatorProfiler &profiler);
	//! Adds the top level query information to the global profiler.
	DUCKDB_API void SetInfo(const double &blocked_thread_time);

	DUCKDB_API void StartPhase(MetricsType phase_metric);
	DUCKDB_API void EndPhase();

	DUCKDB_API void Initialize(const PhysicalOperator &root);

	DUCKDB_API string QueryTreeToString() const;
	DUCKDB_API void QueryTreeToStream(std::ostream &str) const;
	DUCKDB_API void Print();

	//! return the printed as a string. Unlike ToString, which is always formatted as a string,
	//! the return value is formatted based on the current print format (see GetPrintFormat()).
	DUCKDB_API string ToString(ExplainFormat format = ExplainFormat::DEFAULT) const;
	DUCKDB_API string ToString(ProfilerPrintFormat format) const;

	static InsertionOrderPreservingMap<string> JSONSanitize(const InsertionOrderPreservingMap<string> &input);
	static string JSONSanitize(const string &text);
	static string DrawPadded(const string &str, idx_t width);
	DUCKDB_API string ToJSON() const;
	DUCKDB_API void WriteToFile(const char *path, string &info) const;

	idx_t OperatorSize() {
		return tree_map.size();
	}

	void Finalize(ProfilingNode &node);

	//! Return the root of the query tree.
	optional_ptr<ProfilingNode> GetRoot() {
		return root.get();
	}

	//! Provides access to the root of the query tree, but ensures there are no concurrent modifications.
	//! This can be useful when implementing continuous profiling or making customizations.
	DUCKDB_API void GetRootUnderLock(const std::function<void(optional_ptr<ProfilingNode>)> &callback) {
		lock_guard<std::mutex> guard(lock);
		callback(GetRoot());
	}

private:
	unique_ptr<ProfilingNode> CreateTree(const PhysicalOperator &root, const profiler_settings_t &settings,
	                                     const idx_t depth = 0);
	void Render(const ProfilingNode &node, std::ostream &str) const;
	string RenderDisabledMessage(ProfilerPrintFormat format) const;

private:
	ClientContext &context;

	//! Whether or not the query profiler is running
	bool running;
	//! The lock used for accessing the global query profiler or flushing information to it from a thread
	mutable std::mutex lock;

	//! Whether or not the query requires profiling
	bool query_requires_profiling;

	//! The root of the query tree
	unique_ptr<ProfilingNode> root;

	//! Top level query information.
	QueryMetrics query_metrics;

	//! A map of a Physical Operator pointer to a tree node
	TreeMap tree_map;
	//! Whether or not we are running as part of a explain_analyze query
	bool is_explain_analyze;

	//! DDSPosix::pread-specific thread-level statistics (legacy; kept for reference).
	// struct DDSThreadStats {
	// 	uint64_t bytes = 0;
	// 	uint64_t time_ns = 0;
	// 	uint64_t wall_start_ns = 0;
	// 	uint64_t wall_end_ns = 0;
	// };
	// unordered_map<std::thread::id, DDSThreadStats, std::hash<std::thread::id>> dds_pread_thread_stats;
	// std::mutex dds_pread_thread_stats_mutex;

	//! DDSPosix::pread latency storage without per-call mutexes.
	//! Each thread writes to its own buffer and registers it once per query.
	struct DDSPosixPreadLatencyBuffer {
		explicit DDSPosixPreadLatencyBuffer(idx_t reserve_count) {
			// Reserve a large per-thread buffer up-front to avoid per-call reallocations.
			latencies_ns.reserve(reserve_count);
		}
		void Reset() {
			latencies_ns.clear();
			bytes = 0;
			time_ns = 0;
			wall_start_ns = 0;
			wall_end_ns = 0;
		}
		vector<uint64_t> latencies_ns;
		uint64_t bytes = 0;
		uint64_t time_ns = 0;
		uint64_t wall_start_ns = 0;
		uint64_t wall_end_ns = 0;
	};
	//! Global registry of per-thread latency buffers for tail metric aggregation.
	vector<DDSPosixPreadLatencyBuffer *> dds_pread_latency_buffers;
	//! Number of registered per-thread buffers for the current query.
	atomic<idx_t> dds_pread_latency_buffer_count {0};
	//! Generation counter used to reset thread-local buffers between queries.
	atomic<uint64_t> dds_pread_latency_generation {0};

public:
	const TreeMap &GetTreeMap() const {
		return tree_map;
	}

private:
	//! The timer used to time the individual phases of the planning process
	Profiler phase_profiler;
	//! A mapping of the phase names to the timings
	using PhaseTimingStorage = unordered_map<MetricsType, double, MetricsTypeHashFunction>;
	PhaseTimingStorage phase_timings;
	using PhaseTimingItem = PhaseTimingStorage::value_type;
	//! The stack of currently active phases
	vector<MetricsType> phase_stack;

private:
	void MoveOptimizerPhasesToRoot();

	//! Check whether or not an operator type requires query profiling. If none of the ops in a query require profiling
	//! no profiling information is output.
	bool OperatorRequiresProfiling(const PhysicalOperatorType op_type);
	ExplainFormat GetExplainFormat(ProfilerPrintFormat format) const;
};

} // namespace duckdb
