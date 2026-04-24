#include "duckdb/function/scalar/string_functions.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/profiler.hpp"
#include "duckdb/main/query_profiler.hpp"
#include <cstdio>

namespace duckdb {

namespace {

static uint64_t ElapsedNs(const Profiler &profiler) {
	return static_cast<uint64_t>(profiler.Elapsed() * 1000000000.0);
}

static bool SuffixFunction(const string_t &str, const string_t &suffix) {
	auto suffix_size = suffix.GetSize();
	auto str_size = str.GetSize();
	if (suffix_size > str_size) {
		return false;
	}

	auto suffix_data = suffix.GetData();
	auto str_data = str.GetData();
	auto suf_idx = UnsafeNumericCast<int32_t>(suffix_size) - 1;
	idx_t str_idx = str_size - 1;
	for (; suf_idx >= 0; --suf_idx, --str_idx) {
		if (suffix_data[suf_idx] != str_data[str_idx]) {
			return false;
		}
	}
	return true;
}

struct SuffixOperator {
	template <class TA, class TB, class TR>
	static inline TR Operation(TA left, TB right) {
		return SuffixFunction(left, right);
	}
};

void SuffixFunctionProfiled(DataChunk &args, ExpressionState &state, Vector &result) {
	const bool in_table_filter_scope = QueryProfiler::InTableFilterExpressionScope();
	const bool has_context = state.HasContext();
	if (in_table_filter_scope && !has_context) {
		printf("[WARN] Table-scan LIKE profiling skipped in SuffixFunctionProfiled: missing ExpressionState context\n");
	}
	const bool should_profile = in_table_filter_scope && has_context;
	Profiler profiler;
	if (should_profile) {
		profiler.Start();
	}
	BinaryExecutor::ExecuteStandard<string_t, string_t, bool, SuffixOperator>(args.data[0], args.data[1], result,
	                                                                           args.size());
	if (should_profile) {
		profiler.End();
		QueryProfiler::Get(state.GetContext()).AddTableScanStringLikeOperatorMetrics(ElapsedNs(profiler));
	}
}

} // namespace

ScalarFunction SuffixFun::GetFunction() {
	return ScalarFunction("suffix",                                     // name of the function
	                      {LogicalType::VARCHAR, LogicalType::VARCHAR}, // argument list
	                      LogicalType::BOOLEAN,                         // return type
	                      SuffixFunctionProfiled);
}

} // namespace duckdb
