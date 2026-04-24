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

bool PrefixFunction(const string_t &str, const string_t &pattern) {
	auto str_length = str.GetSize();
	auto patt_length = pattern.GetSize();
	if (patt_length > str_length) {
		return false;
	}
	if (patt_length <= string_t::PREFIX_LENGTH) {
		// short prefix
		if (patt_length == 0) {
			// length = 0, return true
			return true;
		}

		// prefix early out
		const char *str_pref = str.GetPrefix();
		const char *patt_pref = pattern.GetPrefix();
		for (idx_t i = 0; i < patt_length; ++i) {
			if (str_pref[i] != patt_pref[i]) {
				return false;
			}
		}
		return true;
	} else {
		// prefix early out
		const char *str_pref = str.GetPrefix();
		const char *patt_pref = pattern.GetPrefix();
		for (idx_t i = 0; i < string_t::PREFIX_LENGTH; ++i) {
			if (str_pref[i] != patt_pref[i]) {
				// early out
				return false;
			}
		}
		// compare the rest of the prefix
		const char *str_data = str.GetData();
		const char *patt_data = pattern.GetData();
		D_ASSERT(patt_length <= str_length);
		for (idx_t i = string_t::PREFIX_LENGTH; i < patt_length; ++i) {
			if (str_data[i] != patt_data[i]) {
				return false;
			}
		}
		return true;
	}
}

struct PrefixOperator {
	template <class TA, class TB, class TR>
	static inline TR Operation(TA left, TB right) {
		return PrefixFunction(left, right);
	}
};

void PrefixFunctionProfiled(DataChunk &args, ExpressionState &state, Vector &result) {
	const bool in_table_filter_scope = QueryProfiler::InTableFilterExpressionScope();
	const bool has_context = state.HasContext();
	if (in_table_filter_scope && !has_context) {
		printf("[WARN] Table-scan LIKE profiling skipped in PrefixFunctionProfiled: missing ExpressionState context\n");
	}
	const bool should_profile = in_table_filter_scope && has_context;
	Profiler profiler;
	if (should_profile) {
		profiler.Start();
	}
	BinaryExecutor::ExecuteStandard<string_t, string_t, bool, PrefixOperator>(args.data[0], args.data[1], result,
	                                                                           args.size());
	if (should_profile) {
		profiler.End();
		QueryProfiler::Get(state.GetContext()).AddTableScanStringLikeOperatorMetrics(ElapsedNs(profiler));
	}
}

} // namespace

ScalarFunction PrefixFun::GetFunction() {
	return ScalarFunction("prefix",                                     // name of the function
	                      {LogicalType::VARCHAR, LogicalType::VARCHAR}, // argument list
	                      LogicalType::BOOLEAN,                         // return type
	                      PrefixFunctionProfiled);
}

} // namespace duckdb
