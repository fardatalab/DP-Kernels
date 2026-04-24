#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/profiler.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include <cstdio>

namespace duckdb {

namespace {

static uint64_t ElapsedNs(const Profiler &profiler) {
	return static_cast<uint64_t>(profiler.Elapsed() * 1000000000.0);
}

// Detect whether this operator expression should be counted as a string
// constant-comparison predicate in table-scan expression-filter scope.
static bool ShouldProfileStringInOperator(const BoundOperatorExpression &expr) {
	auto expression_type = expr.GetExpressionType();
	if (expression_type != ExpressionType::COMPARE_IN && expression_type != ExpressionType::COMPARE_NOT_IN) {
		return false;
	}
	if (expr.children.empty()) {
		return false;
	}
	return expr.children[0]->return_type.InternalType() == PhysicalType::VARCHAR;
}

} // namespace

unique_ptr<ExpressionState> ExpressionExecutor::InitializeState(const BoundOperatorExpression &expr,
                                                                ExpressionExecutorState &root) {
	auto result = make_uniq<ExpressionState>(expr, root);
	for (auto &child : expr.children) {
		result->AddChild(*child);
	}

	result->Finalize();
	return result;
}

void ExpressionExecutor::Execute(const BoundOperatorExpression &expr, ExpressionState *state,
                                 const SelectionVector *sel, idx_t count, Vector &result) {
	// special handling for special snowflake 'IN'
	// IN has n children
	auto expression_type = expr.GetExpressionType();
	if (expression_type == ExpressionType::COMPARE_IN || expression_type == ExpressionType::COMPARE_NOT_IN) {
		const bool in_table_filter_scope = QueryProfiler::InTableFilterExpressionScope();
		const bool should_profile_string_in = in_table_filter_scope && ShouldProfileStringInOperator(expr);
		const bool has_context = state && state->HasContext();
		if (should_profile_string_in && !has_context) {
			printf("[WARN] Table-scan string constant-comparison profiling skipped in ExpressionExecutor::Execute(IN): missing ExpressionState context\n");
		}

		Profiler profiler;
		if (should_profile_string_in && has_context) {
			profiler.Start();
		}

		if (expr.children.size() < 2) {
			throw InvalidInputException("IN needs at least two children");
		}

		Vector left(expr.children[0]->return_type);
		// eval left side
		Execute(*expr.children[0], state->child_states[0].get(), sel, count, left);

		// init result to false
		Vector intermediate(LogicalType::BOOLEAN);
		Value false_val = Value::BOOLEAN(false);
		intermediate.Reference(false_val);

		// in rhs is a list of constants
		// for every child, OR the result of the comparison with the left
		// to get the overall result.
		for (idx_t child = 1; child < expr.children.size(); child++) {
			Vector vector_to_check(expr.children[child]->return_type);
			Vector comp_res(LogicalType::BOOLEAN);

			Execute(*expr.children[child], state->child_states[child].get(), sel, count, vector_to_check);
			VectorOperations::Equals(left, vector_to_check, comp_res, count);

			if (child == 1) {
				// first child: move to result
				intermediate.Reference(comp_res);
			} else {
				// otherwise OR together
				Vector new_result(LogicalType::BOOLEAN, true, false);
				VectorOperations::Or(intermediate, comp_res, new_result, count);
				intermediate.Reference(new_result);
			}
		}
		if (expression_type == ExpressionType::COMPARE_NOT_IN) {
			// NOT IN: invert result
			VectorOperations::Not(intermediate, result, count);
		} else {
			// directly use the result
			result.Reference(intermediate);
		}
		if (should_profile_string_in && has_context) {
			profiler.End();
			QueryProfiler::Get(state->GetContext()).AddTableScanStringConstantComparisonMetrics(ElapsedNs(profiler));
		}
	} else if (expression_type == ExpressionType::OPERATOR_COALESCE) {
		SelectionVector sel_a(count);
		SelectionVector sel_b(count);
		SelectionVector slice_sel(count);
		SelectionVector result_sel(count);
		SelectionVector *next_sel = &sel_a;
		const SelectionVector *current_sel = sel;
		idx_t remaining_count = count;
		idx_t next_count;
		for (idx_t child = 0; child < expr.children.size(); child++) {
			Vector vector_to_check(expr.children[child]->return_type);
			Execute(*expr.children[child], state->child_states[child].get(), current_sel, remaining_count,
			        vector_to_check);

			UnifiedVectorFormat vdata;
			vector_to_check.ToUnifiedFormat(remaining_count, vdata);

			idx_t result_count = 0;
			next_count = 0;
			for (idx_t i = 0; i < remaining_count; i++) {
				auto base_idx = current_sel ? current_sel->get_index(i) : i;
				auto idx = vdata.sel->get_index(i);
				if (vdata.validity.RowIsValid(idx)) {
					slice_sel.set_index(result_count, i);
					result_sel.set_index(result_count++, base_idx);
				} else {
					next_sel->set_index(next_count++, base_idx);
				}
			}
			if (result_count > 0) {
				vector_to_check.Slice(slice_sel, result_count);
				FillSwitch(vector_to_check, result, result_sel, NumericCast<sel_t>(result_count));
			}
			current_sel = next_sel;
			next_sel = next_sel == &sel_a ? &sel_b : &sel_a;
			remaining_count = next_count;
			if (next_count == 0) {
				break;
			}
		}
		if (remaining_count > 0) {
			for (idx_t i = 0; i < remaining_count; i++) {
				FlatVector::SetNull(result, current_sel->get_index(i), true);
			}
		}
		if (sel) {
			result.Slice(*sel, count);
		} else if (count == 1) {
			result.SetVectorType(VectorType::CONSTANT_VECTOR);
		}
	} else if (expression_type == ExpressionType::OPERATOR_TRY) {
		auto &child_state = *state->child_states[0];
		try {
			Execute(*expr.children[0], &child_state, sel, count, result);
			return;
		} catch (std::exception &ex) {
			ErrorData error(ex);
			auto error_type = error.Type();
			if (!Exception::IsExecutionError(error_type)) {
				throw;
			}
		}
		SelectionVector selvec(1);
		DataChunk intermediate;
		intermediate.Initialize(GetAllocator(), {result.GetType()}, 1);
		for (idx_t i = 0; i < count; i++) {
			intermediate.Reset();
			intermediate.SetCardinality(1);
			selvec.set_index(0, sel ? sel->get_index(i) : i);
			Value val(result.GetType());
			try {
				Execute(*expr.children[0], &child_state, &selvec, 1, intermediate.data[0]);
				val = intermediate.GetValue(0, 0);
			} catch (std::exception &ex) {
				ErrorData error(ex);
				auto error_type = error.Type();
				if (!Exception::IsExecutionError(error_type)) {
					throw;
				}
			}
			result.SetValue(i, val);
		}
		if (count == 1) {
			result.SetVectorType(VectorType::CONSTANT_VECTOR);
		}
	} else if (expr.children.size() == 1) {
		state->intermediate_chunk.Reset();
		auto &child = state->intermediate_chunk.data[0];

		Execute(*expr.children[0], state->child_states[0].get(), sel, count, child);
		switch (expr.GetExpressionType()) {
		case ExpressionType::OPERATOR_NOT: {
			VectorOperations::Not(child, result, count);
			break;
		}
		case ExpressionType::OPERATOR_IS_NULL: {
			VectorOperations::IsNull(child, result, count);
			break;
		}
		case ExpressionType::OPERATOR_IS_NOT_NULL: {
			VectorOperations::IsNotNull(child, result, count);
			break;
		}
		default:
			throw NotImplementedException("Unsupported operator type with 1 child!");
		}
	} else {
		throw NotImplementedException("operator");
	}
}

} // namespace duckdb
