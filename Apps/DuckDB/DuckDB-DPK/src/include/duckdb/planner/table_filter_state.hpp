//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/table_filter_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/table_filter.hpp"
#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/execution/expression_executor.hpp"

namespace duckdb {
class ClientContext;

//! Thread-local state for executing a table filter
struct TableFilterState {
public:
	virtual ~TableFilterState() = default;

public:
	static unique_ptr<TableFilterState> Initialize(ClientContext &context, const TableFilter &filter);
	//! Context for the query that owns this filter state.
	optional_ptr<ClientContext> context;

public:
	bool HasContext() const {
		return context != nullptr;
	}
	// Non-const access to the ClientContext for callers that will mutate it.
	ClientContext &GetContext() {
		D_ASSERT(context);
		return *context;
	}
	// Const access to the ClientContext for callers that should not mutate it.
	const ClientContext &GetContext() const {
		D_ASSERT(context);
		return *context;
	}

public:
	template <class TARGET>
	TARGET &Cast() {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<TARGET &>(*this);
	}
	template <class TARGET>
	const TARGET &Cast() const {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<const TARGET &>(*this);
	}
};

struct ConjunctionAndFilterState : public TableFilterState {
public:
	vector<unique_ptr<TableFilterState>> child_states;
};

struct ConjunctionOrFilterState : public TableFilterState {
public:
	vector<unique_ptr<TableFilterState>> child_states;
};

struct ExpressionFilterState : public TableFilterState {
public:
	ExpressionFilterState(ClientContext &context, const Expression &expression);

	ExpressionExecutor executor;
};

} // namespace duckdb
