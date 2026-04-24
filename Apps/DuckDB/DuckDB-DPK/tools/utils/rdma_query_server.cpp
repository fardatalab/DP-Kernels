#include "duckdb.hpp"

#include "rdma_query_transport.hpp"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>

#include <rdma/rdma_cma.h>

using duckdb::Connection;
using duckdb::DuckDB;
using duckdb::MaterializedQueryResult;
using rdma_query::EscapeForCsv;
using rdma_query::MessageType;
using rdma_query::NowNanos;
using rdma_query::RdmaEndpoint;

namespace {

/**
 * Runtime configuration for the RDMA DuckDB server executable.
 */
struct ServerConfig {
	std::string bind_ip;
	uint16_t port;
	std::string db_path;
	std::string metrics_file;
	std::string profile_output_dir;
	int threads;
	std::string key_name;
	std::string key_value;

	ServerConfig()
	    : bind_ip("0.0.0.0"), port(7500), db_path("tpch_metadata.db"), metrics_file("server_metrics.csv"),
	      profile_output_dir("tpch_profiles_rdma"), threads(16) {
	}
};

/**
 * A very small CSV logger dedicated to per-query server metrics.
 */
class ServerMetricsLogger {
public:
	explicit ServerMetricsLogger(const std::string &path) : out_(path.c_str(), std::ios::out | std::ios::trunc) {
		if (!out_.is_open()) {
			throw std::runtime_error("failed to open server metrics file: " + path);
		}
		out_ << "timestamp_ns,query_id,query_bytes,execute_ns,result_bytes,result_send_ns,status,error\n";
		out_.flush();
	}

	/**
	 * Append one metrics row and flush immediately to survive process failures.
	 */
	void LogRow(uint64_t timestamp_ns, uint32_t query_id, size_t query_bytes, uint64_t execute_ns, size_t result_bytes,
	            uint64_t result_send_ns, const std::string &status, const std::string &error) {
		out_ << timestamp_ns << ',' << query_id << ',' << query_bytes << ',' << execute_ns << ',' << result_bytes << ','
		     << result_send_ns << ',' << EscapeForCsv(status) << ',' << EscapeForCsv(error) << '\n';
		out_.flush();
	}

private:
	std::ofstream out_;
};

/**
 * Parse an unsigned 16-bit port from CLI text.
 */
bool ParsePort(const std::string &value, uint16_t &port, std::string &error) {
	char *end = nullptr;
	errno = 0;
	const long parsed = std::strtol(value.c_str(), &end, 10);
	if (errno != 0 || !end || *end != '\0' || parsed <= 0 || parsed > 65535) {
		error = "invalid port: " + value;
		return false;
	}
	port = static_cast<uint16_t>(parsed);
	return true;
}

/**
 * Parse a positive integer for the DuckDB threads setting.
 */
bool ParsePositiveInt(const std::string &value, int &out, std::string &error) {
	char *end = nullptr;
	errno = 0;
	const long parsed = std::strtol(value.c_str(), &end, 10);
	if (errno != 0 || !end || *end != '\0' || parsed <= 0 || parsed > 1024) {
		error = "invalid positive integer: " + value;
		return false;
	}
	out = static_cast<int>(parsed);
	return true;
}

/**
 * Print CLI usage and argument contract.
 */
void PrintUsage(const char *prog) {
	std::cerr << "Usage: " << prog << " [options]\n"
	          << "  --bind-ip <ip>            default: 0.0.0.0\n"
	          << "  --port <port>             default: 7500\n"
	          << "  --db-path <duckdb-file>   default: tpch_metadata.db\n"
	          << "  --metrics-file <csv>      default: server_metrics.csv\n"
	          << "  --profile-dir <dir>       default: tpch_profiles_rdma\n"
	          << "  --threads <n>             default: 16\n"
	          << "  --key-name <name>         optional parquet key name\n"
	          << "  --key-value <value>       optional parquet key value\n";
}

/**
 * Parse server CLI flags into ServerConfig.
 */
bool ParseArgs(int argc, char **argv, ServerConfig &config, std::string &error) {
	for (int i = 1; i < argc; i++) {
		const std::string arg = argv[i];
		auto require_value = [&](const std::string &name) -> const char * {
			if (i + 1 >= argc) {
				error = "missing value for " + name;
				return nullptr;
			}
			i++;
			return argv[i];
		};

		if (arg == "--bind-ip") {
			const char *v = require_value(arg);
			if (!v) {
				return false;
			}
			config.bind_ip = v;
		} else if (arg == "--port") {
			const char *v = require_value(arg);
			if (!v) {
				return false;
			}
			if (!ParsePort(v, config.port, error)) {
				return false;
			}
		} else if (arg == "--db-path") {
			const char *v = require_value(arg);
			if (!v) {
				return false;
			}
			config.db_path = v;
		} else if (arg == "--metrics-file") {
			const char *v = require_value(arg);
			if (!v) {
				return false;
			}
			config.metrics_file = v;
		} else if (arg == "--profile-dir") {
			const char *v = require_value(arg);
			if (!v) {
				return false;
			}
			config.profile_output_dir = v;
		} else if (arg == "--threads") {
			const char *v = require_value(arg);
			if (!v) {
				return false;
			}
			if (!ParsePositiveInt(v, config.threads, error)) {
				return false;
			}
		} else if (arg == "--key-name") {
			const char *v = require_value(arg);
			if (!v) {
				return false;
			}
			config.key_name = v;
		} else if (arg == "--key-value") {
			const char *v = require_value(arg);
			if (!v) {
				return false;
			}
			config.key_value = v;
		} else if (arg == "--help" || arg == "-h") {
			PrintUsage(argv[0]);
			std::exit(0);
		} else {
			error = "unknown argument: " + arg;
			return false;
		}
	}

	if (config.key_name.empty() != config.key_value.empty()) {
		error = "--key-name and --key-value must be provided together";
		return false;
	}
	return true;
}

/**
 * Ensure a directory path exists for profile output files.
 */
bool EnsureDirectoryExists(const std::string &path, std::string &error) {
	if (path.empty()) {
		return true;
	}
	if (mkdir(path.c_str(), 0755) == 0) {
		return true;
	}
	if (errno == EEXIST) {
		return true;
	}
	error = rdma_query::ErrnoMessage("mkdir failed for " + path);
	return false;
}

/**
 * Receive one specific CM event type and validate status.
 * Caller is responsible for calling rdma_ack_cm_event(event).
 */
bool WaitForCmEvent(rdma_event_channel *channel, rdma_cm_event_type expected, rdma_cm_event *&event,
                    std::string &error) {
	if (rdma_get_cm_event(channel, &event) != 0) {
		error = rdma_query::ErrnoMessage("rdma_get_cm_event failed");
		return false;
	}
	if (event->event != expected) {
		std::ostringstream ss;
		ss << "unexpected CM event. expected=" << static_cast<int>(expected)
		   << " actual=" << static_cast<int>(event->event);
		error = ss.str();
		return false;
	}
	if (event->status != 0) {
		std::ostringstream ss;
		ss << "CM event returned non-zero status: " << event->status;
		error = ss.str();
		return false;
	}
	return true;
}

/**
 * Create the server listener CM ID and enter LISTEN state.
 */
bool SetupListener(const ServerConfig &config, rdma_event_channel *&event_channel, rdma_cm_id *&listener,
                   std::string &error) {
	event_channel = rdma_create_event_channel();
	if (!event_channel) {
		error = rdma_query::ErrnoMessage("rdma_create_event_channel failed");
		return false;
	}
	if (rdma_create_id(event_channel, &listener, nullptr, RDMA_PS_TCP) != 0) {
		error = rdma_query::ErrnoMessage("rdma_create_id(listener) failed");
		return false;
	}

	sockaddr_in addr;
	std::memset(&addr, 0, sizeof(addr));
	addr.sin_family = AF_INET;
	addr.sin_port = htons(config.port);
	if (inet_pton(AF_INET, config.bind_ip.c_str(), &addr.sin_addr) != 1) {
		error = "invalid bind ip: " + config.bind_ip;
		return false;
	}

	if (rdma_bind_addr(listener, reinterpret_cast<sockaddr *>(&addr)) != 0) {
		error = rdma_query::ErrnoMessage("rdma_bind_addr failed");
		return false;
	}
	if (rdma_listen(listener, 1) != 0) {
		error = rdma_query::ErrnoMessage("rdma_listen failed");
		return false;
	}
	return true;
}

/**
 * Configure per-connection DuckDB settings needed for TPCH-style runs.
 */
bool ConfigureDuckDB(const ServerConfig &config, Connection &con, std::string &error) {
	{
		std::ostringstream set_threads_sql;
		set_threads_sql << "SET threads TO " << config.threads;
		auto set_threads = con.Query(set_threads_sql.str());
		if (!set_threads || set_threads->HasError()) {
			error = set_threads ? set_threads->GetError() : "failed to set threads";
			return false;
		}
	}

	if (!config.key_name.empty()) {
		std::ostringstream pragma_sql;
		pragma_sql << "PRAGMA add_parquet_key('" << config.key_name << "', '" << config.key_value << "');";
		auto pragma_result = con.Query(pragma_sql.str());
		if (!pragma_result || pragma_result->HasError()) {
			error = pragma_result ? pragma_result->GetError() : "failed to add parquet key";
			return false;
		}
	}
	return true;
}

/**
 * Escape single quotes for safe SQL literal injection in `SET profiling_output`.
 */
std::string EscapeSqlLiteral(const std::string &input) {
	std::string escaped;
	escaped.reserve(input.size() + 8);
	for (size_t i = 0; i < input.size(); i++) {
		const char c = input[i];
		if (c == '\'') {
			escaped.push_back('\'');
			escaped.push_back('\'');
		} else {
			escaped.push_back(c);
		}
	}
	return escaped;
}

/**
 * Build per-query profiling output file path.
 */
std::string BuildProfileFilePath(const ServerConfig &config, uint32_t query_id) {
	std::ostringstream ss;
	ss << config.profile_output_dir << "/query" << query_id << ".json";
	return ss.str();
}

/**
 * Configure profiling just before running one query, mirroring run_tpch.sh behavior.
 */
bool ConfigureProfilingForQuery(const ServerConfig &config, Connection &con, uint32_t query_id,
                                std::string &profile_output_path, std::string &error) {
	if (config.profile_output_dir.empty()) {
		profile_output_path.clear();
		return true;
	}

	profile_output_path = BuildProfileFilePath(config, query_id);
	const std::string escaped_path = EscapeSqlLiteral(profile_output_path);

	std::ostringstream set_output_sql;
	set_output_sql << "SET profiling_output = '" << escaped_path << "'";
	auto set_output_result = con.Query(set_output_sql.str());
	if (!set_output_result || set_output_result->HasError()) {
		error = set_output_result ? set_output_result->GetError() : "failed to set profiling_output";
		return false;
	}

	auto set_mode_result = con.Query("SET profiling_mode = 'detailed'");
	if (!set_mode_result || set_mode_result->HasError()) {
		error = set_mode_result ? set_mode_result->GetError() : "failed to set profiling_mode";
		return false;
	}

	auto set_enable_result = con.Query("SET enable_profiling = 'json'");
	if (!set_enable_result || set_enable_result->HasError()) {
		error = set_enable_result ? set_enable_result->GetError() : "failed to enable json profiling";
		return false;
	}
	return true;
}

/**
 * Execute one SQL text on DuckDB and return either result text or error text.
 */
void ExecuteQueryToText(Connection &con, const std::string &sql, std::string &out_text, bool &ok, std::string &error_text,
                        uint64_t &execute_ns) {
	const uint64_t exec_start_ns = NowNanos();
	std::unique_ptr<MaterializedQueryResult> result = con.Query(sql);
	if (!result) {
		execute_ns = NowNanos() - exec_start_ns;
		ok = false;
		error_text = "duckdb returned null result pointer";
		out_text = error_text;
		return;
	}
	if (result->HasError()) {
		execute_ns = NowNanos() - exec_start_ns;
		ok = false;
		error_text = result->GetError();
		out_text = error_text;
		return;
	}
	ok = true;
	error_text.clear();
	out_text = result->ToString();
	// execute_ns intentionally includes Query + ToString, not just Query.
	execute_ns = NowNanos() - exec_start_ns;
}

} // namespace

int main(int argc, char **argv) {
	ServerConfig config;
	std::string error;
	if (!ParseArgs(argc, argv, config, error)) {
		std::cerr << "Argument error: " << error << "\n";
		PrintUsage(argv[0]);
		return 1;
	}

	std::cerr << "[server] starting with bind_ip=" << config.bind_ip << " port=" << config.port
	          << " db_path=" << config.db_path << " metrics=" << config.metrics_file << "\n";

	ServerMetricsLogger metrics(config.metrics_file);

	DuckDB db(config.db_path);
	Connection con(db);
	if (!ConfigureDuckDB(config, con, error)) {
		std::cerr << "[server] DuckDB setup failed: " << error << "\n";
		return 1;
	}
	if (!EnsureDirectoryExists(config.profile_output_dir, error)) {
		std::cerr << "[server] profile directory setup failed: " << error << "\n";
		return 1;
	}

	rdma_event_channel *event_channel = nullptr;
	rdma_cm_id *listener = nullptr;
	rdma_cm_id *client_id = nullptr;
	RdmaEndpoint endpoint;

	if (!SetupListener(config, event_channel, listener, error)) {
		std::cerr << "[server] listener setup failed: " << error << "\n";
		if (listener) {
			rdma_destroy_id(listener);
		}
		if (event_channel) {
			rdma_destroy_event_channel(event_channel);
		}
		return 1;
	}

	std::cerr << "[server] waiting for RDMA client connection...\n";
	rdma_cm_event *event = nullptr;
	if (!WaitForCmEvent(event_channel, RDMA_CM_EVENT_CONNECT_REQUEST, event, error)) {
		std::cerr << "[server] failed waiting for CONNECT_REQUEST: " << error << "\n";
		if (event) {
			rdma_ack_cm_event(event);
		}
		rdma_destroy_id(listener);
		rdma_destroy_event_channel(event_channel);
		return 1;
	}
	client_id = event->id;

	if (!endpoint.Initialize(client_id, error)) {
		std::cerr << "[server] endpoint initialize failed: " << error << "\n";
		rdma_ack_cm_event(event);
		rdma_destroy_id(client_id);
		rdma_destroy_id(listener);
		rdma_destroy_event_channel(event_channel);
		return 1;
	}
	if (!endpoint.PostInitialReceives(error)) {
		std::cerr << "[server] posting receive ring failed: " << error << "\n";
		rdma_ack_cm_event(event);
		endpoint.Shutdown();
		rdma_destroy_id(client_id);
		rdma_destroy_id(listener);
		rdma_destroy_event_channel(event_channel);
		return 1;
	}

	rdma_conn_param conn_param;
	std::memset(&conn_param, 0, sizeof(conn_param));
	conn_param.responder_resources = 16;
	conn_param.initiator_depth = 16;
	conn_param.rnr_retry_count = 7;
	if (rdma_accept(client_id, &conn_param) != 0) {
		std::cerr << "[server] rdma_accept failed: " << rdma_query::ErrnoMessage("rdma_accept") << "\n";
		rdma_ack_cm_event(event);
		endpoint.Shutdown();
		rdma_destroy_id(client_id);
		rdma_destroy_id(listener);
		rdma_destroy_event_channel(event_channel);
		return 1;
	}
	rdma_ack_cm_event(event);
	event = nullptr;

	if (!WaitForCmEvent(event_channel, RDMA_CM_EVENT_ESTABLISHED, event, error)) {
		std::cerr << "[server] failed waiting for ESTABLISHED: " << error << "\n";
		if (event) {
			rdma_ack_cm_event(event);
		}
		endpoint.Shutdown();
		rdma_destroy_id(client_id);
		rdma_destroy_id(listener);
		rdma_destroy_event_channel(event_channel);
		return 1;
	}
	rdma_ack_cm_event(event);
	event = nullptr;

	std::cerr << "[server] RDMA connection established, entering query loop\n";
	bool running = true;
	while (running) {
		MessageType request_type = MessageType::QUERY_TEXT;
		uint32_t query_id = 0;
		std::string query_payload;
		if (!endpoint.ReceiveMessage(request_type, query_id, query_payload, error)) {
			std::cerr << "[server] ReceiveMessage failed: " << error << "\n";
			break;
		}

		if (request_type == MessageType::SHUTDOWN) {
			std::cerr << "[server] received shutdown request\n";
			break;
		}

		if (request_type != MessageType::QUERY_TEXT) {
			const std::string unexpected = "unexpected request message type";
			uint64_t send_ns = 0;
			std::string send_error;
			endpoint.SendMessage(MessageType::ERROR_TEXT, query_id, unexpected.data(), unexpected.size(), send_ns,
			                     send_error);
			metrics.LogRow(NowNanos(), query_id, query_payload.size(), 0, unexpected.size(), send_ns, "protocol_error",
			               unexpected);
			continue;
		}

		std::cerr << "[server] query_id=" << query_id << " bytes=" << query_payload.size() << " received\n";

		uint64_t profiling_setup_ns = 0;
		const uint64_t profiling_setup_start_ns = NowNanos();
		std::string profile_output_path;
		if (!ConfigureProfilingForQuery(config, con, query_id, profile_output_path, error)) {
			profiling_setup_ns = NowNanos() - profiling_setup_start_ns;
			const std::string profiling_error = "profiling setup failed: " + error;
			uint64_t send_ns = 0;
			std::string send_error;
			if (!endpoint.SendMessage(MessageType::ERROR_TEXT, query_id, profiling_error.data(), profiling_error.size(),
			                          send_ns, send_error)) {
				std::cerr << "[server] SendMessage failed while returning profiling error for query_id=" << query_id
				          << ": " << send_error << "\n";
				metrics.LogRow(NowNanos(), query_id, query_payload.size(), profiling_setup_ns, profiling_error.size(), send_ns,
				               "transport_error", send_error);
				break;
			}
			metrics.LogRow(NowNanos(), query_id, query_payload.size(), profiling_setup_ns, profiling_error.size(), send_ns,
			               "profiling_error", profiling_error);
			continue;
		}
		profiling_setup_ns = NowNanos() - profiling_setup_start_ns;

		std::string result_payload;
		bool query_ok = false;
		std::string query_error;
		uint64_t query_and_stringify_ns = 0;
		ExecuteQueryToText(con, query_payload, result_payload, query_ok, query_error, query_and_stringify_ns);
		// execute_ns now matches the requested accounting contract:
		// profiling setup + query execution + ToString materialization.
		const uint64_t execute_ns = profiling_setup_ns + query_and_stringify_ns;

		const MessageType response_type = query_ok ? MessageType::RESULT_TEXT : MessageType::ERROR_TEXT;
		uint64_t result_send_ns = 0;
		std::string send_error;
		if (!endpoint.SendMessage(response_type, query_id, result_payload.data(), result_payload.size(), result_send_ns,
		                         send_error)) {
			std::cerr << "[server] SendMessage failed for query_id=" << query_id << ": " << send_error << "\n";
			metrics.LogRow(NowNanos(), query_id, query_payload.size(), execute_ns, result_payload.size(), result_send_ns,
			               "transport_error", send_error);
			break;
		}

		const std::string status = query_ok ? "ok" : "query_error";
		metrics.LogRow(NowNanos(), query_id, query_payload.size(), execute_ns, result_payload.size(), result_send_ns,
		               status, query_error);
		std::cerr << "[server] query_id=" << query_id << " status=" << status << " execute_ns=" << execute_ns
		          << " result_send_ns=" << result_send_ns << " result_bytes=" << result_payload.size()
		          << " profile_file=" << (profile_output_path.empty() ? "none" : profile_output_path) << "\n";
	}

	if (client_id) {
		rdma_disconnect(client_id);
	}
	endpoint.Shutdown();
	if (client_id) {
		rdma_destroy_id(client_id);
	}
	if (listener) {
		rdma_destroy_id(listener);
	}
	if (event_channel) {
		rdma_destroy_event_channel(event_channel);
	}

	std::cerr << "[server] shutdown complete\n";
	return 0;
}
