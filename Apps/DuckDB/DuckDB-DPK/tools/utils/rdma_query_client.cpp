#include "rdma_query_transport.hpp"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

#include <rdma/rdma_cma.h>

using rdma_query::EscapeForCsv;
using rdma_query::MessageType;
using rdma_query::NowNanos;
using rdma_query::ReadFileToString;
using rdma_query::RdmaEndpoint;
using rdma_query::WriteStringToFile;

namespace {

/**
 * One query input item and its explicit correlation id.
 */
struct QueryInput {
	uint32_t query_id;
	std::string path;
};

/**
 * Runtime configuration for the RDMA query client executable.
 */
struct ClientConfig {
	std::string server_ip;
	uint16_t port;
	std::string metrics_file;

	bool single_query_mode;
	uint32_t single_query_id;
	std::string single_query_file;

	std::string query_dir;
	std::string query_template;
	int query_start;
	int query_end;

	std::string result_dir;

	ClientConfig()
	    : server_ip("127.0.0.1"), port(7500), metrics_file("client_metrics.csv"), single_query_mode(false),
	      single_query_id(0), query_template("tpch-q%d.sql"), query_start(1), query_end(22) {
	}
};

/**
 * CSV logger dedicated to per-query client-side transport measurements.
 */
class ClientMetricsLogger {
public:
	explicit ClientMetricsLogger(const std::string &path) : out_(path.c_str(), std::ios::out | std::ios::trunc) {
		if (!out_.is_open()) {
			throw std::runtime_error("failed to open client metrics file: " + path);
		}
		out_ << "timestamp_ns,query_id,query_bytes,query_send_ns,e2e_wall_ns,response_bytes,response_type,status,error\n";
		out_.flush();
	}

	/**
	 * Append one client metrics row and flush immediately.
	 */
	void LogRow(uint64_t timestamp_ns, uint32_t query_id, size_t query_bytes, uint64_t query_send_ns,
	            uint64_t e2e_wall_ns, size_t response_bytes, const std::string &response_type,
	            const std::string &status, const std::string &error) {
		out_ << timestamp_ns << ',' << query_id << ',' << query_bytes << ',' << query_send_ns << ',' << e2e_wall_ns
		     << ',' << response_bytes << ',' << EscapeForCsv(response_type) << ',' << EscapeForCsv(status) << ','
		     << EscapeForCsv(error) << '\n';
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
 * Parse a positive integer with explicit range guard.
 */
bool ParsePositiveInt(const std::string &value, int min_value, int max_value, int &out, std::string &error) {
	char *end = nullptr;
	errno = 0;
	const long parsed = std::strtol(value.c_str(), &end, 10);
	if (errno != 0 || !end || *end != '\0' || parsed < min_value || parsed > max_value) {
		error = "invalid integer value: " + value;
		return false;
	}
	out = static_cast<int>(parsed);
	return true;
}

/**
 * Parse a positive 32-bit query id.
 */
bool ParseQueryId(const std::string &value, uint32_t &out, std::string &error) {
	int parsed = 0;
	if (!ParsePositiveInt(value, 1, 2147483647, parsed, error)) {
		return false;
	}
	out = static_cast<uint32_t>(parsed);
	return true;
}

/**
 * Print CLI usage for both single-query and batch modes.
 */
void PrintUsage(const char *prog) {
	std::cerr << "Usage: " << prog << " [options]\n"
	          << "  --server-ip <ip>            default: 127.0.0.1\n"
	          << "  --port <port>               default: 7500\n"
	          << "  --metrics-file <csv>        default: client_metrics.csv\n"
	          << "\n"
	          << "  Single-query mode:\n"
	          << "    --query-id <id> --query-file <path>\n"
	          << "\n"
	          << "  Batch mode (TPCH-style):\n"
	          << "    --query-dir <dir>\n"
	          << "    [--query-template <fmt>]  default: tpch-q%d.sql\n"
	          << "    [--query-start <n>]       default: 1\n"
	          << "    [--query-end <n>]         default: 22\n"
	          << "\n"
	          << "  Optional:\n"
	          << "    --result-dir <dir>        dump returned text as query<ID>.txt\n";
}

/**
 * Parse client CLI arguments into ClientConfig.
 */
bool ParseArgs(int argc, char **argv, ClientConfig &config, std::string &error) {
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

		if (arg == "--server-ip") {
			const char *v = require_value(arg);
			if (!v) {
				return false;
			}
			config.server_ip = v;
		} else if (arg == "--port") {
			const char *v = require_value(arg);
			if (!v) {
				return false;
			}
			if (!ParsePort(v, config.port, error)) {
				return false;
			}
		} else if (arg == "--metrics-file") {
			const char *v = require_value(arg);
			if (!v) {
				return false;
			}
			config.metrics_file = v;
		} else if (arg == "--query-id") {
			const char *v = require_value(arg);
			if (!v) {
				return false;
			}
			if (!ParseQueryId(v, config.single_query_id, error)) {
				return false;
			}
			config.single_query_mode = true;
		} else if (arg == "--query-file") {
			const char *v = require_value(arg);
			if (!v) {
				return false;
			}
			config.single_query_file = v;
		} else if (arg == "--query-dir") {
			const char *v = require_value(arg);
			if (!v) {
				return false;
			}
			config.query_dir = v;
		} else if (arg == "--query-template") {
			const char *v = require_value(arg);
			if (!v) {
				return false;
			}
			config.query_template = v;
		} else if (arg == "--query-start") {
			const char *v = require_value(arg);
			if (!v) {
				return false;
			}
			if (!ParsePositiveInt(v, 1, 1000000, config.query_start, error)) {
				return false;
			}
		} else if (arg == "--query-end") {
			const char *v = require_value(arg);
			if (!v) {
				return false;
			}
			if (!ParsePositiveInt(v, 1, 1000000, config.query_end, error)) {
				return false;
			}
		} else if (arg == "--result-dir") {
			const char *v = require_value(arg);
			if (!v) {
				return false;
			}
			config.result_dir = v;
		} else if (arg == "--help" || arg == "-h") {
			PrintUsage(argv[0]);
			std::exit(0);
		} else {
			error = "unknown argument: " + arg;
			return false;
		}
	}

	if (config.single_query_mode) {
		if (config.single_query_id == 0 || config.single_query_file.empty()) {
			error = "single-query mode requires both --query-id and --query-file";
			return false;
		}
		if (!config.query_dir.empty()) {
			error = "single-query mode and --query-dir are mutually exclusive";
			return false;
		}
	} else {
		if (config.query_dir.empty()) {
			error = "batch mode requires --query-dir (or use single-query mode)";
			return false;
		}
		if (config.query_start > config.query_end) {
			error = "--query-start cannot be greater than --query-end";
			return false;
		}
	}

	return true;
}

/**
 * Build the query workload list based on CLI mode.
 */
bool BuildQueryList(const ClientConfig &config, std::vector<QueryInput> &queries, std::string &error) {
	queries.clear();
	if (config.single_query_mode) {
		QueryInput q;
		q.query_id = config.single_query_id;
		q.path = config.single_query_file;
		queries.push_back(q);
		return true;
	}

	for (int id = config.query_start; id <= config.query_end; id++) {
		char file_name[512];
		std::snprintf(file_name, sizeof(file_name), config.query_template.c_str(), id);

		QueryInput q;
		q.query_id = static_cast<uint32_t>(id);
		q.path = config.query_dir + "/" + file_name;
		queries.push_back(q);
	}
	return true;
}

/**
 * Ensure the result output directory exists when result dumping is enabled.
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
 * Receive one specific CM event type and verify status.
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
 * Establish one client-side RDMA connection and initialize endpoint resources.
 */
bool ConnectToServer(const ClientConfig &config, rdma_event_channel *&event_channel, rdma_cm_id *&cm_id,
                     RdmaEndpoint &endpoint, std::string &error) {
	event_channel = rdma_create_event_channel();
	if (!event_channel) {
		error = rdma_query::ErrnoMessage("rdma_create_event_channel failed");
		return false;
	}
	if (rdma_create_id(event_channel, &cm_id, nullptr, RDMA_PS_TCP) != 0) {
		error = rdma_query::ErrnoMessage("rdma_create_id(client) failed");
		return false;
	}

	sockaddr_in remote_addr;
	std::memset(&remote_addr, 0, sizeof(remote_addr));
	remote_addr.sin_family = AF_INET;
	remote_addr.sin_port = htons(config.port);
	if (inet_pton(AF_INET, config.server_ip.c_str(), &remote_addr.sin_addr) != 1) {
		error = "invalid server ip: " + config.server_ip;
		return false;
	}

	if (rdma_resolve_addr(cm_id, nullptr, reinterpret_cast<sockaddr *>(&remote_addr), 5000) != 0) {
		error = rdma_query::ErrnoMessage("rdma_resolve_addr failed");
		return false;
	}
	rdma_cm_event *event = nullptr;
	if (!WaitForCmEvent(event_channel, RDMA_CM_EVENT_ADDR_RESOLVED, event, error)) {
		if (event) {
			rdma_ack_cm_event(event);
		}
		return false;
	}
	rdma_ack_cm_event(event);
	event = nullptr;

	if (rdma_resolve_route(cm_id, 5000) != 0) {
		error = rdma_query::ErrnoMessage("rdma_resolve_route failed");
		return false;
	}
	if (!WaitForCmEvent(event_channel, RDMA_CM_EVENT_ROUTE_RESOLVED, event, error)) {
		if (event) {
			rdma_ack_cm_event(event);
		}
		return false;
	}
	rdma_ack_cm_event(event);
	event = nullptr;

	if (!endpoint.Initialize(cm_id, error)) {
		return false;
	}
	if (!endpoint.PostInitialReceives(error)) {
		return false;
	}

	rdma_conn_param conn_param;
	std::memset(&conn_param, 0, sizeof(conn_param));
	conn_param.responder_resources = 16;
	conn_param.initiator_depth = 16;
	conn_param.retry_count = 7;
	conn_param.rnr_retry_count = 7;
	if (rdma_connect(cm_id, &conn_param) != 0) {
		error = rdma_query::ErrnoMessage("rdma_connect failed");
		return false;
	}
	if (!WaitForCmEvent(event_channel, RDMA_CM_EVENT_ESTABLISHED, event, error)) {
		if (event) {
			rdma_ack_cm_event(event);
		}
		return false;
	}
	rdma_ack_cm_event(event);
	return true;
}

/**
 * Build a result output file path for a query id.
 */
std::string BuildResultFilePath(const std::string &result_dir, uint32_t query_id) {
	std::ostringstream ss;
	ss << result_dir << "/query" << query_id << ".txt";
	return ss.str();
}

} // namespace

int main(int argc, char **argv) {
	ClientConfig config;
	std::string error;
	if (!ParseArgs(argc, argv, config, error)) {
		std::cerr << "Argument error: " << error << "\n";
		PrintUsage(argv[0]);
		return 1;
	}

	std::vector<QueryInput> queries;
	if (!BuildQueryList(config, queries, error)) {
		std::cerr << "Failed to build query list: " << error << "\n";
		return 1;
	}
	if (!EnsureDirectoryExists(config.result_dir, error)) {
		std::cerr << "Failed to create result dir: " << error << "\n";
		return 1;
	}

	std::cerr << "[client] connecting to " << config.server_ip << ':' << config.port << " with " << queries.size()
	          << " queries\n";

	ClientMetricsLogger metrics(config.metrics_file);
	rdma_event_channel *event_channel = nullptr;
	rdma_cm_id *cm_id = nullptr;
	RdmaEndpoint endpoint;
	if (!ConnectToServer(config, event_channel, cm_id, endpoint, error)) {
		std::cerr << "[client] RDMA connect failed: " << error << "\n";
		if (cm_id) {
			rdma_destroy_id(cm_id);
		}
		if (event_channel) {
			rdma_destroy_event_channel(event_channel);
		}
		return 1;
	}

	std::cerr << "[client] connection established, sending queries\n";
	for (size_t i = 0; i < queries.size(); i++) {
		const QueryInput &q = queries[i];
		std::string sql;
		if (!ReadFileToString(q.path, sql, error)) {
			std::cerr << "[client] failed to read query file for query_id=" << q.query_id << ": " << error << "\n";
			metrics.LogRow(NowNanos(), q.query_id, 0, 0, 0, 0, "none", "input_error", error);
			continue;
		}

		// End-to-end wall clock starts before the first query SEND posting.
		const uint64_t e2e_start_ns = NowNanos();
		uint64_t query_send_ns = 0;
		if (!endpoint.SendMessage(MessageType::QUERY_TEXT, q.query_id, sql.data(), sql.size(), query_send_ns, error)) {
			const uint64_t e2e_wall_ns = NowNanos() - e2e_start_ns;
			std::cerr << "[client] failed sending query_id=" << q.query_id << ": " << error << "\n";
			metrics.LogRow(NowNanos(), q.query_id, sql.size(), query_send_ns, e2e_wall_ns, 0, "none", "transport_error",
			               error);
			break;
		}

		MessageType response_type = MessageType::ERROR_TEXT;
		uint32_t response_query_id = 0;
		std::string response_payload;
		if (!endpoint.ReceiveMessage(response_type, response_query_id, response_payload, error)) {
			const uint64_t e2e_wall_ns = NowNanos() - e2e_start_ns;
			std::cerr << "[client] failed receiving response for query_id=" << q.query_id << ": " << error << "\n";
			metrics.LogRow(NowNanos(), q.query_id, sql.size(), query_send_ns, e2e_wall_ns, 0, "none", "transport_error",
			               error);
			break;
		}
		const uint64_t e2e_wall_ns = NowNanos() - e2e_start_ns;

		if (response_query_id != q.query_id) {
			std::ostringstream mismatch;
			mismatch << "response query_id mismatch, expected=" << q.query_id << " actual=" << response_query_id;
			std::cerr << "[client] " << mismatch.str() << "\n";
			metrics.LogRow(NowNanos(), q.query_id, sql.size(), query_send_ns, e2e_wall_ns, response_payload.size(),
			               "mismatch", "protocol_error", mismatch.str());
			break;
		}

		std::string response_type_text = "unknown";
		std::string status = "ok";
		std::string row_error;
		if (response_type == MessageType::RESULT_TEXT) {
			response_type_text = "result";
		} else if (response_type == MessageType::ERROR_TEXT) {
			response_type_text = "error";
			status = "query_error";
			row_error = response_payload;
		} else {
			response_type_text = "invalid";
			status = "protocol_error";
			row_error = "unexpected response message type";
		}

		if (!config.result_dir.empty() && response_type == MessageType::RESULT_TEXT) {
			const std::string output_path = BuildResultFilePath(config.result_dir, q.query_id);
			std::string write_error;
			if (!WriteStringToFile(output_path, response_payload, write_error)) {
				status = "output_error";
				row_error = write_error;
			}
		}

		metrics.LogRow(NowNanos(), q.query_id, sql.size(), query_send_ns, e2e_wall_ns, response_payload.size(),
		               response_type_text, status, row_error);
		std::cerr << "[client] query_id=" << q.query_id << " status=" << status << " query_send_ns=" << query_send_ns
		          << " e2e_wall_ns=" << e2e_wall_ns << " response_bytes=" << response_payload.size() << "\n";
	}

	uint64_t shutdown_send_ns = 0;
	std::string shutdown_error;
	endpoint.SendMessage(MessageType::SHUTDOWN, 0, nullptr, 0, shutdown_send_ns, shutdown_error);

	if (cm_id) {
		rdma_disconnect(cm_id);
	}
	endpoint.Shutdown();
	if (cm_id) {
		rdma_destroy_id(cm_id);
	}
	if (event_channel) {
		rdma_destroy_event_channel(event_channel);
	}

	std::cerr << "[client] shutdown complete\n";
	return 0;
}
