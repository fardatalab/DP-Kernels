#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include <rdma/rdma_cma.h>

namespace rdma_query {

static const uint32_t kFrameMagic = 0x52514442; // "RQDB"
static const uint16_t kFrameVersion = 1;
// 128 MiB chunks (>= 100 MB as requested) to avoid splitting large text results.
static const size_t kMaxFramePayloadBytes = 128UL << 20;
// Keep the posted receive ring small because each slot now reserves a large pinned MR region.
static const size_t kRecvDepth = 2;

enum class MessageType : uint16_t {
	QUERY_TEXT = 1,
	RESULT_TEXT = 2,
	ERROR_TEXT = 3,
	SHUTDOWN = 4,
};

#pragma pack(push, 1)
struct FrameHeader {
	uint32_t magic;
	uint16_t version;
	uint16_t message_type;
	uint32_t query_id;
	uint32_t frame_index;
	uint32_t frame_count;
	uint64_t payload_size;
	uint64_t total_payload_size;
};
#pragma pack(pop)

static_assert(sizeof(FrameHeader) <= 64, "FrameHeader unexpectedly large");

/**
 * Convert the current steady clock reading to nanoseconds.
 */
uint64_t NowNanos();

/**
 * Escape arbitrary text for safe single-cell CSV output.
 */
std::string EscapeForCsv(const std::string &input);

/**
 * Read the whole file into a string.
 */
bool ReadFileToString(const std::string &path, std::string &out, std::string &error);

/**
 * Write a string as a file, replacing previous content.
 */
bool WriteStringToFile(const std::string &path, const std::string &content, std::string &error);

/**
 * Build a simple errno-based error message.
 */
std::string ErrnoMessage(const std::string &context);

/**
 * A connected RDMA endpoint with reusable send/recv buffers.
 *
 * This wrapper keeps one registered send buffer and a fixed-depth ring of
 * registered receive buffers, so the peer can send frames back-to-back while
 * we process completions.
 */
class RdmaEndpoint {
public:
	RdmaEndpoint();
	~RdmaEndpoint();

	RdmaEndpoint(const RdmaEndpoint &) = delete;
	RdmaEndpoint &operator=(const RdmaEndpoint &) = delete;

	/**
	 * Create PD/CQ/QP and register all buffers for an already-created cm_id.
	 */
	bool Initialize(rdma_cm_id *id, std::string &error);

	/**
	 * Destroy QP/CQ/PD/MR resources attached to this endpoint.
	 */
	void Shutdown();

	/**
	 * Post all receives once before any incoming traffic.
	 */
	bool PostInitialReceives(std::string &error);

	/**
	 * Send one application message split into one or more RDMA SEND frames.
	 *
	 * send_elapsed_ns: sender-side time from first post_send until last SEND CQE.
	 */
	bool SendMessage(MessageType message_type, uint32_t query_id, const char *payload, size_t payload_size,
	                 uint64_t &send_elapsed_ns, std::string &error);

	/**
	 * Receive one full application message assembled from one or more frames.
	 */
	bool ReceiveMessage(MessageType &message_type, uint32_t &query_id, std::string &payload, std::string &error);

	/**
	 * Expose configured max payload for message chunking decisions.
	 */
	size_t MaxFramePayloadBytes() const;

private:
	/**
	 * Post one receive work request for a specific slot in the receive ring.
	 */
	bool PostReceiveSlot(uint64_t slot_index, std::string &error);

	/**
	 * Poll the send CQ until one SEND completion is available.
	 */
	bool WaitSendCompletion(std::string &error);

	/**
	 * Poll the recv CQ until one RECV completion is available.
	 */
	bool WaitRecvCompletion(ibv_wc &wc, std::string &error);

	/**
	 * Build one wire frame in the send buffer and post it.
	 */
	bool SendOneFrame(const FrameHeader &header, const char *payload, size_t payload_size, std::string &error);

private:
	rdma_cm_id *cm_id_;
	ibv_pd *pd_;
	ibv_cq *send_cq_;
	ibv_cq *recv_cq_;
	ibv_mr *send_mr_;
	ibv_mr *recv_mr_;

	std::vector<char> send_buffer_;
	std::vector<char> recv_buffers_;
	std::vector<bool> recv_slot_posted_;
};

} // namespace rdma_query
