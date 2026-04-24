#include "rdma_query_transport.hpp"

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <fstream>
#include <sstream>

#include <infiniband/verbs.h>

namespace rdma_query {

namespace {

static const size_t kMaxFrameBytes = sizeof(FrameHeader) + kMaxFramePayloadBytes;

/**
 * Build a formatted message string for transport-level validation failures.
 */
std::string ValidationError(const std::string &message) {
	return "transport validation failed: " + message;
}

} // namespace

uint64_t NowNanos() {
	const auto now = std::chrono::steady_clock::now().time_since_epoch();
	return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(now).count());
}

std::string EscapeForCsv(const std::string &input) {
	std::string escaped;
	escaped.reserve(input.size() + 8);
	escaped.push_back('"');
	for (size_t i = 0; i < input.size(); i++) {
		const char c = input[i];
		if (c == '"') {
			escaped.push_back('"');
			escaped.push_back('"');
		} else if (c == '\n' || c == '\r') {
			escaped.push_back(' ');
		} else {
			escaped.push_back(c);
		}
	}
	escaped.push_back('"');
	return escaped;
}

bool ReadFileToString(const std::string &path, std::string &out, std::string &error) {
	std::ifstream in(path.c_str(), std::ios::in | std::ios::binary);
	if (!in.is_open()) {
		error = "failed to open file for read: " + path;
		return false;
	}
	std::ostringstream buffer;
	buffer << in.rdbuf();
	if (!in.good() && !in.eof()) {
		error = "failed while reading file: " + path;
		return false;
	}
	out = buffer.str();
	return true;
}

bool WriteStringToFile(const std::string &path, const std::string &content, std::string &error) {
	std::ofstream out(path.c_str(), std::ios::out | std::ios::binary | std::ios::trunc);
	if (!out.is_open()) {
		error = "failed to open file for write: " + path;
		return false;
	}
	out.write(content.data(), static_cast<std::streamsize>(content.size()));
	if (!out.good()) {
		error = "failed while writing file: " + path;
		return false;
	}
	return true;
}

std::string ErrnoMessage(const std::string &context) {
	std::ostringstream ss;
	ss << context << ": errno=" << errno << " (" << std::strerror(errno) << ")";
	return ss.str();
}

RdmaEndpoint::RdmaEndpoint()
    : cm_id_(nullptr), pd_(nullptr), send_cq_(nullptr), recv_cq_(nullptr), send_mr_(nullptr), recv_mr_(nullptr) {
}

RdmaEndpoint::~RdmaEndpoint() {
	Shutdown();
}

bool RdmaEndpoint::Initialize(rdma_cm_id *id, std::string &error) {
	if (!id || !id->verbs) {
		error = "Initialize called with invalid rdma_cm_id";
		return false;
	}
	if (cm_id_) {
		error = "Initialize called twice on RdmaEndpoint";
		return false;
	}
	cm_id_ = id;

	pd_ = ibv_alloc_pd(cm_id_->verbs);
	if (!pd_) {
		error = ErrnoMessage("ibv_alloc_pd failed");
		return false;
	}

	send_cq_ = ibv_create_cq(cm_id_->verbs, static_cast<int>(kRecvDepth * 4), nullptr, nullptr, 0);
	if (!send_cq_) {
		error = ErrnoMessage("ibv_create_cq(send) failed");
		return false;
	}
	recv_cq_ = ibv_create_cq(cm_id_->verbs, static_cast<int>(kRecvDepth * 4), nullptr, nullptr, 0);
	if (!recv_cq_) {
		error = ErrnoMessage("ibv_create_cq(recv) failed");
		return false;
	}

	ibv_qp_init_attr qp_attr;
	std::memset(&qp_attr, 0, sizeof(qp_attr));
	qp_attr.send_cq = send_cq_;
	qp_attr.recv_cq = recv_cq_;
	qp_attr.qp_type = IBV_QPT_RC;
	qp_attr.cap.max_send_wr = static_cast<uint32_t>(kRecvDepth * 4);
	qp_attr.cap.max_recv_wr = static_cast<uint32_t>(kRecvDepth * 4);
	qp_attr.cap.max_send_sge = 1;
	qp_attr.cap.max_recv_sge = 1;

	if (rdma_create_qp(cm_id_, pd_, &qp_attr) != 0) {
		error = ErrnoMessage("rdma_create_qp failed");
		return false;
	}

	send_buffer_.assign(kMaxFrameBytes, 0);
	recv_buffers_.assign(kRecvDepth * kMaxFrameBytes, 0);
	recv_slot_posted_.assign(kRecvDepth, false);

	send_mr_ = ibv_reg_mr(pd_, send_buffer_.data(), send_buffer_.size(), IBV_ACCESS_LOCAL_WRITE);
	if (!send_mr_) {
		error = ErrnoMessage("ibv_reg_mr(send) failed");
		return false;
	}
	recv_mr_ = ibv_reg_mr(pd_, recv_buffers_.data(), recv_buffers_.size(), IBV_ACCESS_LOCAL_WRITE);
	if (!recv_mr_) {
		error = ErrnoMessage("ibv_reg_mr(recv) failed");
		return false;
	}
	return true;
}

void RdmaEndpoint::Shutdown() {
	if (send_mr_) {
		ibv_dereg_mr(send_mr_);
		send_mr_ = nullptr;
	}
	if (recv_mr_) {
		ibv_dereg_mr(recv_mr_);
		recv_mr_ = nullptr;
	}
	if (cm_id_ && cm_id_->qp) {
		rdma_destroy_qp(cm_id_);
	}
	if (send_cq_) {
		ibv_destroy_cq(send_cq_);
		send_cq_ = nullptr;
	}
	if (recv_cq_) {
		ibv_destroy_cq(recv_cq_);
		recv_cq_ = nullptr;
	}
	if (pd_) {
		ibv_dealloc_pd(pd_);
		pd_ = nullptr;
	}
	cm_id_ = nullptr;
	send_buffer_.clear();
	recv_buffers_.clear();
	recv_slot_posted_.clear();
}

bool RdmaEndpoint::PostReceiveSlot(uint64_t slot_index, std::string &error) {
	if (slot_index >= kRecvDepth) {
		error = "PostReceiveSlot index out of range";
		return false;
	}
	ibv_sge sge;
	std::memset(&sge, 0, sizeof(sge));
	sge.addr = reinterpret_cast<uint64_t>(recv_buffers_.data() + slot_index * kMaxFrameBytes);
	sge.length = static_cast<uint32_t>(kMaxFrameBytes);
	sge.lkey = recv_mr_->lkey;

	ibv_recv_wr wr;
	std::memset(&wr, 0, sizeof(wr));
	wr.wr_id = slot_index;
	wr.sg_list = &sge;
	wr.num_sge = 1;

	ibv_recv_wr *bad = nullptr;
	if (ibv_post_recv(cm_id_->qp, &wr, &bad) != 0) {
		error = ErrnoMessage("ibv_post_recv failed");
		return false;
	}
	recv_slot_posted_[slot_index] = true;
	return true;
}

bool RdmaEndpoint::PostInitialReceives(std::string &error) {
	if (!cm_id_ || !cm_id_->qp || !recv_mr_) {
		error = "PostInitialReceives called before Initialize";
		return false;
	}
	for (uint64_t slot = 0; slot < kRecvDepth; slot++) {
		if (!PostReceiveSlot(slot, error)) {
			return false;
		}
	}
	return true;
}

bool RdmaEndpoint::WaitSendCompletion(std::string &error) {
	while (true) {
		ibv_wc wc;
		std::memset(&wc, 0, sizeof(wc));
		const int polled = ibv_poll_cq(send_cq_, 1, &wc);
		if (polled < 0) {
			error = ErrnoMessage("ibv_poll_cq(send) failed");
			return false;
		}
		if (polled == 0) {
			// Busy-poll for lowest latency; do not yield to scheduler.
			continue;
		}
		if (wc.status != IBV_WC_SUCCESS) {
			std::ostringstream ss;
			ss << "send completion status=" << static_cast<int>(wc.status) << " wr_id=" << wc.wr_id;
			error = ss.str();
			return false;
		}
		if (wc.opcode != IBV_WC_SEND) {
			std::ostringstream ss;
			ss << "unexpected send CQ opcode=" << static_cast<int>(wc.opcode);
			error = ss.str();
			return false;
		}
		return true;
	}
}

bool RdmaEndpoint::WaitRecvCompletion(ibv_wc &wc, std::string &error) {
	while (true) {
		std::memset(&wc, 0, sizeof(wc));
		const int polled = ibv_poll_cq(recv_cq_, 1, &wc);
		if (polled < 0) {
			error = ErrnoMessage("ibv_poll_cq(recv) failed");
			return false;
		}
		if (polled == 0) {
			// Busy-poll for lowest latency; do not yield to scheduler.
			continue;
		}
		if (wc.status != IBV_WC_SUCCESS) {
			std::ostringstream ss;
			ss << "recv completion status=" << static_cast<int>(wc.status) << " wr_id=" << wc.wr_id;
			error = ss.str();
			return false;
		}
		if (wc.opcode != IBV_WC_RECV) {
			std::ostringstream ss;
			ss << "unexpected recv CQ opcode=" << static_cast<int>(wc.opcode);
			error = ss.str();
			return false;
		}
		return true;
	}
}

bool RdmaEndpoint::SendOneFrame(const FrameHeader &header, const char *payload, size_t payload_size, std::string &error) {
	if (payload_size > kMaxFramePayloadBytes) {
		error = "SendOneFrame payload exceeds max frame payload";
		return false;
	}
	const size_t frame_size = sizeof(FrameHeader) + payload_size;
	if (frame_size > send_buffer_.size()) {
		error = "SendOneFrame frame does not fit send buffer";
		return false;
	}

	// Serialize header+payload into the registered send buffer before posting SEND.
	std::memcpy(send_buffer_.data(), &header, sizeof(FrameHeader));
	if (payload_size > 0) {
		std::memcpy(send_buffer_.data() + sizeof(FrameHeader), payload, payload_size);
	}

	ibv_sge sge;
	std::memset(&sge, 0, sizeof(sge));
	sge.addr = reinterpret_cast<uint64_t>(send_buffer_.data());
	sge.length = static_cast<uint32_t>(frame_size);
	sge.lkey = send_mr_->lkey;

	ibv_send_wr wr;
	std::memset(&wr, 0, sizeof(wr));
	wr.wr_id = 1;
	wr.sg_list = &sge;
	wr.num_sge = 1;
	wr.opcode = IBV_WR_SEND;
	wr.send_flags = IBV_SEND_SIGNALED;

	ibv_send_wr *bad = nullptr;
	if (ibv_post_send(cm_id_->qp, &wr, &bad) != 0) {
		error = ErrnoMessage("ibv_post_send failed");
		return false;
	}
	return WaitSendCompletion(error);
}

bool RdmaEndpoint::SendMessage(MessageType message_type, uint32_t query_id, const char *payload, size_t payload_size,
                               uint64_t &send_elapsed_ns, std::string &error) {
	if (!cm_id_ || !cm_id_->qp || !send_mr_) {
		error = "SendMessage called before Initialize";
		return false;
	}
	const size_t frame_count = std::max<size_t>(1, (payload_size + kMaxFramePayloadBytes - 1) / kMaxFramePayloadBytes);
	const uint64_t start_ns = NowNanos();
	for (size_t i = 0; i < frame_count; i++) {
		const size_t offset = i * kMaxFramePayloadBytes;
		const size_t chunk_size = std::min(kMaxFramePayloadBytes, payload_size - std::min(payload_size, offset));

		FrameHeader header;
		header.magic = kFrameMagic;
		header.version = kFrameVersion;
		header.message_type = static_cast<uint16_t>(message_type);
		header.query_id = query_id;
		header.frame_index = static_cast<uint32_t>(i);
		header.frame_count = static_cast<uint32_t>(frame_count);
		header.payload_size = chunk_size;
		header.total_payload_size = payload_size;

		const char *chunk_payload = payload_size > 0 ? payload + offset : nullptr;
		if (!SendOneFrame(header, chunk_payload, chunk_size, error)) {
			return false;
		}
	}
	send_elapsed_ns = NowNanos() - start_ns;
	return true;
}

bool RdmaEndpoint::ReceiveMessage(MessageType &message_type, uint32_t &query_id, std::string &payload, std::string &error) {
	if (!cm_id_ || !cm_id_->qp || !recv_mr_) {
		error = "ReceiveMessage called before Initialize";
		return false;
	}

	bool header_initialized = false;
	uint32_t expected_index = 0;
	uint32_t expected_frame_count = 0;
	uint64_t expected_total_payload = 0;
	payload.clear();

	while (true) {
		ibv_wc wc;
		if (!WaitRecvCompletion(wc, error)) {
			return false;
		}

		if (wc.wr_id >= kRecvDepth) {
			error = ValidationError("recv completion wr_id out of range");
			return false;
		}
		const uint64_t slot = wc.wr_id;
		recv_slot_posted_[slot] = false;

		if (wc.byte_len < sizeof(FrameHeader)) {
			error = ValidationError("frame shorter than header");
			return false;
		}
		const char *frame_ptr = recv_buffers_.data() + slot * kMaxFrameBytes;
		FrameHeader header;
		std::memcpy(&header, frame_ptr, sizeof(FrameHeader));

		if (header.magic != kFrameMagic) {
			error = ValidationError("invalid frame magic");
			return false;
		}
		if (header.version != kFrameVersion) {
			error = ValidationError("unsupported frame version");
			return false;
		}
		if (header.frame_count == 0) {
			error = ValidationError("frame_count is zero");
			return false;
		}
		if (header.frame_index >= header.frame_count) {
			error = ValidationError("frame_index out of range");
			return false;
		}
		if (header.payload_size > kMaxFramePayloadBytes) {
			error = ValidationError("payload_size exceeds max frame payload");
			return false;
		}
		if (sizeof(FrameHeader) + header.payload_size != wc.byte_len) {
			error = ValidationError("byte_len does not match header payload_size");
			return false;
		}

		if (!header_initialized) {
			header_initialized = true;
			message_type = static_cast<MessageType>(header.message_type);
			query_id = header.query_id;
			expected_index = 0;
			expected_frame_count = header.frame_count;
			expected_total_payload = header.total_payload_size;
			if (expected_total_payload > 0) {
				payload.reserve(static_cast<size_t>(expected_total_payload));
			}
		} else {
			if (header.message_type != static_cast<uint16_t>(message_type) || header.query_id != query_id ||
			    header.frame_count != expected_frame_count || header.total_payload_size != expected_total_payload) {
				error = ValidationError("frame metadata mismatch within same message");
				return false;
			}
		}

		if (header.frame_index != expected_index) {
			error = ValidationError("unexpected frame ordering");
			return false;
		}
		if (header.payload_size > 0) {
			payload.append(frame_ptr + sizeof(FrameHeader), frame_ptr + sizeof(FrameHeader) + header.payload_size);
		}
		expected_index++;

		// Re-arm this receive slot immediately so the peer keeps credits.
		if (!PostReceiveSlot(slot, error)) {
			return false;
		}

		if (expected_index == expected_frame_count) {
			if (payload.size() != expected_total_payload) {
				error = ValidationError("assembled payload size does not match header total_payload_size");
				return false;
			}
			return true;
		}
	}
}

size_t RdmaEndpoint::MaxFramePayloadBytes() const {
	return kMaxFramePayloadBytes;
}

} // namespace rdma_query
