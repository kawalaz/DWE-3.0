#pragma once

#include "dwe/network/RttEstimator.h"
#include "dwe/network/Time.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <optional>
#include <span>
#include <vector>

namespace dwe::network {

// Both sender and receiver use this window. It is a wire-level constant: the Ack message carries
// a 64-bit bitmap (bit i acknowledges next_expected + 1 + i), so at most 64 consecutive sequence
// numbers can be outstanding.
inline constexpr std::size_t kReliableWindow = 64;

struct ReliableConfig {
    std::size_t max_backlog = 256;             // messages waiting for window space (send backpressure)
    // Consecutive retransmissions of a message WITHOUT any acknowledgement progress after which the
    // channel is declared failed. Any new acknowledgement resets the count (as TCP does with its
    // retransmission timeouts), so a link that is lossy but alive is never abandoned.
    std::uint32_t max_retransmits = 10;
    std::uint32_t fast_retransmit_threshold = 3;  // later messages acked "around" a gap before resending it
    std::size_t max_reorder_bytes = 32 * 1024; // receiver reorder buffer cap (bounds memory per channel)
    // First sequence number on the wire. The protocol fixes this to 1 and both peers must agree;
    // it is configurable only so tests can start near 2^32 and exercise the wraparound.
    std::uint32_t initial_sequence = 1;
};

struct ReliableStats {
    std::uint64_t first_transmissions = 0;
    std::uint64_t retransmissions = 0;       // includes fast retransmissions
    std::uint64_t fast_retransmissions = 0;
    std::uint64_t acked = 0;                 // messages confirmed by the peer
    std::uint64_t delivered = 0;             // messages handed to the application, in order
    std::uint64_t duplicates = 0;            // already delivered/buffered; dropped (and re-acked)
    std::uint64_t out_of_window = 0;         // beyond the receive window; dropped
    std::uint64_t buffer_full = 0;           // reorder buffer byte cap hit; dropped (peer will retransmit)
    std::uint64_t invalid_acks = 0;          // acknowledged something that was never sent
    std::uint64_t stale_acks = 0;            // acknowledged only what was already acknowledged
};

// One direction-pair of a reliable-ordered channel: the send side (sequence assignment, window,
// retransmission) and the receive side (duplicate detection, reordering, ack generation).
// Pure state machine: no sockets, no system clock.
class ReliableChannel {
public:
    explicit ReliableChannel(ReliableConfig config = {})
        : config_(config), next_sequence_(config.initial_sequence), next_expected_(config.initial_sequence) {}

    // ------------------------------------------------------------------ send side
    struct Transmission {
        std::uint32_t sequence;
        const std::vector<std::uint8_t>* data;  // valid until the next non-const call
        bool retransmit;
    };

    // Queues a message. Returns false when the backlog is full (caller should back off).
    bool enqueue(std::vector<std::uint8_t> data);

    // Appends every transmission that is due at `now`: first sends (as window space allows) and
    // retransmissions (timeout with exponential backoff, or fast retransmit). Sets failed() when a
    // message exceeded max_retransmits.
    void collect(TimePoint now, const RttEstimator& rtt, std::vector<Transmission>& out);

    struct AckResult {
        bool valid{false};      // false: acknowledged unsent data (ignored)
        std::size_t newly_acked{0};
    };
    AckResult on_ack(std::uint32_t next_expected, std::uint64_t received_bits, TimePoint now,
                     RttEstimator& rtt);

    bool failed() const { return failed_; }
    std::size_t in_flight() const { return inflight_.size(); }
    std::size_t backlog() const { return backlog_.size(); }
    bool send_idle() const { return inflight_.empty() && backlog_.empty(); }

    // ------------------------------------------------------------------ receive side
    enum class Receive { Accepted, Duplicate, OutOfWindow, BufferFull };

    // Processes one received message. Messages that became deliverable (this one and any that were
    // waiting behind it) are appended to `delivered`, in order.
    Receive receive(std::uint32_t sequence, std::span<const std::uint8_t> data,
                    std::vector<std::vector<std::uint8_t>>& delivered);

    bool ack_pending() const { return ack_pending_; }
    std::uint32_t next_expected() const { return next_expected_; }
    std::uint64_t received_bits() const;
    void ack_sent() { ack_pending_ = false; }

    const ReliableStats& stats() const { return stats_; }

private:
    struct Slot {
        std::uint32_t sequence{0};
        std::vector<std::uint8_t> data;
        bool sent{false};
        bool acked{false};
        bool retransmitted{false};  // Karn: its RTT sample would be ambiguous
        std::uint32_t retries{0};   // retransmissions since the last acknowledgement progress
        std::uint32_t skipped{0};  // times later messages were acked while this one was not
        TimePoint first_sent{};
        TimePoint deadline{};
    };
    using Ring = std::array<std::optional<std::vector<std::uint8_t>>, kReliableWindow>;

    std::uint32_t send_base() const {
        return inflight_.empty() ? next_sequence_ : inflight_.front().sequence;
    }
    void drain_ring(std::vector<std::vector<std::uint8_t>>& delivered);

    ReliableConfig config_;
    ReliableStats stats_;

    // send side
    std::uint32_t next_sequence_;     // sequence of the next message to enter the window
    std::deque<Slot> inflight_;       // contiguous sequences starting at send_base()
    std::deque<std::vector<std::uint8_t>> backlog_;
    bool failed_{false};

    // receive side
    std::uint32_t next_expected_;
    std::unique_ptr<Ring> ring_;      // allocated lazily: most sessions never reorder
    std::size_t buffered_bytes_{0};
    bool ack_pending_{false};
};

} // namespace dwe::network
