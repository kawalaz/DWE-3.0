#include "dwe/network/ReliableChannel.h"

#include "dwe/core/Sequence.h"

#include <utility>
#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>

namespace dwe::network {

using dwe::core::seq_after;
using dwe::core::seq_before;

// ---------------------------------------------------------------------------------- send side

bool ReliableChannel::enqueue(std::vector<std::uint8_t> data) {
    if (failed_ || backlog_.size() >= config_.max_backlog) return false;
    backlog_.push_back(std::move(data));
    return true;
}

void ReliableChannel::collect(TimePoint now, const RttEstimator& rtt, std::vector<Transmission>& out) {
    if (failed_) return;

    // Admit queued messages while the window has room. Sequence numbers are assigned here, so they
    // are always contiguous with the messages already in flight.
    while (!backlog_.empty() && inflight_.size() < kReliableWindow) {
        Slot slot;
        slot.sequence = next_sequence_++;
        slot.data = std::move(backlog_.front());
        backlog_.pop_front();
        inflight_.push_back(std::move(slot));
    }

    for (auto& slot : inflight_) {
        if (slot.acked) continue;
        if (!slot.sent) {
            slot.sent = true;
            slot.first_sent = now;
            slot.deadline = now + rtt.backoff_rto(0);
            ++stats_.first_transmissions;
            out.push_back({slot.sequence, &slot.data, false});
            continue;
        }
        const bool timed_out = now >= slot.deadline;
        const bool fast = slot.skipped >= config_.fast_retransmit_threshold;
        if (!timed_out && !fast) continue;
        if (slot.retries >= config_.max_retransmits) {
            failed_ = true;  // the peer is not acknowledging: the ordered stream cannot be repaired
            return;
        }
        ++slot.retries;
        slot.retransmitted = true;
        slot.skipped = 0;
        slot.deadline = now + rtt.backoff_rto(slot.retries);
        ++stats_.retransmissions;
        if (fast && !timed_out) ++stats_.fast_retransmissions;
        out.push_back({slot.sequence, &slot.data, true});
    }
}

ReliableChannel::AckResult ReliableChannel::on_ack(std::uint32_t next_expected, std::uint64_t received_bits,
                                                   TimePoint now, RttEstimator& rtt) {
    // Valid cumulative acks lie in [send_base, next_sequence_]: anything below is an old ack,
    // anything above acknowledges messages that were never sent.
    if (seq_before(next_expected, send_base())) {
        ++stats_.stale_acks;
        return {true, 0};
    }
    if (seq_after(next_expected, next_sequence_)) {
        ++stats_.invalid_acks;
        return {false, 0};
    }

    std::size_t newly = 0;
    while (!inflight_.empty() && seq_before(inflight_.front().sequence, next_expected)) {
        const Slot& slot = inflight_.front();
        if (!slot.acked) {
            ++newly;
            // Karn's algorithm: a retransmitted message gives an ambiguous RTT sample.
            if (!slot.retransmitted) rtt.add_sample(now - slot.first_sent);
        }
        inflight_.pop_front();
    }

    // Selective acks: bit i acknowledges next_expected + 1 + i. After the pops above the front of
    // the window is exactly next_expected (the message the receiver is still waiting for), so
    // bit i maps to window index i + 1.
    bool newly_sacked = false;
    std::size_t highest_sacked = 0;
    bool valid = true;
    for (std::size_t i = 0; i < 64; ++i) {
        if (!(received_bits & (std::uint64_t{1} << i))) continue;
        const std::size_t index = i + 1;
        if (index >= inflight_.size()) {
            ++stats_.invalid_acks;  // acknowledges a sequence that was never sent
            valid = false;
            break;
        }
        Slot& slot = inflight_[index];
        if (!slot.acked) {
            slot.acked = true;
            ++newly;
            newly_sacked = true;
            if (!slot.retransmitted) rtt.add_sample(now - slot.first_sent);
        }
        highest_sacked = index;
    }

    // A fresh selective ack proves that later messages arrived while earlier ones are still
    // missing: count it towards fast retransmit of the gap.
    if (newly_sacked) {
        for (std::size_t i = 0; i < highest_sacked; ++i) {
            if (!inflight_[i].acked) ++inflight_[i].skipped;
        }
    }
    stats_.acked += newly;
    if (newly > 0) {
        // Progress: the peer is alive and receiving. Restart the backoff / failure budget of
        // whatever is still outstanding.
        for (auto& slot : inflight_) slot.retries = 0;
    }
    return {valid, newly};
}

// -------------------------------------------------------------------------------- receive side

ReliableChannel::Receive ReliableChannel::receive(std::uint32_t sequence, std::span<const std::uint8_t> data,
                                                  std::vector<std::vector<std::uint8_t>>& delivered) {
    // Already delivered: the peer is retransmitting because our ack was lost or late. Re-ack.
    if (seq_before(sequence, next_expected_)) {
        ++stats_.duplicates;
        ack_pending_ = true;
        return Receive::Duplicate;
    }
    const std::uint32_t offset = sequence - next_expected_;
    if (offset >= kReliableWindow) {
        ++stats_.out_of_window;
        return Receive::OutOfWindow;
    }

    if (offset == 0) {
        delivered.emplace_back(data.begin(), data.end());
        ++next_expected_;
        ++stats_.delivered;
        drain_ring(delivered);
        ack_pending_ = true;
        return Receive::Accepted;
    }

    const std::size_t slot = sequence % kReliableWindow;
    if (ring_ && (*ring_)[slot].has_value()) {
        ++stats_.duplicates;
        ack_pending_ = true;
        return Receive::Duplicate;
    }
    if (buffered_bytes_ + data.size() > config_.max_reorder_bytes) {
        ++stats_.buffer_full;  // not acked: the sender will retransmit once there is room
        return Receive::BufferFull;
    }
    if (!ring_) ring_ = std::make_unique<Ring>();
    (*ring_)[slot].emplace(data.begin(), data.end());
    buffered_bytes_ += data.size();
    ack_pending_ = true;
    return Receive::Accepted;
}

void ReliableChannel::drain_ring(std::vector<std::vector<std::uint8_t>>& delivered) {
    while (ring_) {
        auto& slot = (*ring_)[next_expected_ % kReliableWindow];
        if (!slot.has_value()) break;
        buffered_bytes_ -= slot->size();
        delivered.push_back(std::move(*slot));
        slot.reset();
        ++next_expected_;
        ++stats_.delivered;
    }
}

std::uint64_t ReliableChannel::received_bits() const {
    if (!ring_) return 0;
    std::uint64_t bits = 0;
    for (std::uint32_t i = 0; i < 63; ++i) {
        // Bit i <=> next_expected + 1 + i is buffered. (next_expected itself is never buffered.)
        if ((*ring_)[(next_expected_ + 1 + i) % kReliableWindow].has_value()) bits |= std::uint64_t{1} << i;
    }
    return bits;
}

} // namespace dwe::network
