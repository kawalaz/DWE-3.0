#pragma once

#include "dwe/core/Packet.h"
#include "dwe/network/ClientConnection.h"
#include "dwe/network/Time.h"

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <vector>
#include <cstddef>

namespace dwe::network {

struct WorkloadConfig {
    double reliable_hz = 5.0;     // messages per second on EACH reliable channel
    double input_hz = 20.0;       // Input (unreliable, deduplicated)
    double snapshot_hz = 10.0;    // Snapshot (unreliable, newest wins)
    std::size_t payload_bytes = 32;  // >= 12 (counter + timestamp), at most kMaxChannelDataSize
    // Stop sending after this many messages per channel (0 = unlimited). Useful to let a test drain.
    std::uint32_t max_per_channel = 0;
};

struct ChannelCheck {
    std::uint64_t sent = 0;
    std::uint64_t echoed = 0;
    std::uint64_t out_of_order = 0;  // reliable: a message jumped ahead of an earlier one (gap)
    std::uint64_t duplicates = 0;    // the same message delivered more than once
    std::uint64_t regressions = 0;   // Snapshot: an older message delivered after a newer one
    std::uint64_t send_rejected = 0; // QueueFull etc.
};

// Sends sequence-numbered, timestamped messages on every channel of a ClientConnection and checks
// what the echo server sends back against the guarantee of each channel:
//   ReliableOrdered      -> every message returns exactly once and in order
//   Unreliable (Input)   -> no message returns twice (loss is allowed)
//   UnreliableSequenced  -> returned messages are strictly increasing (loss is allowed)
// The same check is used by the integration tests and by the load generator.
class EchoWorkload {
public:
    EchoWorkload(ClientConnection& client, WorkloadConfig config) : client_(client), config_(config) {
        if (config_.payload_bytes < 12) config_.payload_bytes = 12;
        if (config_.payload_bytes > dwe::core::kMaxChannelDataSize) config_.payload_bytes = dwe::core::kMaxChannelDataSize;
    }

    // Call regularly with the current time: emits whatever is due and verifies received echoes.
    // The caller still calls client.update() to run the network side.
    void tick(TimePoint now) {
        if (!started_) { started_ = true; last_tick_ = now; }
        const double elapsed = std::chrono::duration<double>(now - last_tick_).count();
        last_tick_ = now;
        if (client_.connected() && sending_) {
            for (std::size_t i = 0; i < dwe::core::kChannelCount; ++i) {
                const auto channel = static_cast<dwe::core::ChannelId>(i);
                const double hz = rate_for(channel);
                if (hz <= 0.0) continue;
                credit_[i] += elapsed * hz;
                while (credit_[i] >= 1.0) {
                    credit_[i] -= 1.0;
                    if (config_.max_per_channel != 0 && checks_[i].sent >= config_.max_per_channel) { credit_[i] = 0; break; }
                    send_one(channel, now);
                }
            }
        }
        receive(now);
    }

    // Stops generating traffic (echoes still being received are verified).
    void stop_sending() { sending_ = false; }

    void receive(TimePoint now) {
        scratch_.clear();
        client_.drain_delivered(scratch_);
        for (const auto& message : scratch_) check_echo(message, now);
    }

    const ChannelCheck& channel(dwe::core::ChannelId id) const { return checks_[static_cast<std::size_t>(id)]; }

    // Everything reliable that was sent came back.
    bool reliable_complete() const {
        for (std::size_t i = 0; i < dwe::core::kChannelCount; ++i) {
            if (dwe::core::is_reliable(static_cast<dwe::core::ChannelId>(i)) && checks_[i].echoed != checks_[i].sent) return false;
        }
        return true;
    }
    // Sum of violations of the reliability guarantees. Must be zero in every run.
    std::uint64_t violations() const {
        std::uint64_t v = 0;
        for (std::size_t i = 0; i < dwe::core::kChannelCount; ++i) {
            v += checks_[i].duplicates + checks_[i].out_of_order + checks_[i].regressions;
        }
        return v;
    }
    std::uint64_t reliable_sent() const { return sum(true, &ChannelCheck::sent); }
    std::uint64_t reliable_echoed() const { return sum(true, &ChannelCheck::echoed); }
    std::uint64_t unreliable_sent() const { return sum(false, &ChannelCheck::sent); }
    std::uint64_t unreliable_echoed() const { return sum(false, &ChannelCheck::echoed); }

    // Round-trip times in milliseconds, separately for reliable and unreliable echoes.
    const std::vector<float>& reliable_rtt_ms() const { return reliable_rtt_; }
    const std::vector<float>& unreliable_rtt_ms() const { return unreliable_rtt_; }

private:
    double rate_for(dwe::core::ChannelId c) const {
        switch (dwe::core::channel_delivery(c)) {
            case dwe::core::ChannelDelivery::ReliableOrdered: return config_.reliable_hz;
            case dwe::core::ChannelDelivery::Unreliable: return config_.input_hz;
            case dwe::core::ChannelDelivery::UnreliableSequenced: return config_.snapshot_hz;
        }
        return 0.0;
    }
    std::uint64_t sum(bool reliable, std::uint64_t ChannelCheck::*field) const {
        std::uint64_t total = 0;
        for (std::size_t i = 0; i < dwe::core::kChannelCount; ++i) {
            if (dwe::core::is_reliable(static_cast<dwe::core::ChannelId>(i)) == reliable) total += checks_[i].*field;
        }
        return total;
    }

    void send_one(dwe::core::ChannelId channel, TimePoint now) {
        const auto i = static_cast<std::size_t>(channel);
        std::vector<std::uint8_t> payload(config_.payload_bytes, 0);
        const std::uint32_t counter = next_counter_[i];
        for (int b = 0; b < 4; ++b) payload[static_cast<std::size_t>(b)] = static_cast<std::uint8_t>(counter >> (24 - 8 * b));
        const auto ns = static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch()).count());
        for (int b = 0; b < 8; ++b) payload[static_cast<std::size_t>(4 + b)] = static_cast<std::uint8_t>(ns >> (56 - 8 * b));
        if (client_.send(channel, payload) == SendStatus::Ok) {
            ++next_counter_[i];
            ++checks_[i].sent;
        } else {
            ++checks_[i].send_rejected;  // back-pressure: the counter is not consumed, so order is kept
        }
    }

    void check_echo(const dwe::core::ChannelMessage& message, TimePoint now) {
        if (message.data.size() < 12) return;
        const auto i = static_cast<std::size_t>(message.channel);
        auto& check = checks_[i];
        std::uint32_t counter = 0;
        for (int b = 0; b < 4; ++b) counter = (counter << 8) | message.data[static_cast<std::size_t>(b)];
        std::uint64_t ns = 0;
        for (int b = 0; b < 8; ++b) ns = (ns << 8) | message.data[static_cast<std::size_t>(4 + b)];
        const double rtt_ms = std::chrono::duration<double, std::milli>(
            now - TimePoint(std::chrono::duration_cast<Duration>(std::chrono::nanoseconds(ns)))).count();

        ++check.echoed;
        switch (dwe::core::channel_delivery(message.channel)) {
            case dwe::core::ChannelDelivery::ReliableOrdered:
                if (counter == expected_[i]) ++expected_[i];
                else if (counter < expected_[i]) ++check.duplicates;
                else { ++check.out_of_order; expected_[i] = counter + 1; }
                reliable_rtt_.push_back(static_cast<float>(rtt_ms));
                break;
            case dwe::core::ChannelDelivery::Unreliable:
                if (counter >= seen_.size()) seen_.resize(static_cast<std::size_t>(counter) + 1, false);
                if (seen_[counter]) ++check.duplicates;
                seen_[counter] = true;
                unreliable_rtt_.push_back(static_cast<float>(rtt_ms));
                break;
            case dwe::core::ChannelDelivery::UnreliableSequenced:
                if (have_snapshot_ && counter <= last_snapshot_) ++check.regressions;
                else { have_snapshot_ = true; last_snapshot_ = counter; }
                unreliable_rtt_.push_back(static_cast<float>(rtt_ms));
                break;
        }
    }

    ClientConnection& client_;
    WorkloadConfig config_;
    bool started_{false};
    bool sending_{true};
    TimePoint last_tick_{};
    std::array<double, dwe::core::kChannelCount> credit_{};
    std::array<std::uint32_t, dwe::core::kChannelCount> next_counter_{};
    std::array<std::uint32_t, dwe::core::kChannelCount> expected_{};
    std::array<ChannelCheck, dwe::core::kChannelCount> checks_{};
    std::vector<bool> seen_;          // Input duplicates
    bool have_snapshot_{false};
    std::uint32_t last_snapshot_{0};
    std::vector<float> reliable_rtt_, unreliable_rtt_;
    std::vector<dwe::core::ChannelMessage> scratch_;
};

} // namespace dwe::network
