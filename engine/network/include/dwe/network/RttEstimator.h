#pragma once

#include "dwe/network/Time.h"

#include <algorithm>
#include <chrono>
#include <cstdint>

namespace dwe::network {

struct RttConfig {
    Duration initial_rto = std::chrono::milliseconds(250);  // before the first RTT sample
    Duration min_rto = std::chrono::milliseconds(100);
    Duration max_rto = std::chrono::seconds(2);
};

// Smoothed round-trip time and retransmission timeout, following RFC 6298
// (SRTT/RTTVAR with alpha=1/8, beta=1/4, RTO = SRTT + max(G, 4*RTTVAR)).
class RttEstimator {
public:
    explicit RttEstimator(RttConfig config = {}) : config_(config) {}

    void add_sample(Duration rtt) {
        const double r = std::max(0.0, std::chrono::duration<double>(rtt).count());
        if (!has_sample_) {
            srtt_ = r;
            rttvar_ = r / 2.0;
            has_sample_ = true;
        } else {
            const double err = srtt_ > r ? srtt_ - r : r - srtt_;
            rttvar_ = 0.75 * rttvar_ + 0.25 * err;
            srtt_ = 0.875 * srtt_ + 0.125 * r;
        }
        ++samples_;
    }

    // Retransmission timeout for a first transmission.
    Duration rto() const { return clamp(raw_rto()); }

    // Timeout for the next retransmission after `retries` previous retransmissions
    // (exponential backoff, capped at max_rto).
    Duration backoff_rto(std::uint32_t retries) const {
        double seconds = std::chrono::duration<double>(raw_rto()).count();
        const std::uint32_t doublings = std::min<std::uint32_t>(retries, 20);
        for (std::uint32_t i = 0; i < doublings; ++i) seconds *= 2.0;
        return clamp(std::chrono::duration_cast<Duration>(std::chrono::duration<double>(seconds)));
    }

    bool has_sample() const { return has_sample_; }
    std::uint64_t samples() const { return samples_; }
    Duration srtt() const { return std::chrono::duration_cast<Duration>(std::chrono::duration<double>(srtt_)); }
    Duration rttvar() const { return std::chrono::duration_cast<Duration>(std::chrono::duration<double>(rttvar_)); }
    const RttConfig& config() const { return config_; }

private:
    Duration raw_rto() const {
        if (!has_sample_) return config_.initial_rto;
        const double granularity = 0.001;  // 1 ms clock granularity (the "G" in RFC 6298)
        const double seconds = srtt_ + std::max(granularity, 4.0 * rttvar_);
        return std::chrono::duration_cast<Duration>(std::chrono::duration<double>(seconds));
    }
    Duration clamp(Duration d) const { return std::clamp(d, config_.min_rto, config_.max_rto); }

    RttConfig config_;
    bool has_sample_{false};
    double srtt_{0.0};
    double rttvar_{0.0};
    std::uint64_t samples_{0};
};

} // namespace dwe::network
