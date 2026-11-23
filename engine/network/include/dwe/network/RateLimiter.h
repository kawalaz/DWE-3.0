#pragma once

#include "dwe/network/Time.h"

#include <cstddef>
#include <cstdint>
#include <string>
#include <unordered_map>
#include <chrono>

namespace dwe::network {

struct TokenBucketConfig {
    double rate_per_second = 0.0;  // sustained refill rate
    double burst = 0.0;            // bucket capacity; must be >= the largest single cost
    bool enabled = true;           // false: never limits

    static TokenBucketConfig unlimited() { return {0.0, 0.0, false}; }
};

// Classic token bucket. Refill is computed lazily from the elapsed time, so there is no timer.
class TokenBucket {
public:
    TokenBucket() = default;
    TokenBucket(const TokenBucketConfig& config, TimePoint now)
        : config_(config), tokens_(config.burst), last_(now) {}

    // Takes `cost` tokens if available (all or nothing).
    bool try_consume(double cost, TimePoint now) {
        refill(now);
        if (!config_.enabled) return true;
        if (tokens_ < cost) return false;
        tokens_ -= cost;
        return true;
    }
    // A full bucket is indistinguishable from a bucket that was never created.
    bool is_full(TimePoint now) {
        refill(now);
        return tokens_ >= config_.burst;
    }
    double tokens() const { return tokens_; }

private:
    void refill(TimePoint now) {
        if (now <= last_) return;  // never move backwards
        const double elapsed = std::chrono::duration<double>(now - last_).count();
        tokens_ += elapsed * config_.rate_per_second;
        if (tokens_ > config_.burst) tokens_ = config_.burst;
        last_ = now;
    }

    TokenBucketConfig config_{};
    double tokens_{0.0};
    TimePoint last_{};
};

// One token bucket per remote endpoint, with a hard cap on the number of tracked endpoints.
//
// The cap matters: source addresses on UDP are trivially spoofed, so an attacker can present
// millions of distinct "endpoints". Entries whose bucket has refilled completely carry no
// information and are pruned; if the table is still full after pruning, new endpoints are rejected
// (fail closed) while endpoints that are already tracked keep being served.
class EndpointRateLimiter {
public:
    explicit EndpointRateLimiter(TokenBucketConfig config, std::size_t max_endpoints = 65536,
                                 Duration prune_interval = std::chrono::milliseconds(100))
        : config_(config), max_endpoints_(max_endpoints), prune_interval_(prune_interval) {}

    bool allow(const std::string& key, double cost, TimePoint now);
    // Drops entries that have refilled to capacity. Returns how many were removed.
    std::size_t prune(TimePoint now);

    std::size_t tracked() const { return buckets_.size(); }
    std::uint64_t rejected_table_full() const { return rejected_table_full_; }
    const TokenBucketConfig& config() const { return config_; }

private:
    TokenBucketConfig config_;
    std::size_t max_endpoints_;
    Duration prune_interval_;
    std::unordered_map<std::string, TokenBucket> buckets_;
    TimePoint last_prune_{};
    bool pruned_once_{false};
    std::uint64_t rejected_table_full_{0};
};

} // namespace dwe::network
