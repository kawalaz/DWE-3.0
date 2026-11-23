#include "dwe/network/RateLimiter.h"

#include <cstddef>
#include <string>

namespace dwe::network {

bool EndpointRateLimiter::allow(const std::string& key, double cost, TimePoint now) {
    if (!config_.enabled) return true;

    auto it = buckets_.find(key);
    if (it == buckets_.end()) {
        if (buckets_.size() >= max_endpoints_) {
            // Pruning is O(n); doing it for every packet while full would turn the defence into
            // the attack, so it is throttled.
            if (!pruned_once_ || now - last_prune_ >= prune_interval_) prune(now);
            if (buckets_.size() >= max_endpoints_) {
                ++rejected_table_full_;
                return false;
            }
        }
        it = buckets_.emplace(key, TokenBucket(config_, now)).first;
    }
    return it->second.try_consume(cost, now);
}

std::size_t EndpointRateLimiter::prune(TimePoint now) {
    pruned_once_ = true;
    last_prune_ = now;
    std::size_t removed = 0;
    for (auto it = buckets_.begin(); it != buckets_.end();) {
        if (it->second.is_full(now)) {
            it = buckets_.erase(it);
            ++removed;
        } else {
            ++it;
        }
    }
    return removed;
}

} // namespace dwe::network
