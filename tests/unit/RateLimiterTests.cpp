#include "dwe/network/RateLimiter.h"

#include <cstdlib>
#include <iostream>
#include <string>
#include <chrono>
#include <cstdint>

using namespace dwe::network;
using std::chrono::milliseconds;
using std::chrono::seconds;

namespace {
int failures = 0;
void check(bool condition, const std::string& message) {
    if (!condition) {
        std::cerr << "FAIL: " << message << '\n';
        ++failures;
    }
}
const TimePoint t0{};
}

int main() {
    // ---- token bucket
    TokenBucket b({10.0, 5.0, true}, t0);  // 10 tokens/s, burst 5
    int granted = 0;
    for (int i = 0; i < 20; ++i) granted += b.try_consume(1, t0) ? 1 : 0;
    check(granted == 5, "a fresh bucket grants exactly its burst");
    check(!b.try_consume(1, t0 + milliseconds(99)), "no token yet after 99 ms at 10/s");
    check(b.try_consume(1, t0 + milliseconds(100)), "one token after 100 ms at 10/s");
    check(!b.try_consume(1, t0 + milliseconds(100)), "...and only one");
    granted = 0;
    for (int i = 0; i < 100; ++i) granted += b.try_consume(1, t0 + seconds(60)) ? 1 : 0;
    check(granted == 5, "a long idle period refills only up to the burst, not beyond");
    check(!b.try_consume(6, t0 + seconds(120)), "a cost larger than the burst can never be granted");
    check(b.try_consume(5, t0 + seconds(120)), "a cost equal to the burst can");
    // going backwards in time must not mint tokens
    TokenBucket back({10.0, 5.0, true}, t0 + seconds(10));
    for (int i = 0; i < 5; ++i) back.try_consume(1, t0 + seconds(10));
    check(!back.try_consume(1, t0), "an earlier timestamp does not refill the bucket");
    check(TokenBucket(TokenBucketConfig::unlimited(), t0).try_consume(1e12, t0), "unlimited bucket never refuses");

    // ---- sustained rate: over 10 s at 100/s exactly burst + rate*time are granted
    TokenBucket s({100.0, 20.0, true}, t0);
    int total = 0;
    for (int ms = 0; ms <= 10000; ++ms) {
        for (int k = 0; k < 5; ++k) total += s.try_consume(1, t0 + milliseconds(ms)) ? 1 : 0;  // offered 5000/s
    }
    check(total >= 1000 + 19 && total <= 1000 + 21, "sustained throughput is bounded by rate*time + burst (got " + std::to_string(total) + ")");

    // ---- per-endpoint limiter
    EndpointRateLimiter lim({10.0, 3.0, true}, 4);
    for (int i = 0; i < 3; ++i) check(lim.allow("a:1", 1, t0), "endpoint a within burst");
    check(!lim.allow("a:1", 1, t0), "endpoint a over burst is refused");
    check(lim.allow("b:2", 1, t0), "endpoint b has its own bucket");
    check(lim.tracked() == 2, "two endpoints tracked");

    // Table bound: fill to the cap with "busy" endpoints.
    EndpointRateLimiter capped({10.0, 3.0, true}, 4, milliseconds(100));
    for (int i = 0; i < 4; ++i) check(capped.allow("e" + std::to_string(i), 1, t0), "fill table");
    check(!capped.allow("new:1", 1, t0), "a new endpoint is rejected while the table is full of busy endpoints");
    check(capped.rejected_table_full() == 1, "table-full rejection counted");
    check(capped.allow("e0", 1, t0), "an already tracked endpoint is still served when the table is full");
    check(capped.tracked() == 4, "the table never exceeds its cap");
    // After the buckets refill (>=100 ms for 1 token), pruning frees space; no information was lost.
    check(capped.allow("new:2", 1, t0 + milliseconds(1000)), "after refill the full buckets are pruned and new endpoints fit");
    check(capped.tracked() <= 2, "pruned entries were removed (only recently active ones remain)");
    // Pruning is lossless: a pruned endpoint behaves exactly like a never-seen one (full burst).
    int g = 0;
    for (int i = 0; i < 10; ++i) g += capped.allow("e1", 1, t0 + milliseconds(1000)) ? 1 : 0;
    check(g == 3, "pruned endpoint gets its full burst again");

    // Spoofed-source flood: 100,000 distinct endpoints cannot grow the table past the cap.
    EndpointRateLimiter flood({10.0, 3.0, true}, 1000, milliseconds(100));
    std::uint64_t admitted = 0;
    for (int i = 0; i < 100000; ++i) admitted += flood.allow("spoof:" + std::to_string(i), 1, t0 + milliseconds(i / 1000)) ? 1u : 0u;
    check(flood.tracked() <= 1000, "table bounded under a spoofed flood");
    check(admitted < 100000 && flood.rejected_table_full() > 0, "flood is shed rather than admitted");

    check(EndpointRateLimiter(TokenBucketConfig::unlimited()).allow("x", 1e9, t0), "disabled limiter allows everything");

    if (failures == 0) std::cout << "All DWE rate limiter tests passed.\n";
    return failures == 0 ? EXIT_SUCCESS : EXIT_FAILURE;
}
