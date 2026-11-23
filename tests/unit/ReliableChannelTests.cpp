// Unit and randomized tests for the reliable-ordered channel state machine.
#include "dwe/core/Sequence.h"
#include "dwe/network/ReliableChannel.h"

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <map>
#include <string>
#include <vector>
#include <chrono>
#include <cstddef>
#include <utility>

using namespace dwe::network;
using std::chrono::milliseconds;

namespace {
int failures = 0;
void check(bool condition, const std::string& message) {
    if (!condition) {
        std::cerr << "FAIL: " << message << '\n';
        ++failures;
    }
}

std::vector<std::uint8_t> bytes(std::uint32_t value) {
    return {static_cast<std::uint8_t>(value >> 24), static_cast<std::uint8_t>(value >> 16),
            static_cast<std::uint8_t>(value >> 8), static_cast<std::uint8_t>(value)};
}
std::uint32_t value_of(const std::vector<std::uint8_t>& b) {
    return (std::uint32_t{b[0]} << 24) | (std::uint32_t{b[1]} << 16) | (std::uint32_t{b[2]} << 8) | b[3];
}

// Small deterministic generator (identical on every platform).
struct Rng {
    std::uint64_t state;
    explicit Rng(std::uint64_t seed) : state(seed * 2654435761u + 88172645463325252ULL) {}
    std::uint64_t next() {
        state ^= state << 13; state ^= state >> 7; state ^= state << 17;
        return state;
    }
    double uniform() { return static_cast<double>(next() >> 11) * (1.0 / 9007199254740992.0); }
    std::uint64_t below(std::uint64_t n) { return next() % n; }
};

const TimePoint t0{};

std::vector<ReliableChannel::Transmission> collect(ReliableChannel& c, TimePoint now, const RttEstimator& rtt) {
    std::vector<ReliableChannel::Transmission> out;
    c.collect(now, rtt, out);
    return out;
}

// ------------------------------------------------------------------------------------------------
void test_basic_send_and_ack() {
    ReliableChannel c;
    RttEstimator rtt;
    for (std::uint32_t i = 0; i < 3; ++i) check(c.enqueue(bytes(i)), "enqueue");
    auto tx = collect(c, t0, rtt);
    check(tx.size() == 3, "three first transmissions");
    check(tx[0].sequence == 1 && tx[1].sequence == 2 && tx[2].sequence == 3, "sequences start at 1 and are contiguous");
    check(!tx[0].retransmit, "first send is not a retransmit");
    check(collect(c, t0, rtt).empty(), "nothing more is due at the same instant");
    check(c.in_flight() == 3, "three in flight");

    auto r = c.on_ack(3, 0, t0 + milliseconds(40), rtt);  // acks 1 and 2
    check(r.valid && r.newly_acked == 2, "cumulative ack frees two messages");
    check(c.in_flight() == 1, "one still in flight");
    check(rtt.has_sample(), "RTT sampled from an unretransmitted message");
    r = c.on_ack(4, 0, t0 + milliseconds(50), rtt);
    check(r.valid && r.newly_acked == 1 && c.send_idle(), "final ack drains the channel");
    check(c.stats().acked == 3, "acked counter");
}

void test_window_and_backlog() {
    ReliableConfig cfg;
    cfg.max_backlog = 100;
    ReliableChannel c(cfg);
    RttEstimator rtt;
    int accepted = 0;
    for (int i = 0; i < 200; ++i) accepted += c.enqueue(bytes(static_cast<std::uint32_t>(i))) ? 1 : 0;
    check(accepted == 100, "enqueue refuses beyond max_backlog (backpressure)");
    auto tx = collect(c, t0, rtt);
    check(tx.size() == kReliableWindow, "at most kReliableWindow messages in flight");
    check(c.backlog() == 100 - kReliableWindow, "the rest waits in the backlog");
    check(c.enqueue(bytes(0)), "window admission freed backlog space");

    // Larger scenario: window slides as acks arrive.
    ReliableConfig big;
    big.max_backlog = 1000;
    ReliableChannel d(big);
    for (int i = 0; i < 150; ++i) d.enqueue(bytes(static_cast<std::uint32_t>(i)));
    tx = collect(d, t0, rtt);
    check(tx.size() == 64 && d.backlog() == 86, "64 sent, 86 waiting");
    d.on_ack(11, 0, t0 + milliseconds(10), rtt);  // acks 1..10
    tx = collect(d, t0 + milliseconds(10), rtt);
    check(tx.size() == 10 && tx.front().sequence == 65 && tx.back().sequence == 74, "window slides by exactly the acked amount");
}

void test_timeout_backoff_and_failure() {
    ReliableConfig cfg;
    cfg.max_retransmits = 3;
    ReliableChannel c(cfg);
    RttEstimator rtt;  // initial RTO 250 ms, min 100 ms, max 2 s
    c.enqueue(bytes(1));
    check(collect(c, t0, rtt).size() == 1, "first send");
    check(collect(c, t0 + milliseconds(249), rtt).empty(), "no retransmit before the RTO");
    auto tx = collect(c, t0 + milliseconds(250), rtt);
    check(tx.size() == 1 && tx[0].retransmit && tx[0].sequence == 1, "retransmit at the RTO");
    check(collect(c, t0 + milliseconds(250 + 499), rtt).empty(), "backoff: second timeout is 2x");
    check(collect(c, t0 + milliseconds(250 + 500), rtt).size() == 1, "second retransmit after 500 ms");
    check(collect(c, t0 + milliseconds(750 + 999), rtt).empty(), "backoff: third timeout is 4x");
    check(collect(c, t0 + milliseconds(750 + 1000), rtt).size() == 1, "third retransmit after 1000 ms");
    check(!c.failed(), "not failed while retries remain");
    check(collect(c, t0 + milliseconds(1750 + 2000), rtt).empty() && c.failed(), "exceeding max_retransmits fails the channel");
    check(!c.enqueue(bytes(2)), "a failed channel refuses new messages");
    check(c.stats().retransmissions == 3, "three retransmissions counted");

    // Karn's algorithm: a retransmitted message must not produce an RTT sample.
    ReliableChannel k;
    RttEstimator krtt;
    k.enqueue(bytes(1));
    collect(k, t0, krtt);
    collect(k, t0 + milliseconds(300), krtt);  // retransmit
    k.on_ack(2, 0, t0 + milliseconds(320), krtt);
    check(!krtt.has_sample(), "no RTT sample from a retransmitted message");
}

void test_progress_resets_the_failure_budget() {
    // A lossy-but-alive link must not be abandoned: the budget counts retransmissions WITHOUT progress.
    ReliableConfig cfg;
    cfg.max_retransmits = 3;
    ReliableChannel c(cfg);
    RttEstimator rtt;
    c.enqueue(bytes(1));
    c.enqueue(bytes(2));
    collect(c, t0, rtt);  // 1 and 2 in flight
    TimePoint now = t0;
    // Message 1 is lost over and over; every ~RTO the peer still acknowledges something new (here:
    // freshly sent later messages), proving it is alive. Far more than 3 retransmissions of message 1
    // must therefore be tolerated.
    std::uint32_t next_value = 3;
    bool failed = false;
    std::uint32_t retransmits_of_1 = 0;
    for (int round = 0; round < 12 && !failed; ++round) {
        now += std::chrono::seconds(3);
        for (const auto& t : collect(c, now, rtt)) retransmits_of_1 += (t.sequence == 1 && t.retransmit);
        failed = c.failed();
        // peer acknowledges a newly sent message 'around' the missing one 1 (selective ack of seq 2 first, then later ones)
        c.enqueue(bytes(next_value++));
        const auto fresh = collect(c, now, rtt);
        std::uint64_t bits = 0;
        for (std::size_t i = 0; i < c.in_flight() - 1 && i < 62; ++i) bits |= std::uint64_t{1} << i;  // everything after 1 is received
        c.on_ack(1, bits, now + milliseconds(10), rtt);
        (void)fresh;
    }
    check(!failed, "alive-but-lossy link: acknowledgement progress keeps the channel from failing");
    check(retransmits_of_1 > 3, "message 1 was retransmitted far more often than max_retransmits");

    // Without any progress the same budget does run out.
    ReliableChannel dead(cfg);
    dead.enqueue(bytes(1));
    collect(dead, t0, rtt);
    TimePoint t = t0;
    for (int i = 0; i < 10 && !dead.failed(); ++i) { t += std::chrono::seconds(3); collect(dead, t, rtt); }
    check(dead.failed(), "no progress at all: the channel still fails after max_retransmits");
}

void test_selective_ack_and_fast_retransmit() {
    ReliableChannel c;
    RttEstimator rtt;
    for (std::uint32_t i = 0; i < 6; ++i) c.enqueue(bytes(i));
    collect(c, t0, rtt);  // sequences 1..6 in flight

    // Receiver has 2 (bit 0), then 2,3 (bits 0,1), then 2,3,4 and is still waiting for 1.
    auto r = c.on_ack(1, 0b1, t0 + milliseconds(20), rtt);
    check(r.valid && r.newly_acked == 1, "selective ack of seq 2");
    r = c.on_ack(1, 0b1, t0 + milliseconds(21), rtt);
    check(r.valid && r.newly_acked == 0, "repeating the same ack carries no new information");
    check(collect(c, t0 + milliseconds(22), rtt).empty(), "no fast retransmit after one informative ack");
    c.on_ack(1, 0b11, t0 + milliseconds(23), rtt);
    check(collect(c, t0 + milliseconds(24), rtt).empty(), "no fast retransmit after two informative acks");
    c.on_ack(1, 0b111, t0 + milliseconds(25), rtt);
    auto tx = collect(c, t0 + milliseconds(26), rtt);
    check(tx.size() == 1 && tx[0].sequence == 1 && tx[0].retransmit, "fast retransmit of the gap only (seq 1)");
    check(c.stats().fast_retransmissions == 1, "fast retransmit counted");
    check(collect(c, t0 + milliseconds(27), rtt).empty(), "fast retransmit happens once, not every call");

    // Acked-by-bits messages are not resent on timeout, and the cumulative ack then frees them all.
    tx = collect(c, t0 + milliseconds(2000), rtt);
    for (const auto& t : tx) check(t.sequence == 1 || t.sequence >= 5, "selectively acked 2..4 are never retransmitted");
    r = c.on_ack(5, 0, t0 + milliseconds(2001), rtt);
    check(r.valid && r.newly_acked == 1, "cumulative ack frees seq 1 (2..4 were counted already)");
    check(c.in_flight() == 2, "5 and 6 remain");
}

void test_invalid_and_stale_acks() {
    ReliableChannel c;
    RttEstimator rtt;
    for (std::uint32_t i = 0; i < 3; ++i) c.enqueue(bytes(i));
    collect(c, t0, rtt);  // 1..3, next to assign = 4
    auto r = c.on_ack(9, 0, t0, rtt);
    check(!r.valid && c.in_flight() == 3, "ack of never-sent sequences is rejected and changes nothing");
    r = c.on_ack(1, 0b1000, t0, rtt);  // bit 3 = seq 5, never sent
    check(!r.valid, "selective ack of a never-sent sequence is rejected");
    r = c.on_ack(1, std::uint64_t{1} << 63, t0, rtt);
    check(!r.valid, "bit 63 can never be legitimate");
    check(c.stats().invalid_acks == 3, "invalid acks counted");
    c.on_ack(3, 0, t0 + milliseconds(10), rtt);
    r = c.on_ack(1, 0, t0 + milliseconds(11), rtt);  // old ack arriving late
    check(r.valid && r.newly_acked == 0 && c.stats().stale_acks == 1, "late old ack is ignored as stale");
    check(c.in_flight() == 1, "stale ack did not disturb the window");
    // An ack must not free more than was sent even when numerically "after" via wrap-around garbage.
    r = c.on_ack(0xF0000000u, 0, t0, rtt);
    check(!r.valid || c.in_flight() == 1, "garbage ack value does not corrupt the window");
}

void test_receiver_ordering_and_duplicates() {
    ReliableChannel c;
    std::vector<std::vector<std::uint8_t>> out;
    using R = ReliableChannel::Receive;
    check(c.receive(1, bytes(10), out) == R::Accepted && out.size() == 1, "in-order message delivered immediately");
    check(c.next_expected() == 2 && c.ack_pending(), "next_expected advances, ack pending");
    c.ack_sent();
    check(c.receive(1, bytes(10), out) == R::Duplicate && out.size() == 1, "duplicate is not delivered again");
    check(c.ack_pending(), "a duplicate triggers a re-ack (the first ack may have been lost)");
    c.ack_sent();

    // Out of order: 4 and 3 arrive before 2.
    check(c.receive(4, bytes(40), out) == R::Accepted && out.size() == 1, "message 4 buffered, not delivered");
    check(c.received_bits() == 0b10, "bit 1 (= next_expected + 2 = 4) set");
    check(c.receive(4, bytes(40), out) == R::Duplicate, "buffered duplicate detected");
    check(c.receive(3, bytes(30), out) == R::Accepted && out.size() == 1, "message 3 buffered");
    check(c.received_bits() == 0b11, "bits for 3 and 4");
    check(c.receive(2, bytes(20), out) == R::Accepted, "message 2 arrives");
    check(out.size() == 4, "2, 3, 4 released together");
    check(value_of(out[1]) == 20 && value_of(out[2]) == 30 && value_of(out[3]) == 40, "released strictly in order");
    check(c.next_expected() == 5 && c.received_bits() == 0, "state cleaned up after the gap closed");

    // Window limit.
    check(c.receive(5 + 64, bytes(1), out) == R::OutOfWindow, "beyond the window is rejected");
    check(c.receive(5 + 63, bytes(1), out) == R::Accepted, "last slot of the window is accepted");
    c.ack_sent();
    check(c.receive(5 + 64, bytes(1), out) == R::OutOfWindow && !c.ack_pending(), "out-of-window is not acked");

    // Byte cap on the reorder buffer.
    ReliableConfig cfg;
    cfg.max_reorder_bytes = 10;
    ReliableChannel capped(cfg);
    std::vector<std::vector<std::uint8_t>> sink;
    check(capped.receive(3, std::vector<std::uint8_t>(8, 1), sink) == R::Accepted, "8 buffered bytes fit");
    check(capped.receive(4, std::vector<std::uint8_t>(8, 1), sink) == R::BufferFull, "cap rejects the next out-of-order message");
    check(capped.receive(1, std::vector<std::uint8_t>(100, 1), sink) == R::Accepted && sink.size() == 1,
          "the next expected message is always accepted (never needs buffering)");
}

// ------------------------------------------------------------------------------------------------
// Randomized end-to-end: a sender and a receiver connected through a network that loses, duplicates,
// delays and reorders both data and acknowledgements. Verdict: every message delivered exactly once,
// in order, whatever the seed; also across the 32-bit sequence wrap.
struct Event {
    TimePoint at;
    bool is_ack;
    std::uint32_t seq{0};
    std::vector<std::uint8_t> data;
    std::uint32_t next_expected{0};
    std::uint64_t bits{0};
};

struct SimResult {
    bool ok{false};
    std::string why;
    TimePoint finished{};
    ReliableStats sender, receiver;
};

SimResult run_pair(std::uint64_t seed, double loss, double dup, double reorder, std::uint32_t messages,
                   std::uint32_t initial_sequence) {
    Rng rng(seed);
    ReliableConfig cfg;
    cfg.max_retransmits = 60;  // this test is about correctness of repair, not about giving up
    cfg.initial_sequence = initial_sequence;
    ReliableChannel sender(cfg), receiver(cfg);
    RttEstimator rtt;
    std::vector<Event> net;
    std::vector<std::vector<std::uint8_t>> delivered;
    std::uint32_t produced = 0;
    TimePoint now = t0;

    auto push = [&](Event e) {
        if (rng.uniform() < loss) return;
        const int copies = rng.uniform() < dup ? 2 : 1;
        for (int i = 0; i < copies; ++i) {
            Event c = e;
            std::uint64_t ms = 10 + rng.below(20);
            if (rng.uniform() < reorder) ms += rng.below(80);
            c.at = now + milliseconds(ms);
            net.push_back(std::move(c));
        }
    };

    const TimePoint limit = t0 + std::chrono::seconds(3000);
    while (now < limit) {
        // application: produce up to 3 messages per ms-step until done
        for (int i = 0; i < 3 && produced < messages; ++i) {
            if (!sender.enqueue(bytes(produced))) break;
            ++produced;
        }
        // sender transmits
        std::vector<ReliableChannel::Transmission> tx;
        sender.collect(now, rtt, tx);
        if (sender.failed()) return {false, "sender failed (retry budget exhausted)", now, {}, {}};
        for (const auto& t : tx) push(Event{now, false, t.sequence, *t.data, 0, 0});

        // network delivers due events (in arbitrary stored order => extra reordering)
        std::vector<Event> due;
        for (std::size_t i = 0; i < net.size();) {
            if (net[i].at <= now) { due.push_back(std::move(net[i])); net[i] = std::move(net.back()); net.pop_back(); }
            else ++i;
        }
        std::sort(due.begin(), due.end(), [](const Event& a, const Event& b) { return a.at < b.at; });
        for (auto& e : due) {
            if (e.is_ack) sender.on_ack(e.next_expected, e.bits, now, rtt);
            else receiver.receive(e.seq, e.data, delivered);
        }
        // receiver acks after processing the batch
        if (receiver.ack_pending()) {
            push(Event{now, true, 0, {}, receiver.next_expected(), receiver.received_bits()});
            receiver.ack_sent();
        }
        if (delivered.size() >= messages && sender.send_idle()) break;
        now += milliseconds(1);
    }

    SimResult res;
    res.finished = now;
    res.sender = sender.stats();
    res.receiver = receiver.stats();
    if (delivered.size() != messages) { res.why = "delivered " + std::to_string(delivered.size()) + " of " + std::to_string(messages); return res; }
    for (std::uint32_t i = 0; i < messages; ++i) {
        if (value_of(delivered[i]) != i) { res.why = "order/duplicate violation at index " + std::to_string(i); return res; }
    }
    res.ok = true;
    return res;
}

void test_randomized_pairs() {
    struct Profile { const char* name; double loss, dup, reorder; };
    const Profile profiles[] = {
        {"clean", 0.0, 0.0, 0.0},
        {"10% loss", 0.10, 0.0, 0.0},
        {"30% loss + 20% duplication + heavy reordering", 0.30, 0.20, 0.50},
        {"50% loss", 0.50, 0.05, 0.30},
        {"reorder + duplicate only", 0.0, 0.40, 0.80},
    };
    for (const auto& p : profiles) {
        int bad = 0;
        std::string first_why;
        std::uint64_t retrans = 0, dups = 0;
        for (std::uint64_t seed = 1; seed <= 60; ++seed) {
            // Even seeds start right below the 32-bit wrap so sequence arithmetic is exercised in-flight.
            const std::uint32_t start = (seed % 2 == 0) ? 0xFFFFFFFFu - 100u : 1u;
            auto r = run_pair(seed, p.loss, p.dup, p.reorder, 400, start);
            retrans += r.sender.retransmissions;
            dups += r.receiver.duplicates;
            if (!r.ok) { ++bad; if (first_why.empty()) first_why = "seed " + std::to_string(seed) + ": " + r.why; }
        }
        check(bad == 0, std::string("randomized pair [") + p.name + "]: " + first_why);
        if (p.loss > 0.0) check(retrans > 0, std::string("[") + p.name + "] exercised retransmission");
        if (p.dup > 0.0) check(dups > 0, std::string("[") + p.name + "] exercised duplicate suppression");
    }
}

} // namespace

int main() {
    test_basic_send_and_ack();
    test_window_and_backlog();
    test_timeout_backoff_and_failure();
    test_selective_ack_and_fast_retransmit();
    test_progress_resets_the_failure_budget();
    test_invalid_and_stale_acks();
    test_receiver_ordering_and_duplicates();
    test_randomized_pairs();
    if (failures == 0) std::cout << "All DWE reliable channel tests passed.\n";
    return failures == 0 ? EXIT_SUCCESS : EXIT_FAILURE;
}
