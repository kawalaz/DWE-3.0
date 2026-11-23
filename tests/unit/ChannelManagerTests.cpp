// Tests for per-session channel handling: delivery modes, acknowledgements, failure, randomized links.
#include "dwe/network/ChannelManager.h"

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

using namespace dwe::core;
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
const TimePoint t0{};
constexpr SessionId kSession = 77;

std::vector<std::uint8_t> bytes(std::uint32_t v) {
    return {static_cast<std::uint8_t>(v >> 24), static_cast<std::uint8_t>(v >> 16), static_cast<std::uint8_t>(v >> 8),
            static_cast<std::uint8_t>(v)};
}
std::uint32_t value_of(const std::vector<std::uint8_t>& b) {
    return (std::uint32_t{b[0]} << 24) | (std::uint32_t{b[1]} << 16) | (std::uint32_t{b[2]} << 8) | b[3];
}

struct Rng {
    std::uint64_t s;
    explicit Rng(std::uint64_t seed) : s(seed * 6364136223846793005ULL + 1442695040888963407ULL) {}
    std::uint64_t next() { s ^= s << 13; s ^= s >> 7; s ^= s << 17; return s; }
    double uniform() { return static_cast<double>(next() >> 11) * (1.0 / 9007199254740992.0); }
    std::uint64_t below(std::uint64_t n) { return next() % n; }
};

// Feeds packets produced by `from` into `to`, as the session layer would after validating the session.
void deliver(const std::vector<Packet>& packets, ChannelManager& to, TimePoint now) {
    for (const auto& p : packets) {
        if (p.type == MessageType::ChannelData) {
            ChannelMessage m;
            if (!decode_channel_payload(p.payload, m)) { check(false, "malformed channel payload produced"); continue; }
            check(m.session_id == kSession, "packet carries the session ID");
            to.on_channel_data(m.channel, p.sequence, m.data, now);
        } else if (p.type == MessageType::Ack) {
            AckMessage a;
            if (!decode_ack_payload(p.payload, a)) { check(false, "malformed ack produced"); continue; }
            to.on_ack(a, now);
        } else {
            check(false, "unexpected packet type from ChannelManager");
        }
    }
}

std::vector<Packet> drain(ChannelManager& m) {
    std::vector<Packet> out;
    m.drain_outgoing(out);
    return out;
}
std::vector<ChannelMessage> delivered(ChannelManager& m) {
    std::vector<ChannelMessage> out;
    m.drain_delivered(out);
    return out;
}

// ------------------------------------------------------------------------------------------------
void test_unreliable_modes() {
    ChannelManager a(kSession), b(kSession);

    // Unreliable sends need no update(): the packet is queued at once, with increasing sequences.
    check(a.send(ChannelId::Input, bytes(1), t0) == SendStatus::Ok, "send input");
    check(a.send(ChannelId::Input, bytes(2), t0) == SendStatus::Ok, "send input 2");
    auto pk = drain(a);
    check(pk.size() == 2 && pk[0].sequence == 1 && pk[1].sequence == 2, "unreliable packets are sequenced 1, 2");

    // Input = Unreliable with duplicate/too-old detection.
    deliver({pk[1]}, b, t0);   // arrives first
    deliver({pk[0]}, b, t0);   // reordered but inside the window
    deliver({pk[1], pk[0]}, b, t0);  // network duplicates
    auto got = delivered(b);
    check(got.size() == 2 && value_of(got[0].data) == 2 && value_of(got[1].data) == 1,
          "Input: reordered messages delivered once each, in arrival order");
    check(b.stats().duplicates_dropped == 2, "Input: duplicates counted");
    // A message far older than the window is stale.
    for (std::uint32_t i = 3; i < 200; ++i) a.send(ChannelId::Input, bytes(i), t0);
    auto many = drain(a);
    deliver({many.back()}, b, t0);
    delivered(b);
    Packet ancient = pk[0];
    ancient.sequence = 1;
    deliver({ancient}, b, t0);  // seq 1 again: duplicate or too old, either way dropped
    check(delivered(b).empty(), "Input: a message older than the window is dropped");
    check(b.stats().stale_dropped >= 1, "Input: stale counted");

    // Snapshot = UnreliableSequenced: only newer than the newest delivered.
    ChannelManager c(kSession), d(kSession);
    for (std::uint32_t i = 1; i <= 4; ++i) c.send(ChannelId::Snapshot, bytes(i), t0);
    auto snaps = drain(c);
    deliver({snaps[2]}, d, t0);  // 3 arrives first
    deliver({snaps[0]}, d, t0);  // 1 is older than 3: dropped, never retransmitted
    deliver({snaps[3]}, d, t0);  // 4
    deliver({snaps[3]}, d, t0);  // duplicate of 4
    deliver({snaps[1]}, d, t0);  // 2 late: dropped
    got = delivered(d);
    check(got.size() == 2 && value_of(got[0].data) == 3 && value_of(got[1].data) == 4,
          "Snapshot: only strictly newer messages are delivered");
    check(d.stats().stale_dropped == 3, "Snapshot: three stale/duplicate drops");
    // Nothing is retransmitted for unreliable channels, however long we wait.
    c.update(t0 + std::chrono::seconds(60));
    check(drain(c).empty(), "unreliable channels are never retransmitted");
    check(c.reliable_idle(), "unreliable traffic does not make the session 'busy'");
}

void test_reliable_roundtrip_and_ack_packing() {
    ChannelManager a(kSession), b(kSession);
    // Messages on three reliable channels.
    for (std::uint32_t i = 0; i < 3; ++i) a.send(ChannelId::Control, bytes(100 + i), t0);
    a.send(ChannelId::Combat, bytes(200), t0);
    a.send(ChannelId::Inventory, bytes(300), t0);
    check(drain(a).empty(), "reliable sends are emitted by update(), not by send()");
    a.update(t0);
    auto pk = drain(a);
    check(pk.size() == 5, "five data packets");
    deliver(pk, b, t0 + milliseconds(20));
    auto got = delivered(b);
    check(got.size() == 5, "all five delivered");
    check(got[0].channel == ChannelId::Control && value_of(got[0].data) == 100 && value_of(got[2].data) == 102,
          "Control messages in order");

    b.update(t0 + milliseconds(20));
    auto acks = drain(b);
    check(acks.size() == 1 && acks[0].type == MessageType::Ack, "one Ack packet covers all channels");
    AckMessage decoded;
    check(decode_ack_payload(acks[0].payload, decoded) && decoded.entries.size() == 3, "ack has an entry per active channel");
    check(!a.reliable_idle(), "sender still waits for the ack");
    deliver(acks, a, t0 + milliseconds(40));
    check(a.reliable_idle(), "ack received: sender idle");
    check(a.rtt().has_sample(), "RTT measured from acks");
    b.update(t0 + milliseconds(41));
    check(drain(b).empty(), "no ack is repeated when nothing new arrived");

    // Retransmitted data after a lost ack is re-acknowledged but not delivered twice.
    a.send(ChannelId::Control, bytes(1000), t0 + milliseconds(50));
    a.update(t0 + milliseconds(50));
    pk = drain(a);
    deliver(pk, b, t0 + milliseconds(60));
    b.update(t0 + milliseconds(60));
    drain(b);                                  // ack "lost"
    a.update(t0 + milliseconds(50) + std::chrono::seconds(1));  // RTO fires
    pk = drain(a);
    check(pk.size() == 1, "sender retransmits the unacknowledged message");
    delivered(b);
    deliver(pk, b, t0 + std::chrono::seconds(2));
    check(delivered(b).empty(), "retransmission is not delivered a second time");
    b.update(t0 + std::chrono::seconds(2));
    acks = drain(b);
    check(acks.size() == 1, "duplicate is re-acknowledged");
    deliver(acks, a, t0 + std::chrono::seconds(2));
    check(a.reliable_idle(), "re-ack completes delivery");
}

void test_limits_and_failure() {
    ChannelManager a(kSession);
    check(a.send(ChannelId::Control, std::vector<std::uint8_t>(kMaxChannelDataSize, 0), t0) == SendStatus::Ok, "largest message fits");
    check(a.send(ChannelId::Control, std::vector<std::uint8_t>(kMaxChannelDataSize + 1, 0), t0) == SendStatus::TooLarge, "larger than one datagram is refused");
    check(a.send(static_cast<ChannelId>(42), bytes(0), t0) == SendStatus::UnknownChannel, "unknown channel is refused");

    ChannelConfig cfg;
    cfg.reliable.max_backlog = 5;
    ChannelManager q(kSession, cfg);
    int ok = 0;
    for (int i = 0; i < 10; ++i) ok += q.send(ChannelId::Combat, bytes(static_cast<std::uint32_t>(i)), t0) == SendStatus::Ok;
    check(ok == 5, "QueueFull once the reliable backlog is exhausted");
    check(q.send(ChannelId::Combat, bytes(0), t0) == SendStatus::QueueFull, "QueueFull status reported");

    // Peer never answers: bounded retransmission, then the session reports failure.
    ChannelConfig fast;
    fast.reliable.max_retransmits = 4;
    ChannelManager f(kSession, fast);
    f.send(ChannelId::Control, bytes(1), t0);
    TimePoint now = t0;
    std::uint64_t data_packets = 0;
    for (int step = 0; step < 20000 && !f.failed(); ++step) {
        f.update(now);
        for (const auto& p : drain(f)) data_packets += p.type == MessageType::ChannelData;
        now += milliseconds(1);
    }
    check(f.failed(), "unanswered reliable message eventually fails the session");
    check(data_packets == 1 + 4, "exactly 1 transmission + max_retransmits retransmissions were made");
    check(f.send(ChannelId::Control, bytes(2), now) == SendStatus::Failed, "sending on a failed session is refused");
    check(f.send(ChannelId::Snapshot, bytes(2), now) == SendStatus::Failed, "...for unreliable channels too");

    // Bad acks are ignored, never crash or corrupt state.
    ChannelManager g(kSession);
    g.send(ChannelId::Control, bytes(1), t0);
    g.update(t0);
    drain(g);
    AckMessage bogus;
    bogus.session_id = kSession;
    bogus.entries = {{ChannelId::Control, 999, 0}, {ChannelId::Control, 1, ~std::uint64_t{0}}};
    g.on_ack(bogus, t0);
    check(!g.reliable_idle(), "invalid acks do not complete delivery");
    check(g.stats().reliable.invalid_acks >= 1, "invalid acks are counted");
    AckMessage for_unreliable;
    for_unreliable.entries = {{ChannelId::Snapshot, 5, 0}};  // not constructible via the codec, but must be ignored here
    g.on_ack(for_unreliable, t0);
    check(!g.failed(), "an ack naming an unreliable channel is ignored");
}

// ------------------------------------------------------------------------------------------------
// Both directions, all channels, hostile link: reliable channels exactly-once in order; Snapshot
// strictly increasing; Input never duplicated and never invented.
void test_randomized_link() {
    struct InFlight { TimePoint at; Packet packet; bool to_b; };
    struct Profile { const char* name; double loss, dup, reorder; };
    const Profile profiles[] = {{"10% loss", 0.10, 0.05, 0.2}, {"35% loss, dup, reorder", 0.35, 0.25, 0.6}};
    for (const auto& prof : profiles) {
        int bad = 0;
        std::string why;
        std::uint64_t total_retransmissions = 0, total_dups_seen = 0, total_stale = 0;
        for (std::uint64_t seed = 1; seed <= 25; ++seed) {
            Rng rng(seed);
            ChannelConfig cfg;
            cfg.reliable.max_retransmits = 60;
            ChannelManager a(kSession, cfg), b(kSession, cfg);
            std::vector<InFlight> net;
            TimePoint now = t0;
            const std::uint32_t kPerChannel = 150;
            const ChannelId reliable_channels[] = {ChannelId::Control, ChannelId::Combat, ChannelId::Inventory, ChannelId::Destruction, ChannelId::WorldSync};
            std::map<ChannelId, std::uint32_t> sent_a, sent_b;       // reliable counters per direction
            std::map<ChannelId, std::uint32_t> got_at_b, got_at_a;
            std::uint32_t input_sent = 0, snap_sent = 0, last_snap_at_b = 0;
            std::vector<std::uint32_t> input_seen;
            bool snap_ok = true, order_ok = true;

            auto link = [&](ChannelManager& from, bool to_b) {
                std::vector<Packet> out;
                from.drain_outgoing(out);
                for (auto& p : out) {
                    if (rng.uniform() < prof.loss) continue;
                    const int copies = rng.uniform() < prof.dup ? 2 : 1;
                    for (int k = 0; k < copies; ++k) {
                        std::uint64_t ms = 8 + rng.below(20);
                        if (rng.uniform() < prof.reorder) ms += rng.below(60);
                        net.push_back({now + milliseconds(ms), p, to_b});
                    }
                }
            };

            const TimePoint limit = t0 + std::chrono::seconds(2000);
            bool done = false;
            for (; now < limit && !done; now += milliseconds(2)) {
                // application traffic for the first 3 simulated seconds
                if (now < t0 + std::chrono::seconds(3)) {
                    for (auto ch : reliable_channels) {
                        if (sent_a[ch] < kPerChannel && rng.uniform() < 0.3 &&
                            a.send(ch, bytes(sent_a[ch]), now) == SendStatus::Ok) ++sent_a[ch];
                        if (sent_b[ch] < kPerChannel && rng.uniform() < 0.3 &&
                            b.send(ch, bytes(sent_b[ch]), now) == SendStatus::Ok) ++sent_b[ch];
                    }
                    if (rng.uniform() < 0.5) a.send(ChannelId::Input, bytes(++input_sent), now);
                    if (rng.uniform() < 0.5) a.send(ChannelId::Snapshot, bytes(++snap_sent), now);
                }
                // deliver due packets
                std::vector<InFlight> due;
                for (std::size_t i = 0; i < net.size();) {
                    if (net[i].at <= now) { due.push_back(std::move(net[i])); net[i] = std::move(net.back()); net.pop_back(); }
                    else ++i;
                }
                std::sort(due.begin(), due.end(), [](const InFlight& x, const InFlight& y) { return x.at < y.at; });
                for (auto& f : due) deliver({f.packet}, f.to_b ? b : a, now);
                a.update(now); b.update(now);
                link(a, true); link(b, false);

                for (auto& m : delivered(b)) {
                    if (m.channel == ChannelId::Input) input_seen.push_back(value_of(m.data));
                    else if (m.channel == ChannelId::Snapshot) { if (value_of(m.data) <= last_snap_at_b) snap_ok = false; last_snap_at_b = value_of(m.data); }
                    else { if (value_of(m.data) != got_at_b[m.channel]++) order_ok = false; }
                }
                for (auto& m : delivered(a)) {
                    if (value_of(m.data) != got_at_a[m.channel]++) order_ok = false;
                }
                if (a.failed() || b.failed()) { why = "session failed"; break; }
                done = now > t0 + std::chrono::seconds(3) && a.reliable_idle() && b.reliable_idle() && net.empty();
                if (done) {
                    for (auto ch : reliable_channels) {
                        if (got_at_b[ch] != sent_a[ch] || got_at_a[ch] != sent_b[ch]) done = false;
                    }
                }
            }
            std::vector<std::uint32_t> sorted = input_seen;
            std::sort(sorted.begin(), sorted.end());
            const bool input_unique = std::adjacent_find(sorted.begin(), sorted.end()) == sorted.end();
            const bool input_real = sorted.empty() || sorted.back() <= input_sent;
            bool counts_ok = done;
            for (auto ch : reliable_channels) {
                if (sent_a[ch] != kPerChannel || sent_b[ch] != kPerChannel) counts_ok = false;  // guard against a vacuous pass
            }
            if (input_sent < 100 || snap_sent < 100) counts_ok = false;
            total_retransmissions += a.stats().reliable.retransmissions + b.stats().reliable.retransmissions;
            total_dups_seen += a.stats().reliable.duplicates + b.stats().reliable.duplicates + b.stats().duplicates_dropped;
            total_stale += b.stats().stale_dropped;
            if (!(counts_ok && order_ok && snap_ok && input_unique && input_real)) {
                ++bad;
                if (why.empty()) {
                    why = "seed " + std::to_string(seed) + ": done=" + std::to_string(done) + " order=" + std::to_string(order_ok) +
                          " snap=" + std::to_string(snap_ok) + " input_unique=" + std::to_string(input_unique) +
                          " input_real=" + std::to_string(input_real);
                }
            }
        }
        check(bad == 0, std::string("randomized link [") + prof.name + "]: " + why);
        check(total_retransmissions > 0, std::string("[") + prof.name + "] retransmission was exercised");
        check(total_dups_seen > 0, std::string("[") + prof.name + "] duplicate suppression was exercised");
        check(total_stale > 0, std::string("[") + prof.name + "] stale-packet dropping was exercised");
    }
}

} // namespace

int main() {
    test_unreliable_modes();
    test_reliable_roundtrip_and_ack_packing();
    test_limits_and_failure();
    test_randomized_link();
    if (failures == 0) std::cout << "All DWE channel manager tests passed.\n";
    return failures == 0 ? EXIT_SUCCESS : EXIT_FAILURE;
}
