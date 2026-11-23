#include "dwe/network/ConnectionManager.h"

#include <cstdlib>
#include <iostream>
#include <string>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <vector>

using namespace std::chrono;
using namespace dwe::core;
using namespace dwe::network;

namespace {
int failures = 0;
void check(bool condition, const std::string& message) {
    if (!condition) {
        std::cerr << "FAIL: " << message << '\n';
        ++failures;
    }
}

const auto start = TimePoint{} + seconds(100);

Packet hello_packet(std::uint32_t sequence, std::uint64_t nonce) {
    Packet p;
    p.type = MessageType::Hello;
    p.sequence = sequence;
    encode_hello_payload(nonce, p.payload);
    return p;
}
SessionId welcome_id(const ConnectionResult& r) {
    SessionId id = 0;
    if (r.response && r.response->type == MessageType::Welcome) decode_session_id(r.response->payload, id);
    return id;
}
Packet channel_packet(SessionId session, ChannelId channel, std::uint32_t sequence, std::vector<std::uint8_t> data) {
    Packet p;
    p.type = MessageType::ChannelData;
    p.sequence = sequence;
    encode_channel_payload(ChannelMessage{session, channel, std::move(data)}, p.payload);
    return p;
}
Packet disconnect_packet(SessionId session, std::uint32_t sequence = 1) {
    Packet p;
    p.type = MessageType::Disconnect;
    p.sequence = sequence;
    encode_disconnect_payload(DisconnectMessage{session, DisconnectReason::Requested}, p.payload);
    return p;
}
Packet ping_packet(SessionId session, std::uint32_t sequence) {
    Packet p;
    p.type = MessageType::Ping;
    p.sequence = sequence;
    encode_session_id(session, p.payload);
    return p;
}
std::size_t count_type(const std::vector<OutgoingPacket>& v, MessageType t) {
    std::size_t n = 0;
    for (const auto& o : v) n += o.packet.type == t;
    return n;
}

struct Rng {
    std::uint64_t s;
    explicit Rng(std::uint64_t seed) : s(seed * 0x9E3779B97F4A7C15ULL + 12345) {}
    std::uint64_t next() { s ^= s << 13; s ^= s >> 7; s ^= s << 17; return s; }
};

// ------------------------------------------------------------------------------------------------
void test_session_lifecycle() {
    ConnectionManager manager(2, seconds(10));
    const NetworkEndpoint client_a{"127.0.0.1", 40001}, client_b{"127.0.0.1", 40002}, attacker{"127.0.0.1", 40003};

    auto connected = manager.process(hello_packet(10, 0xAAAA), client_a, start);
    check(connected.accepted && connected.session_created, "Hello creates a new session");
    check(connected.response && connected.response->type == MessageType::Welcome, "Hello returns Welcome");
    check(connected.response && connected.response->sequence == 10, "Welcome echoes the Hello sequence (transaction id)");
    const SessionId session_id = welcome_id(connected);
    check(session_id != 0 && manager.has_session(session_id) && manager.active_sessions() == 1, "session registered");

    auto again = manager.process(hello_packet(11, 0xAAAA), client_a, start + seconds(1));
    check(welcome_id(again) == session_id && !again.session_created, "retransmitted Hello (same nonce) reuses the session");

    // Application data on an unreliable channel is delivered straight away.
    auto data = manager.process(channel_packet(session_id, ChannelId::Input, 1, {1, 2, 3, 4}), client_a, start + seconds(2));
    check(data.accepted && data.delivered.size() == 1 && data.delivered[0].data == std::vector<std::uint8_t>({1, 2, 3, 4}),
          "valid channel message accepted and exposed as application data");
    check(data.delivered[0].session_id == session_id && data.delivered[0].channel == ChannelId::Input, "delivered message carries session and channel");
    check(!data.response, "the manager itself no longer echoes: the application decides what to answer");

    auto spoofed = manager.process(channel_packet(session_id, ChannelId::Input, 2, {9}), attacker, start + seconds(3));
    check(!spoofed.accepted && !spoofed.error.empty() && spoofed.delivered.empty(), "session cannot be used from another endpoint");

    check(manager.process(hello_packet(1, 0xBBBB), client_b, start + seconds(3)).accepted && manager.active_sessions() == 2, "second client fits");
    auto full = manager.process(hello_packet(1, 0xCCCC), NetworkEndpoint{"127.0.0.1", 40004}, start + seconds(3));
    check(!full.accepted && full.response && full.response->type == MessageType::Error, "capacity limit enforced");

    // Disconnect is confirmed and idempotent.
    auto bye = manager.process(disconnect_packet(session_id), client_a, start + seconds(4));
    check(bye.accepted && bye.disconnected && bye.response && bye.response->type == MessageType::DisconnectAck, "Disconnect closes the session and is confirmed");
    SessionId confirmed = 0;
    check(bye.response && decode_session_id(bye.response->payload, confirmed) && confirmed == session_id, "DisconnectAck names the session");
    check(!manager.has_session(session_id), "session removed");
    auto bye2 = manager.process(disconnect_packet(session_id), client_a, start + seconds(5));
    check(bye2.accepted && !bye2.disconnected && bye2.response && bye2.response->type == MessageType::DisconnectAck,
          "retransmitted Disconnect (our ack was lost) is confirmed again");
    // ...but a stranger cannot close someone else's live session.
    const SessionId b_id = welcome_id(manager.process(hello_packet(2, 0xBBBB), client_b, start + seconds(5)));
    auto kill = manager.process(disconnect_packet(b_id), attacker, start + seconds(5));
    check(!kill.accepted && kill.response && kill.response->type == MessageType::Error && manager.has_session(b_id),
          "Disconnect from another endpoint is refused and the session survives");

    check(manager.expire(start + seconds(15)) == 1 && manager.active_sessions() == 0, "idle session expired by sweep");
    auto out = manager.take_outgoing();
    check(count_type(out, MessageType::Disconnect) == 1, "expired peer is told (best effort)");

    Packet bad = hello_packet(1, 1);
    bad.payload = {0x01};
    auto malformed = manager.process(bad, client_a, start + seconds(20));
    check(!malformed.accepted && malformed.response && malformed.response->type == MessageType::Error, "Hello without a proper nonce is rejected");
    Packet empty_hello;
    empty_hello.type = MessageType::Hello;
    check(!manager.process(empty_hello, client_a, start + seconds(20)).accepted, "0.2.0-style empty Hello is rejected in v2");
}

void test_restarted_client_gets_fresh_state() {
    ConnectionManager manager;
    const NetworkEndpoint ep{"10.0.0.7", 5000};
    const SessionId first = welcome_id(manager.process(hello_packet(1, 111), ep, start));
    // The first incarnation sends reliable messages 1..3.
    for (std::uint32_t seq = 1; seq <= 3; ++seq)
        check(manager.process(channel_packet(first, ChannelId::Control, seq, {static_cast<std::uint8_t>(seq)}), ep, start).delivered.size() == 1, "first incarnation message");

    // The client process restarts on the same address:port and begins again at sequence 1.
    auto second_hello = manager.process(hello_packet(1, 222), ep, start + seconds(1));
    const SessionId second = welcome_id(second_hello);
    check(second_hello.session_created && second != first && manager.has_session(second) && !manager.has_session(first),
          "a different nonce from the same endpoint replaces the session");
    check(manager.stats().sessions_replaced == 1 && manager.active_sessions() == 1, "replacement counted, no leak");
    auto fresh = manager.process(channel_packet(second, ChannelId::Control, 1, {42}), ep, start + seconds(1));
    check(fresh.delivered.size() == 1 && fresh.delivered[0].data[0] == 42,
          "sequence 1 of the new incarnation is delivered (old sequence state was not inherited)");
    check(!manager.process(channel_packet(first, ChannelId::Control, 4, {1}), ep, start + seconds(1)).accepted, "the old session ID is dead");
}

void test_reliable_through_manager() {
    ConnectionManager manager;
    const NetworkEndpoint ep{"10.0.0.1", 6000};
    const SessionId id = welcome_id(manager.process(hello_packet(1, 7), ep, start));

    // Client sends reliable messages 1, 2, 3 on Control; 2 is delayed (arrives after 3).
    auto r1 = manager.process(channel_packet(id, ChannelId::Control, 1, {1}), ep, start);
    auto r3 = manager.process(channel_packet(id, ChannelId::Control, 3, {3}), ep, start);
    check(r1.delivered.size() == 1 && r3.delivered.empty(), "message 3 waits for 2");
    manager.update(start);
    auto out = manager.take_outgoing();
    check(count_type(out, MessageType::Ack) == 1, "one Ack goes out after a burst of packets");
    AckMessage ack;
    check(decode_ack_payload(out[0].packet.payload, ack) && ack.entries.size() == 1 && ack.entries[0].next_expected == 2 &&
              ack.entries[0].received_bits == 0b1,
          "the ack reports next_expected=2 and selectively acknowledges 3");
    check(out[0].endpoint == ep && ack.session_id == id, "ack addressed to the client's endpoint and session");
    auto r2 = manager.process(channel_packet(id, ChannelId::Control, 2, {2}), ep, start + milliseconds(1));
    check(r2.delivered.size() == 2 && r2.delivered[0].data[0] == 2 && r2.delivered[1].data[0] == 3, "2 then 3 released in order");

    // Server -> client reliable message: transmitted, retransmitted when unacknowledged, stops once acked.
    check(manager.send(id, ChannelId::Combat, std::vector<std::uint8_t>{9, 9}, start + milliseconds(2)) == SendStatus::Ok, "send reliable");
    manager.update(start + milliseconds(2));
    out = manager.take_outgoing();
    std::size_t data_packets = count_type(out, MessageType::ChannelData);
    check(data_packets == 1, "first transmission");
    manager.update(start + milliseconds(2) + milliseconds(300));  // past the initial RTO of 250 ms
    out = manager.take_outgoing();
    check(count_type(out, MessageType::ChannelData) == 1, "retransmitted after the RTO");
    // Client acks it.
    Packet ack_packet;
    ack_packet.type = MessageType::Ack;
    encode_ack_payload(AckMessage{id, {{ChannelId::Combat, 2, 0}}}, ack_packet.payload);
    check(manager.process(ack_packet, ep, start + milliseconds(310)).accepted, "ack accepted");
    manager.update(start + seconds(5));
    check(count_type(manager.take_outgoing(), MessageType::ChannelData) == 0, "no further retransmission after the ack");

    // Acks from the wrong endpoint or for unknown sessions are ignored silently.
    auto stranger = manager.process(ack_packet, NetworkEndpoint{"10.9.9.9", 1}, start + seconds(5));
    check(!stranger.accepted && !stranger.response, "ack from another endpoint: ignored, no reply");
    Packet unknown = ack_packet;
    encode_ack_payload(AckMessage{9999, {{ChannelId::Combat, 2, 0}}}, unknown.payload);
    auto nosess = manager.process(unknown, ep, start + seconds(5));
    check(!nosess.accepted && !nosess.response, "ack for an unknown session: ignored, no reply (no reflection)");
    check(manager.send(9999, ChannelId::Control, std::vector<std::uint8_t>{1}, start) == SendStatus::UnknownSession, "send to unknown session reported");
}

void test_acks_are_flushed_between_timer_scans() {
    // The retransmission scan only runs every timer_interval (5 ms); an ack must not wait for it.
    ConnectionManager manager;
    const NetworkEndpoint ep{"10.0.0.9", 9100};
    const SessionId id = welcome_id(manager.process(hello_packet(1, 7), ep, start));
    manager.update(start);  // full scan happens here
    manager.take_outgoing();
    manager.process(channel_packet(id, ChannelId::Control, 1, {1}), ep, start + milliseconds(1));
    manager.update(start + milliseconds(1));  // 1 ms later: no timer scan is due
    check(count_type(manager.take_outgoing(), MessageType::Ack) == 1, "ack leaves immediately, without waiting for the next timer scan");

    // Same for new application data: a send() must go out on the very next update(), not 5 ms later.
    manager.send(id, ChannelId::Control, std::vector<std::uint8_t>{5}, start + milliseconds(2));
    manager.update(start + milliseconds(2));
    check(count_type(manager.take_outgoing(), MessageType::ChannelData) == 1, "send() is transmitted on the next update()");
}

void test_session_failure_when_peer_vanishes() {
    ConnectionConfig cfg;
    cfg.channel.reliable.max_retransmits = 4;
    ConnectionManager manager(cfg);
    const NetworkEndpoint ep{"10.0.0.2", 7000};
    const SessionId id = welcome_id(manager.process(hello_packet(1, 7), ep, start));
    manager.send(id, ChannelId::Control, std::vector<std::uint8_t>{1}, start);
    TimePoint now = start;
    std::size_t transmissions = 0;
    bool told = false;
    for (int i = 0; i < 20000 && manager.has_session(id); ++i, now += milliseconds(1)) {
        // keep the session "alive" from the idle point of view: only the reliable failure should end it
        manager.update(now);
        auto out = manager.take_outgoing();
        transmissions += count_type(out, MessageType::ChannelData);
        for (const auto& o : out) {
            DisconnectMessage m;
            if (o.packet.type == MessageType::Disconnect && decode_disconnect_payload(o.packet.payload, m)) told = m.reason == DisconnectReason::Timeout;
        }
        if (i % 1000 == 999) manager.process(ping_packet(id, 1), ep, now);  // keep-alives from the peer
    }
    check(!manager.has_session(id), "a session whose reliable message is never acknowledged is closed");
    check(manager.stats().sessions_failed == 1, "failure counted");
    check(transmissions == 5, "1 transmission + 4 retransmissions, then give up");
    check(told, "the peer is notified with reason Timeout (best effort)");
}

void test_idle_expiry_is_exact_not_sweep_dependent() {
    ConnectionManager manager(8, seconds(10));
    const NetworkEndpoint ep{"10.0.0.3", 8000};
    const SessionId id = welcome_id(manager.process(hello_packet(1, 7), ep, start));
    check(manager.process(channel_packet(id, ChannelId::Input, 1, {1}), ep, start + seconds(9) + milliseconds(999)).accepted,
          "a packet just before the idle timeout is accepted (and refreshes the session)");
    // No expire()/update() call has run, but the session must still be treated as dead after it idled out.
    check(!manager.process(channel_packet(id, ChannelId::Input, 2, {1}), ep, start + seconds(20) + milliseconds(1)).accepted,
          "a packet after the idle timeout is refused even if no sweep ran");
    check(!manager.has_session(id), "and the session was removed on contact");
}

void test_reply_loops_are_impossible() {
    ConnectionManager manager;
    const NetworkEndpoint ep{"10.0.0.4", 9000};
    const SessionId id = welcome_id(manager.process(hello_packet(1, 7), ep, start));
    // Reply-class messages must never produce a reply, whatever the payload.
    for (auto type : {MessageType::Error, MessageType::Echo, MessageType::Welcome, MessageType::DisconnectAck, MessageType::Ack}) {
        for (auto payload : {std::vector<std::uint8_t>{}, std::vector<std::uint8_t>(8, 1), std::vector<std::uint8_t>{'e', 'r', 'r'}}) {
            Packet p;
            p.type = type;
            p.sequence = 5;
            p.payload = payload;
            auto r = manager.process(p, ep, start);
            check(!r.response, std::string("no reply to message type ") + std::to_string(static_cast<int>(type)));
        }
    }
    // Requests that elicit a reply elicit exactly one non-reply-class... and that reply class is never answered.
    auto echo = manager.process(ping_packet(id, 77), ep, start);
    check(echo.response && echo.response->type == MessageType::Echo && echo.response->sequence == 77, "Ping is answered with Echo");
    check(!manager.process(*echo.response, ep, start).response, "...and the Echo is not answered again");
    auto err = manager.process(ping_packet(424242, 1), ep, start);
    check(err.response && err.response->type == MessageType::Error, "Ping for an unknown session gets an Error");
    check(!manager.process(*err.response, ep, start).response, "...and the Error is not answered");
    check(!manager.process(ping_packet(id, 1), NetworkEndpoint{"6.6.6.6", 1}, start).accepted, "Ping from another endpoint is refused");
}

void test_rate_limiting() {
    ConnectionConfig cfg;
    cfg.rate.packets = {10.0, 5.0, true};
    cfg.rate.bytes = {1000.0, 2000.0, true};
    cfg.rate.hello_per_endpoint = {1.0, 3.0, true};
    cfg.rate.hello_global = {1.0, 4.0, true};
    ConnectionManager manager(cfg);
    const NetworkEndpoint flooder{"10.1.1.1", 1111}, victim{"10.2.2.2", 2222};

    int passed = 0;
    for (int i = 0; i < 100; ++i) passed += manager.admit(flooder, 100, start);
    check(passed == 5, "packet burst limit: only 5 of 100 back-to-back packets admitted");
    check(manager.admit(victim, 100, start), "other endpoints are unaffected by the flooder");
    check(manager.stats().rate_limited_packets == 95, "dropped packets counted");
    check(manager.admit(flooder, 100, start + seconds(1)), "the flooder recovers after waiting");
    // Byte limit: two 1000-byte datagrams fit the 2000-byte burst, the third does not.
    check(manager.admit(NetworkEndpoint{"10.3.3.3", 1}, 1000, start) && manager.admit(NetworkEndpoint{"10.3.3.3", 1}, 1000, start) &&
              !manager.admit(NetworkEndpoint{"10.3.3.3", 1}, 1000, start),
          "byte-rate limit");

    // Hello flood from one endpoint: silently dropped, no reply and no state.
    const NetworkEndpoint h{"10.4.4.4", 4444};
    int replies = 0, silent = 0;
    for (std::uint32_t i = 0; i < 10; ++i) {
        auto r = manager.process(hello_packet(i + 1, 0xFEED), h, start);
        if (r.response) ++replies;
        if (r.rate_limited) { ++silent; check(!r.response, "rate-limited Hello gets no reply"); }
    }
    check(replies == 3 && silent == 7, "per-endpoint Hello limit: 3 answered (retransmissions of one Hello), 7 dropped");
    check(manager.active_sessions() == 1, "retransmitted Hellos did not create extra sessions");

    // Global session-creation limit: burst of 4 distinct new clients, the 5th is refused (the first
    // client's session already consumed one token above).
    int created = 0;
    for (std::uint16_t i = 0; i < 6; ++i) {
        auto r = manager.process(hello_packet(1, 100 + i), NetworkEndpoint{"10.5.5.5", static_cast<std::uint16_t>(5000 + i)}, start);
        created += r.session_created;
        if (!r.session_created) check(r.rate_limited && !r.response, "global limit is silent");
    }
    check(created == 3, "global limit allows burst(4) - 1 already used = 3 more sessions");
    // Idempotent retransmission does not need a global token.
    auto idem = manager.process(hello_packet(2, 0xFEED), h, start + milliseconds(1500));
    check(idem.accepted && !idem.session_created, "Hello retransmission still answered when the global bucket is empty");
}

void test_statistics_survive_session_end() {
    ConnectionManager manager;
    const NetworkEndpoint ep{"10.0.0.5", 5100};
    const SessionId id = welcome_id(manager.process(hello_packet(1, 9), ep, start));
    manager.send(id, ChannelId::Control, std::vector<std::uint8_t>{1}, start);
    manager.update(start);
    manager.update(start + milliseconds(300));  // RTO passes: one retransmission
    manager.take_outgoing();
    check(manager.aggregate_channel_stats().reliable.retransmissions == 1, "live session counted");
    manager.process(disconnect_packet(id), ep, start + seconds(1));
    check(!manager.has_session(id), "session gone");
    check(manager.aggregate_channel_stats().reliable.retransmissions == 1 &&
              manager.aggregate_channel_stats().reliable.first_transmissions == 1,
          "its counters are still part of the totals after it ended");
    // close_all keeps them too
    const SessionId id2 = welcome_id(manager.process(hello_packet(1, 10), NetworkEndpoint{"10.0.0.6", 5200}, start + seconds(2)));
    manager.send(id2, ChannelId::Control, std::vector<std::uint8_t>{1}, start + seconds(2));
    manager.update(start + seconds(2));
    manager.close_all(DisconnectReason::ServerShutdown);
    check(manager.aggregate_channel_stats().reliable.first_transmissions == 2, "close_all keeps the counters as well");
}

void test_close_all_and_untrusted_input() {
    ConnectionManager manager;
    for (std::uint16_t i = 0; i < 5; ++i) manager.process(hello_packet(1, 50 + i), NetworkEndpoint{"10.6.6.6", static_cast<std::uint16_t>(6000 + i)}, start);
    manager.close_all(DisconnectReason::ServerShutdown);
    auto out = manager.take_outgoing();
    check(manager.active_sessions() == 0 && count_type(out, MessageType::Disconnect) == 5, "close_all notifies every peer");
    DisconnectMessage m;
    check(decode_disconnect_payload(out[0].packet.payload, m) && m.reason == DisconnectReason::ServerShutdown, "reason ServerShutdown");

    // Fuzz: arbitrary packets from arbitrary endpoints must never crash or corrupt the table.
    ConnectionConfig cfg;
    cfg.max_sessions = 16;
    cfg.rate = RateLimitConfig::unlimited();
    ConnectionManager fuzzed(cfg);
    Rng rng(99);
    TimePoint now = start;
    for (int i = 0; i < 200000; ++i) {
        Packet p;
        p.type = static_cast<MessageType>(1 + rng.next() % 9);
        p.sequence = static_cast<std::uint32_t>(rng.next());
        const auto len = rng.next() % 40;
        for (std::uint64_t k = 0; k < len; ++k) p.payload.push_back(static_cast<std::uint8_t>(rng.next()));
        if (rng.next() % 3 == 0 && len >= 9) {   // make some of them structurally plausible
            for (int k = 0; k < 7; ++k) p.payload[static_cast<std::size_t>(k)] = 0;
            p.payload[7] = static_cast<std::uint8_t>(1 + rng.next() % 20);
        }
        const NetworkEndpoint ep{"10.7.7." + std::to_string(rng.next() % 8), static_cast<std::uint16_t>(1 + rng.next() % 4)};
        now += microseconds(rng.next() % 3000);
        auto r = fuzzed.process(p, ep, now);
        if (r.response) {
            check(r.response->type != MessageType::Echo || p.type == MessageType::Ping, "only Ping can produce Echo");
        }
        if (i % 50 == 0) { fuzzed.update(now); fuzzed.take_outgoing(); }
        if (fuzzed.active_sessions() > 16) { check(false, "session table exceeded its capacity"); break; }
    }
}

} // namespace

int main() {
    test_session_lifecycle();
    test_restarted_client_gets_fresh_state();
    test_reliable_through_manager();
    test_acks_are_flushed_between_timer_scans();
    test_session_failure_when_peer_vanishes();
    test_idle_expiry_is_exact_not_sweep_dependent();
    test_reply_loops_are_impossible();
    test_rate_limiting();
    test_statistics_survive_session_end();
    test_close_all_and_untrusted_input();
    if (failures == 0) std::cout << "All DWE connection manager tests passed.\n";
    return failures == 0 ? EXIT_SUCCESS : EXIT_FAILURE;
}
