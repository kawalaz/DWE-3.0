// End-to-end reliability tests: a real Server and real ClientConnections talking through the
// deterministic SimulatedNetwork (virtual time, seeded loss / duplication / reordering / latency).
#include "dwe/network/ClientConnection.h"
#include "dwe/network/EchoWorkload.h"
#include "dwe/network/Server.h"
#include "dwe/network/SimulatedNetwork.h"

#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>
#include <vector>
#include <chrono>
#include <cstddef>
#include <cstdint>

using namespace dwe::core;
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

struct World {
    SimulatedNetwork net;
    std::unique_ptr<Server> server;
    std::vector<std::unique_ptr<SimTransport>> transports;
    std::vector<std::unique_ptr<ClientConnection>> clients;
    std::vector<std::unique_ptr<EchoWorkload>> workloads;
    ClientConfig client_defaults;
    WorkloadConfig workload_defaults;
    bool use_workloads = true;  // false: the test reads client.drain_delivered() itself

    explicit World(ImpairmentConfig impairment, std::uint64_t seed, bool rate_limits = false, std::size_t max_sessions = 4096)
        : net(impairment, seed) {
        ServerConfig cfg;
        cfg.port = 27015;
        cfg.receive_timeout_ms = 0;
        cfg.connection.max_sessions = max_sessions;
        if (!rate_limits) cfg.connection.rate = RateLimitConfig::unlimited();
        server = std::make_unique<Server>(net.create_transport("10.0.0.1"), cfg, [this] { return net.now(); });
        std::string error;
        if (!server->start(&error)) check(false, "server start: " + error);
        client_defaults.host = "10.0.0.1";
        client_defaults.port = 27015;
        client_defaults.connect_timeout = seconds(20);
    }

    ClientConnection& add_client() {
        const std::size_t i = clients.size();
        transports.push_back(net.create_transport("10.1." + std::to_string(i / 250) + "." + std::to_string(i % 250 + 1)));
        std::string error;
        if (!transports.back()->open(0, &error)) check(false, "client open: " + error);
        ClientConfig cfg = client_defaults;
        cfg.nonce = 0x1000 + i;
        clients.push_back(std::make_unique<ClientConnection>(*transports.back(), cfg, [this] { return net.now(); }));
        workloads.push_back(std::make_unique<EchoWorkload>(*clients.back(), workload_defaults));
        clients.back()->connect();
        return *clients.back();
    }

    void restart_client(std::size_t i, std::uint64_t nonce) {  // new process, same address:port
        ClientConfig cfg = client_defaults;
        cfg.nonce = nonce;
        clients[i] = std::make_unique<ClientConnection>(*transports[i], cfg, [this] { return net.now(); });
        workloads[i] = std::make_unique<EchoWorkload>(*clients[i], workload_defaults);
        clients[i]->connect();
    }

    void step(Duration dt = milliseconds(1)) {
        net.advance(dt);
        server->poll();
        for (std::size_t i = 0; i < clients.size(); ++i) {
            clients[i]->update();
            if (use_workloads) workloads[i]->tick(net.now());
        }
    }
    void run_for(Duration d) {
        const TimePoint end = net.now() + d;
        while (net.now() < end) step();
    }
    bool all_connected() const {
        for (const auto& c : clients) if (!c->connected()) return false;
        return true;
    }
    bool all_reliable_complete() const {
        for (const auto& w : workloads) if (!w->reliable_complete()) return false;
        return true;
    }
    std::uint64_t violations() const {
        std::uint64_t v = 0;
        for (const auto& w : workloads) v += w->violations();
        return v;
    }
};

ImpairmentConfig hostile() {
    ImpairmentConfig c;
    c.loss = 0.25; c.duplicate = 0.20; c.reorder = 0.5;
    c.latency = milliseconds(25); c.jitter = milliseconds(10); c.reorder_extra = milliseconds(60);
    return c;
}

// ------------------------------------------------------------------------------------------------
void test_handshake_and_basic_echo() {
    ImpairmentConfig imp;
    imp.latency = milliseconds(20);
    World w(imp, 1);
    w.use_workloads = false;
    auto& c = w.add_client();
    w.run_for(milliseconds(200));
    check(c.connected() && c.session_id() != 0, "client connects over a clean 20 ms link");
    const auto hs = std::chrono::duration<double, std::milli>(c.stats().handshake_latency).count();
    check(hs >= 40.0 && hs <= 45.0, "handshake takes one round trip (got " + std::to_string(hs) + " ms)");
    check(w.server->connections().active_sessions() == 1, "server has one session");

    // One message per channel; the echo server returns each on its own channel.
    for (std::size_t i = 0; i < kChannelCount; ++i)
        check(c.send(static_cast<ChannelId>(i), std::vector<std::uint8_t>{static_cast<std::uint8_t>(i), 1, 2, 3}) == SendStatus::Ok, "send on channel");
    std::vector<ChannelMessage> got;
    for (int i = 0; i < 200; ++i) { w.step(); c.drain_delivered(got); }
    check(got.size() == kChannelCount, "every channel echoed its message exactly once (got " + std::to_string(got.size()) + ")");
    for (const auto& m : got) check(m.data.size() == 4 && m.data[0] == static_cast<std::uint8_t>(m.channel), "echo returns on the same channel with the same payload");
    check(c.reliable_idle(), "all reliable messages acknowledged");
    check(c.rtt().has_sample(), "client measured an RTT");
    const auto rtt_ms = std::chrono::duration<double, std::milli>(c.rtt().srtt()).count();
    check(rtt_ms > 35.0 && rtt_ms < 60.0, "smoothed RTT reflects the 2 x 20 ms path (got " + std::to_string(rtt_ms) + " ms)");
}

void test_handshake_survives_loss() {
    ImpairmentConfig imp;
    imp.loss = 0.2; imp.latency = milliseconds(15);
    World w(imp, 5);
    w.workload_defaults = WorkloadConfig{0, 0, 0, 32, 0};
    for (int i = 0; i < 40; ++i) w.add_client();
    w.run_for(seconds(15));
    check(w.all_connected(), "all 40 clients connect through 20% loss thanks to Hello retransmission");
    check(w.server->connections().active_sessions() == 40, "no duplicate sessions from retransmitted Hellos (got " +
                                                               std::to_string(w.server->connections().active_sessions()) + ")");
    std::uint64_t retries = 0;
    for (const auto& c : w.clients) retries += c->stats().hellos_sent - 1;
    check(retries > 0, "loss really forced Hello retransmissions");
}

void test_exactly_once_in_order_on_a_hostile_network(std::uint64_t seed, int clients, int per_channel) {
    World w(hostile(), seed);
    w.workload_defaults.max_per_channel = static_cast<std::uint32_t>(per_channel);
    w.workload_defaults.reliable_hz = 40;  // fast, so the window and backlog are really exercised
    w.workload_defaults.input_hz = 60;
    w.workload_defaults.snapshot_hz = 30;
    for (int i = 0; i < clients; ++i) w.add_client();
    // Run until everything sent has been echoed, or give up after 10 virtual minutes.
    const TimePoint limit = w.net.now() + seconds(600);
    while (w.net.now() < limit) {
        w.step();
        bool done = w.all_connected();
        for (const auto& wl : w.workloads) {
            if (wl->channel(ChannelId::Control).sent < static_cast<std::uint64_t>(per_channel)) done = false;
        }
        if (done && w.all_reliable_complete()) break;
    }
    const std::string tag = " [seed " + std::to_string(seed) + "]";
    // The echo coming back proves delivery, not that the *senders* were told. Keep the network
    // hostile for a while longer: a sender that never gets its final acks keeps retransmitting
    // until its session is declared dead, which would show up here.
    w.run_for(seconds(40));
    check(w.all_connected(), "all clients still connected" + tag);
    for (const auto& c : w.clients) check(c->reliable_idle(), "every client's reliable messages were acknowledged" + tag);
    check(w.all_reliable_complete(), "every reliable message came back" + tag);
    check(w.violations() == 0, "no duplicate / out-of-order / regressed message was ever delivered" + tag);
    std::uint64_t reliable_sent = 0, reliable_echoed = 0, input_sent = 0, input_echoed = 0, snap_sent = 0, snap_echoed = 0;
    for (const auto& wl : w.workloads) {
        reliable_sent += wl->reliable_sent();
        reliable_echoed += wl->reliable_echoed();
        input_sent += wl->channel(ChannelId::Input).sent;
        input_echoed += wl->channel(ChannelId::Input).echoed;
        snap_sent += wl->channel(ChannelId::Snapshot).sent;
        snap_echoed += wl->channel(ChannelId::Snapshot).echoed;
        for (std::size_t i = 0; i < kChannelCount; ++i) {
            const auto id = static_cast<ChannelId>(i);
            if (is_reliable(id)) check(wl->channel(id).sent == static_cast<std::uint64_t>(per_channel), "the full workload was actually sent (no vacuous pass)" + tag);
        }
    }
    check(reliable_sent == reliable_echoed && reliable_sent == static_cast<std::uint64_t>(clients) * 5 * static_cast<std::uint64_t>(per_channel),
          "reliable: sent == echoed == clients x 5 channels x messages" + tag);
    // Unreliable channels must NOT be repaired: the lossy network has to be visible there.
    check(input_echoed < input_sent && snap_echoed < snap_sent, "unreliable channels lose messages (and are not retransmitted)" + tag);
    check(input_echoed > input_sent / 4 && snap_echoed > snap_sent / 4, "...but a sane share still arrives" + tag);
    const auto agg = w.server->connections().aggregate_channel_stats();
    check(agg.reliable.retransmissions > 0, "server retransmitted lost data" + tag);
    {   // Efficiency regression guard: with selective acks ~0.45 retransmissions per message at 25% loss;
        // without them ~1.8. Anything near 1 means gaps are being repaired the slow, wasteful way.
        std::uint64_t first = agg.reliable.first_transmissions, retr = agg.reliable.retransmissions;
        for (const auto& c : w.clients) { const auto cs = c->channel_stats(); first += cs.reliable.first_transmissions; retr += cs.reliable.retransmissions; }
        const double ratio = static_cast<double>(retr) / static_cast<double>(first);
        check(ratio < 0.9, "retransmission overhead stays reasonable at 25% loss (" + std::to_string(ratio) + " per message)" + tag);
    }
    check(agg.reliable.duplicates > 0, "server suppressed duplicated/retransmitted data" + tag);
    check(w.server->connections().stats().sessions_failed == 0 && w.server->connections().active_sessions() == static_cast<std::size_t>(clients),
          "no session was lost" + tag);
}

void test_short_outage_is_repaired() {
    ImpairmentConfig imp;
    imp.latency = milliseconds(20);
    World w(imp, 3);
    w.workload_defaults.max_per_channel = 400;
    w.workload_defaults.reliable_hz = 20;
    for (int i = 0; i < 8; ++i) w.add_client();
    w.run_for(seconds(3));
    ImpairmentConfig blackout = imp;
    blackout.loss = 1.0;
    w.net.set_impairment(blackout);
    w.run_for(seconds(3));  // total blackout in both directions
    w.net.set_impairment(imp);
    const TimePoint limit = w.net.now() + seconds(300);
    while (w.net.now() < limit) {
        w.step();
        bool sent_all = true;
        for (const auto& wl : w.workloads) if (wl->channel(ChannelId::Control).sent < 400) sent_all = false;
        if (sent_all && w.all_reliable_complete()) break;
    }
    check(w.all_connected(), "sessions survive a 3 s blackout");
    check(w.all_reliable_complete() && w.violations() == 0, "everything sent before, during and after the blackout is delivered exactly once, in order");
    check(w.server->connections().aggregate_channel_stats().reliable.retransmissions > 0, "retransmission did the repair");
}

void test_long_outage_fails_cleanly() {
    ImpairmentConfig imp;
    imp.latency = milliseconds(20);
    World w(imp, 4);
    w.workload_defaults.reliable_hz = 10;
    for (int i = 0; i < 6; ++i) w.add_client();
    w.run_for(seconds(2));
    check(w.all_connected(), "connected before the outage");
    ImpairmentConfig dead = imp;
    dead.loss = 1.0;
    w.net.set_impairment(dead);
    w.run_for(seconds(60));
    bool all_failed = true;
    for (const auto& c : w.clients) all_failed = all_failed && c->state() == ClientState::Failed && !c->failure_reason().empty();
    check(all_failed, "after a 60 s blackout every client reports a failed connection with a reason");
    check(w.server->connections().active_sessions() == 0, "the server dropped the dead sessions (reliable failure or idle timeout)");
    const auto& s = w.server->connections().stats();
    check(s.sessions_failed + s.sessions_expired == 6, "each session was removed exactly once");
}

void test_keepalive_holds_an_idle_session() {
    ImpairmentConfig imp;
    imp.latency = milliseconds(10);
    World w(imp, 6);
    w.workload_defaults = WorkloadConfig{0, 0, 0, 32, 0};
    for (int i = 0; i < 5; ++i) w.add_client();
    w.run_for(seconds(120));  // 8x the server's 15 s idle timeout, with no application traffic at all
    check(w.all_connected() && w.server->connections().active_sessions() == 5, "idle sessions stay alive for 2 minutes");
    std::uint64_t pings = 0, echoes = 0;
    for (const auto& c : w.clients) { pings += c->stats().pings_sent; echoes += c->stats().echoes_received; check(c->rtt().has_sample(), "Ping/Echo gives an RTT sample while idle"); }
    check(pings > 5 * 100 && echoes > 5 * 100, "keep-alives were actually exchanged (2 per second per client)");
}

void test_disconnect_is_reliable() {
    ImpairmentConfig imp;
    imp.loss = 0.25; imp.latency = milliseconds(10);  // 25% each way: the documented sustainable limit (see reliability.md)
    World w(imp, 8);
    w.workload_defaults = WorkloadConfig{0, 0, 0, 32, 0};
    for (int i = 0; i < 20; ++i) w.add_client();
    w.run_for(seconds(15));
    check(w.all_connected(), "connected");
    for (auto& c : w.clients) c->disconnect();
    w.run_for(seconds(5));
    bool closed = true;
    for (const auto& c : w.clients) closed = closed && c->state() == ClientState::Closed;
    check(closed, "every client reaches Closed through 25% loss (Disconnect is retried until confirmed)");
    check(w.server->connections().active_sessions() == 0, "the server released every session (each Disconnect was retried until it got through)");
}

void test_client_restart_on_same_endpoint() {
    ImpairmentConfig imp;
    imp.latency = milliseconds(10);
    World w(imp, 9);
    w.workload_defaults.max_per_channel = 50;
    w.workload_defaults.reliable_hz = 25;
    w.add_client();
    w.run_for(seconds(10));
    check(w.workloads[0]->reliable_complete() && w.workloads[0]->channel(ChannelId::Control).sent == 50, "first incarnation completed its workload");
    const SessionId first = w.clients[0]->session_id();
    w.restart_client(0, 0xBEEF);  // process crashed and restarted: same address and port, sequence numbers start over
    w.run_for(seconds(10));
    check(w.clients[0]->connected() && w.clients[0]->session_id() != first, "the restarted client gets a fresh session");
    check(w.server->connections().active_sessions() == 1 && w.server->connections().stats().sessions_replaced == 1, "the old session was replaced, not leaked");
    check(w.workloads[0]->channel(ChannelId::Control).sent == 50 && w.workloads[0]->reliable_complete() && w.violations() == 0,
          "the restarted client's reliable messages (sequence 1, 2, ...) are delivered - no stale sequence state");
}

void test_forged_packets_are_ignored() {
    ImpairmentConfig imp;
    imp.latency = milliseconds(10);
    World w(imp, 10);
    w.workload_defaults.max_per_channel = 100;
    auto& victim = w.add_client();
    w.run_for(seconds(2));
    check(victim.connected(), "connected");
    auto rogue = w.net.create_transport("66.6.6.6");
    rogue->open(27015);  // spoofs the server's port too: only the source *address* tells it apart
    const SessionId sid = victim.session_id();

    auto send_to = [&](const std::string& host, std::uint16_t port, const Packet& p) {
        std::vector<std::uint8_t> bytes;
        serialize(p, bytes);
        rogue->send_to(host, port, bytes);
    };
    Packet bye;
    bye.type = MessageType::Disconnect;
    encode_disconnect_payload(DisconnectMessage{sid, DisconnectReason::ServerShutdown}, bye.payload);
    Packet data;
    data.type = MessageType::ChannelData;
    data.sequence = 1;
    encode_channel_payload(ChannelMessage{sid, ChannelId::Control, {1, 2, 3, 4, 0, 0, 0, 0, 0, 0, 0, 0}}, data.payload);
    // 1) forged "server" packets to the client, 2) forged "client" packets to the server
    for (int i = 0; i < 20; ++i) { send_to(w.transports[0]->address(), w.transports[0]->local_port(), bye); send_to(w.transports[0]->address(), w.transports[0]->local_port(), data); }
    Packet client_bye;
    client_bye.type = MessageType::Disconnect;
    encode_disconnect_payload(DisconnectMessage{sid, DisconnectReason::Requested}, client_bye.payload);
    for (int i = 0; i < 20; ++i) { send_to("10.0.0.1", 27015, client_bye); send_to("10.0.0.1", 27015, data); }

    w.run_for(seconds(15));
    check(victim.connected() && victim.state() == ClientState::Connected, "client ignores a forged Disconnect from a foreign address");
    check(victim.stats().foreign_source_dropped >= 20, "foreign datagrams were counted and dropped");
    check(w.server->connections().has_session(sid), "server session survives forged Disconnect/ChannelData from a foreign endpoint");
    check(w.workloads[0]->reliable_complete() && w.violations() == 0, "and the forged data never reached the application");
}

void test_flood_is_shed_without_hurting_others() {
    ImpairmentConfig imp;
    imp.latency = milliseconds(10);
    World w(imp, 11, /*rate_limits=*/true);
    w.workload_defaults.max_per_channel = 60;
    w.workload_defaults.reliable_hz = 10;
    w.add_client();
    w.run_for(seconds(2));
    auto flooder = w.net.create_transport("99.9.9.9");
    flooder->open(0);
    Packet ping;
    ping.type = MessageType::Ping;
    encode_session_id(424242, ping.payload);  // unknown session: each processed packet would draw an Error reply
    std::vector<std::uint8_t> bytes;
    serialize(ping, bytes);
    for (int i = 0; i < 5000; ++i) flooder->send_to("10.0.0.1", 27015, bytes);
    w.run_for(seconds(10));
    int replies = 0;
    TransportDatagram d;
    while (flooder->receive(d, 0)) ++replies;
    check(w.server->stats().rate_limited > 4000, "the flood was shed by the rate limiter (limited " + std::to_string(w.server->stats().rate_limited) + " of 5000)");
    check(replies < 500, "the flooder received only a bounded number of replies (" + std::to_string(replies) + ")");
    check(w.clients[0]->connected() && w.workloads[0]->reliable_complete() && w.violations() == 0, "a legitimate client is unaffected by the flood");
}

void test_server_shutdown_notifies_clients() {
    ImpairmentConfig imp;
    imp.latency = milliseconds(10);
    World w(imp, 12);
    w.workload_defaults = WorkloadConfig{0, 0, 0, 32, 0};
    for (int i = 0; i < 5; ++i) w.add_client();
    w.run_for(seconds(2));
    w.server->shutdown();
    w.run_for(milliseconds(200));
    bool ok = true;
    for (const auto& c : w.clients) ok = ok && c->state() == ClientState::Closed && c->server_closed() && c->failure_reason() == "server shut down";
    check(ok, "every client learns that the server shut down");
}

void test_backpressure_loses_nothing_accepted() {
    ImpairmentConfig imp;
    imp.loss = 0.2; imp.latency = milliseconds(20);
    World w(imp, 13);
    w.use_workloads = false;
    auto& c = w.add_client();
    w.run_for(seconds(5));
    check(c.connected(), "connected");
    std::uint32_t accepted = 0;
    bool saw_full = false;
    for (std::uint32_t i = 0; i < 10000; ++i) {
        std::vector<std::uint8_t> payload = {static_cast<std::uint8_t>(i >> 24), static_cast<std::uint8_t>(i >> 16), static_cast<std::uint8_t>(i >> 8), static_cast<std::uint8_t>(i)};
        const auto st = c.send(ChannelId::Inventory, payload);
        if (st == SendStatus::Ok) ++accepted; else { saw_full = st == SendStatus::QueueFull; break; }
    }
    check(saw_full && accepted > 100 && accepted < 1000, "a bounded queue pushes back with QueueFull instead of growing forever (accepted " + std::to_string(accepted) + ")");
    std::vector<ChannelMessage> got;
    for (int i = 0; i < 60000 && got.size() < accepted; ++i) { w.step(); c.drain_delivered(got); }
    bool in_order = got.size() == accepted;
    for (std::uint32_t i = 0; i < got.size() && in_order; ++i) {
        const auto& d = got[i].data;
        in_order = d.size() == 4 && ((std::uint32_t{d[0]} << 24) | (std::uint32_t{d[1]} << 16) | (std::uint32_t{d[2]} << 8) | d[3]) == i;
    }
    check(in_order, "all accepted messages came back, in order, none lost");
}

void test_determinism() {
    auto run = [](std::uint64_t seed) {
        World w(hostile(), seed);
        w.workload_defaults.max_per_channel = 40;
        for (int i = 0; i < 4; ++i) w.add_client();
        w.run_for(seconds(40));
        std::vector<std::uint64_t> fingerprint = {w.net.stats().sent, w.net.stats().lost, w.net.stats().duplicated, w.net.stats().delivered,
                                                  w.server->stats().datagrams_received, w.server->stats().packets_sent};
        for (const auto& wl : w.workloads) { fingerprint.push_back(wl->reliable_echoed()); fingerprint.push_back(wl->unreliable_echoed()); }
        return fingerprint;
    };
    check(run(77) == run(77), "the whole system (server + clients + network) is bit-for-bit reproducible for a given seed");
    check(run(77) != run(78), "and different seeds really differ");
}

} // namespace

int main() {
    test_handshake_and_basic_echo();
    test_handshake_survives_loss();
    // DWE_TEST_SEEDS=N runs the hostile-network scenario for N seeds (soak); the default keeps CI fast.
    std::uint64_t seeds = 4;
    if (const char* env = std::getenv("DWE_TEST_SEEDS")) { const long v = std::atol(env); if (v > 0) seeds = static_cast<std::uint64_t>(v); }
    for (std::uint64_t seed = 1; seed <= seeds; ++seed) test_exactly_once_in_order_on_a_hostile_network(seed, 12, 150);
    test_short_outage_is_repaired();
    test_long_outage_fails_cleanly();
    test_keepalive_holds_an_idle_session();
    test_disconnect_is_reliable();
    test_client_restart_on_same_endpoint();
    test_forged_packets_are_ignored();
    test_flood_is_shed_without_hurting_others();
    test_server_shutdown_notifies_clients();
    test_backpressure_loses_nothing_accepted();
    test_determinism();
    if (failures == 0) std::cout << "All DWE reliability integration tests passed.\n";
    return failures == 0 ? EXIT_SUCCESS : EXIT_FAILURE;
}
