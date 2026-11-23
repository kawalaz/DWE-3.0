// dwe_loadgen - drives hundreds or thousands of DWE clients and verifies the delivery guarantees.
//
//   --mode sim  Server, network and clients all run in this process under a virtual clock and a seeded
//               impairment model (loss / duplication / reordering / latency). Deterministic and fast.
//   --mode udp  Real UDP sockets against a separately started dwe_server (optionally with simulated
//               loss at the client side). Several worker threads, real time.
//
// Each client sends sequence-numbered messages on every channel and checks the echoes against the
// channel's guarantee (exactly once and in order for reliable channels). Exit code 0 = all guarantees held.
#include "dwe/core/Cli.h"
#include "dwe/network/ClientConnection.h"
#include "dwe/network/EchoWorkload.h"
#include "dwe/network/LossyTransport.h"
#include "dwe/network/Server.h"
#include "dwe/network/SimulatedNetwork.h"
#include "dwe/network/TransportFactory.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <ctime>
#include <functional>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>
#ifndef _WIN32
#include <sys/resource.h>
#include <cstddef>
#include <cstdint>
#include <utility>
#endif

using namespace dwe::core;
using namespace dwe::network;
using std::chrono::duration;
using std::chrono::milliseconds;

namespace {

struct Options {
    std::string mode = "sim";
    std::size_t clients = 100;
    double duration_s = 10;
    double ramp_rate = 100;          // clients started per second
    double drain_s = 20;
    std::uint64_t seed = 1;
    WorkloadConfig workload{2.0, 20.0, 10.0, 32, 0};
    // sim
    ImpairmentConfig impairment;
    int tick_ms = 0;                 // 0 = mode default (sim 2 ms, udp 5 ms)
    bool server_rate_limit = true;
    // udp
    std::string host = "127.0.0.1";
    std::uint16_t port = 27015;
    std::size_t threads = 1;
    double drop = 0.0;
    std::string transport = "udp";
    bool require_clean = true;
};

struct Slot {
    std::unique_ptr<ITransport> transport;
    std::unique_ptr<ClientConnection> client;
    std::unique_ptr<EchoWorkload> workload;
    TimePoint start_at{};
    bool started = false;
    bool stopped_sending = false;
    bool disconnect_sent = false;
};

struct Schedule {            // all offsets are relative to the run's start
    double ramp_end_s;
    double send_end_s;
    double drain_end_s;
    double close_end_s;
};

double percentile(std::vector<float>& v, double p) {
    if (v.empty()) return 0.0;
    const std::size_t i = std::min(v.size() - 1, static_cast<std::size_t>(p * static_cast<double>(v.size() - 1) + 0.5));
    std::nth_element(v.begin(), v.begin() + static_cast<std::ptrdiff_t>(i), v.end());
    return static_cast<double>(v[i]);
}

// Advances one group of clients through the phases: ramp -> steady traffic -> drain -> disconnect.
// `step()` moves the world forward by one tick (sim: advance virtual time and run the server;
// udp: sleep). Returns when the schedule is over.
void drive(std::vector<Slot>& slots, const std::function<TimePoint()>& now_fn, const std::function<void()>& step,
           TimePoint t0, const Schedule& sched) {
    const auto at = [&](double s) { return t0 + std::chrono::duration_cast<Duration>(duration<double>(s)); };
    bool sending_stopped = false, disconnected = false;
    for (;;) {
        step();
        const TimePoint now = now_fn();
        for (auto& s : slots) {
            if (!s.started && now >= s.start_at) { s.client->connect(); s.started = true; }
            if (s.started) { s.client->update(); s.workload->tick(now); }
        }
        if (!sending_stopped && now >= at(sched.send_end_s)) {
            for (auto& s : slots) s.workload->stop_sending();
            sending_stopped = true;
        }
        if (sending_stopped && !disconnected) {
            // Drain: wait until every connected client has its reliable echoes back and its sends acknowledged.
            bool all_done = true;
            for (auto& s : slots) {
                if (s.client->connected() && !(s.workload->reliable_complete() && s.client->reliable_idle())) all_done = false;
            }
            if (all_done || now >= at(sched.drain_end_s)) {
                for (auto& s : slots) if (s.client->connected()) s.client->disconnect();
                disconnected = true;
            }
        }
        if (disconnected) {
            bool all_closed = true;
            for (auto& s : slots) if (s.started && !s.client->finished()) all_closed = false;
            if (all_closed || now >= at(sched.close_end_s)) return;
        }
    }
}

struct Totals {
    std::size_t started = 0, connected_ever = 0, failed = 0, still_connecting = 0, closed_cleanly = 0;
    std::uint64_t reliable_sent = 0, reliable_echoed = 0, violations = 0, send_rejected = 0;
    std::uint64_t input_sent = 0, input_echoed = 0, input_dups = 0, snap_sent = 0, snap_echoed = 0, snap_regress = 0;
    std::uint64_t retransmits = 0, fast_retransmits = 0, dup_suppressed = 0, stale_dropped = 0, acks_sent = 0, pings = 0;
    std::uint64_t packets_sent = 0, packets_received = 0, hellos = 0, errors = 0;
    std::vector<float> hs_ms, rel_rtt, unrel_rtt;
    std::vector<std::string> failure_reasons;
};

void collect(const std::vector<Slot>& slots, Totals& t) {
    for (const auto& s : slots) {
        if (!s.started) continue;
        ++t.started;
        const auto& st = s.client->stats();
        t.packets_sent += st.packets_sent; t.packets_received += st.packets_received; t.hellos += st.hellos_sent; t.errors += st.errors_received;
        t.pings += st.pings_sent;
        if (s.client->session_id() != 0) {
            ++t.connected_ever;
            t.hs_ms.push_back(static_cast<float>(duration<double, std::milli>(st.handshake_latency).count()));
        }
        if (s.client->state() == ClientState::Failed) { ++t.failed; t.failure_reasons.push_back(s.client->failure_reason()); }
        else if (s.client->state() == ClientState::Connecting) ++t.still_connecting;
        else if (s.client->state() == ClientState::Closed && !s.client->server_closed()) ++t.closed_cleanly;
        const auto& w = *s.workload;
        t.reliable_sent += w.reliable_sent(); t.reliable_echoed += w.reliable_echoed(); t.violations += w.violations();
        t.input_sent += w.channel(ChannelId::Input).sent; t.input_echoed += w.channel(ChannelId::Input).echoed; t.input_dups += w.channel(ChannelId::Input).duplicates;
        t.snap_sent += w.channel(ChannelId::Snapshot).sent; t.snap_echoed += w.channel(ChannelId::Snapshot).echoed; t.snap_regress += w.channel(ChannelId::Snapshot).regressions;
        for (std::size_t i = 0; i < kChannelCount; ++i) t.send_rejected += w.channel(static_cast<ChannelId>(i)).send_rejected;
        t.rel_rtt.insert(t.rel_rtt.end(), w.reliable_rtt_ms().begin(), w.reliable_rtt_ms().end());
        t.unrel_rtt.insert(t.unrel_rtt.end(), w.unreliable_rtt_ms().begin(), w.unreliable_rtt_ms().end());
        const auto cs = s.client->channel_stats();
        t.retransmits += cs.reliable.retransmissions; t.fast_retransmits += cs.reliable.fast_retransmissions;
        t.dup_suppressed += cs.reliable.duplicates + cs.duplicates_dropped; t.stale_dropped += cs.stale_dropped; t.acks_sent += cs.acks_sent;
    }
}
std::string pct(double part, double whole) {
    char buf[32];
    std::snprintf(buf, sizeof buf, "%.2f%%", whole > 0 ? 100.0 * part / whole : 0.0);
    return buf;
}
std::string ms3(std::vector<float>& v) {
    char buf[96];
    std::snprintf(buf, sizeof buf, "p50 %.1f | p95 %.1f | p99 %.1f | max %.1f ms", percentile(v, 0.50), percentile(v, 0.95), percentile(v, 0.99), percentile(v, 1.0));
    return buf;
}

int report(const Options& o, Totals& t, double wall_s, double sim_or_run_s, const Server* server, const SimulatedNetwork* net, double server_busy_s) {
    std::cout << "\n=== DWE load generator report ===\n";
    std::cout << "mode=" << o.mode << "  clients=" << o.clients << "  steady traffic=" << o.duration_s << "s  seed=" << o.seed
              << "  payload=" << o.workload.payload_bytes << "B\n";
    std::cout << "rates per client: reliable " << o.workload.reliable_hz << " Hz x5 channels, input " << o.workload.input_hz << " Hz, snapshot " << o.workload.snapshot_hz << " Hz\n";
    if (o.mode == "sim") {
        const auto& i = o.impairment;
        std::cout << "network: loss " << i.loss * 100 << "%  dup " << i.duplicate * 100 << "%  reorder " << i.reorder * 100 << "%  latency "
                  << duration<double, std::milli>(i.latency).count() << " ms  jitter " << duration<double, std::milli>(i.jitter).count() << " ms\n";
    } else if (o.drop > 0) {
        std::cout << "client-side simulated drop: " << o.drop * 100 << "% on send and on receive\n";
    }
    std::printf("run time: %.1f s %s, wall %.2f s\n", sim_or_run_s, o.mode == "sim" ? "(virtual)" : "(real)", wall_s);

    std::cout << "\n-- Connections\n";
    std::cout << "  started " << t.started << ", connected " << t.connected_ever << ", failed " << t.failed << ", still connecting " << t.still_connecting
              << ", closed cleanly " << t.closed_cleanly << "\n";
    std::cout << "  handshake: " << ms3(t.hs_ms) << "   (Hello packets sent: " << t.hellos << ")\n";
    if (!t.failure_reasons.empty()) {
        std::cout << "  failure reasons: ";
        std::vector<std::string> seen;
        for (const auto& r : t.failure_reasons) if (std::find(seen.begin(), seen.end(), r) == seen.end()) seen.push_back(r);
        for (const auto& r : seen) std::cout << "'" << r << "' x" << std::count(t.failure_reasons.begin(), t.failure_reasons.end(), r) << "  ";
        std::cout << "\n";
    }
    std::cout << "\n-- Reliable channels (Control, Combat, Inventory, Destruction, WorldSync)\n";
    std::cout << "  sent " << t.reliable_sent << ", echoed back " << t.reliable_echoed << ", missing " << (t.reliable_sent - std::min(t.reliable_sent, t.reliable_echoed))
              << ", rejected by backpressure " << t.send_rejected << "\n  round-trip: " << ms3(t.rel_rtt) << "\n";
    std::cout << "\n-- Unreliable channels (loss is expected here and is NOT repaired)\n";
    std::cout << "  Input    (dedup window)  sent " << t.input_sent << ", echoed " << t.input_echoed << "  -> lost " << pct(static_cast<double>(t.input_sent - std::min(t.input_sent, t.input_echoed)), static_cast<double>(t.input_sent))
              << ", duplicates delivered " << t.input_dups << "\n";
    std::cout << "  Snapshot (newest wins)   sent " << t.snap_sent << ", echoed " << t.snap_echoed << "  -> lost " << pct(static_cast<double>(t.snap_sent - std::min(t.snap_sent, t.snap_echoed)), static_cast<double>(t.snap_sent))
              << ", older-than-newest delivered " << t.snap_regress << "\n  round-trip: " << ms3(t.unrel_rtt) << "\n";

    std::cout << "\n-- Reliability layer (client side" << (server ? " + server" : "") << ")\n";
    std::uint64_t retx = t.retransmits, fast = t.fast_retransmits, dups = t.dup_suppressed, stale = t.stale_dropped, acks = t.acks_sent;
    if (server) {
        const auto sc = server->connections().aggregate_channel_stats();
        retx += sc.reliable.retransmissions; fast += sc.reliable.fast_retransmissions;
        dups += sc.reliable.duplicates + sc.duplicates_dropped; stale += sc.stale_dropped; acks += sc.acks_sent;
    }
    std::cout << "  retransmissions " << retx << " (fast " << fast << "), duplicates suppressed " << dups << ", stale dropped " << stale << ", ack packets " << acks << ", keep-alive pings " << t.pings << "\n";
    std::cout << "  client packets: sent " << t.packets_sent << ", received " << t.packets_received << "  (" << static_cast<std::uint64_t>(static_cast<double>(t.packets_sent) / std::max(1.0, sim_or_run_s)) << " pkt/s sent overall)\n";

    if (server) {
        const auto& ms = server->connections().stats();
        const auto& ss = server->stats();
        std::cout << "\n-- Server (in process)\n";
        std::cout << "  sessions: created " << ms.sessions_created << ", expired " << ms.sessions_expired << ", failed " << ms.sessions_failed << ", rejected (full) " << ms.rejected_full
                  << ", active at end " << server->connections().active_sessions() << "\n";
        std::cout << "  datagrams rx " << ss.datagrams_received << ", tx " << ss.packets_sent << ", malformed " << ss.malformed << ", rate-limited " << ss.rate_limited
                  << " (Hello-limited " << ms.rate_limited_hellos << ")\n";
        std::printf("  server CPU: %.2f s busy for %.1f s of simulated time = %.1f%% of one core\n", server_busy_s, sim_or_run_s, 100.0 * server_busy_s / std::max(0.001, sim_or_run_s));
    }
    if (net) {
        const auto& ns = net->stats();
        std::cout << "\n-- Simulated network\n  datagrams sent " << ns.sent << ", lost " << ns.lost << " (" << pct(static_cast<double>(ns.lost), static_cast<double>(ns.sent)) << "), duplicated " << ns.duplicated
                  << ", delayed for reordering " << ns.delayed_for_reorder << ", delivered " << ns.delivered << "\n";
    }

    std::vector<std::string> problems;
    if (t.violations != 0) problems.push_back(std::to_string(t.violations) + " delivery-guarantee violation(s) (duplicate / out-of-order / regressed)");
    if (t.reliable_echoed != t.reliable_sent) problems.push_back(std::to_string(t.reliable_sent - std::min(t.reliable_sent, t.reliable_echoed)) + " reliable message(s) never came back");
    if (t.connected_ever != o.clients) problems.push_back(std::to_string(o.clients - t.connected_ever) + " client(s) never connected");
    if (t.failed != 0) problems.push_back(std::to_string(t.failed) + " client connection(s) failed");
    std::cout << "\nVERDICT: ";
    if (problems.empty()) { std::cout << "PASS - every client connected and all delivery guarantees held\n"; return 0; }
    std::cout << "FAIL\n";
    for (const auto& p : problems) std::cout << "  - " << p << "\n";
    return o.require_clean ? 1 : 0;
}

void usage() {
    std::cerr <<
        "Usage: dwe_loadgen [--mode sim|udp] [options]\n"
        " common:  --clients N (100)  --duration SEC (10)  --ramp-rate N/s (100)  --drain SEC (20)  --seed S (1)\n"
        "          --reliable-hz H (2, per reliable channel)  --input-hz H (20)  --snapshot-hz H (10)  --payload B (32)\n"
        "          --tick-ms N (sim 2, udp 5)   --no-require-clean (always exit 0)\n"
        " sim:     --loss P  --dup P  --reorder P (0-1)  --latency MS  --jitter MS   --no-server-rate-limit\n"
        " udp:     --host H (127.0.0.1)  --port P (27015)  --threads N (1)  --drop P  --transport udp|gns\n";
}

} // namespace

int main(int argc, char** argv) {
    Options o;
    for (int i = 1; i < argc; ++i) {
        const std::string a = argv[i];
        auto val = [&]() -> const char* {
            if (i + 1 >= argc) { std::cerr << "dwe_loadgen: " << a << " needs a value\n"; usage(); std::exit(2); }
            return argv[++i];
        };
        auto need = [&](bool ok) { if (!ok) { std::cerr << "dwe_loadgen: invalid value for " << a << "\n"; usage(); std::exit(2); } };
        if (a == "--mode") { o.mode = val(); need(o.mode == "sim" || o.mode == "udp"); }
        else if (a == "--clients") { auto v = parse_unsigned(val(), 1, 100000); need(v.has_value()); o.clients = static_cast<std::size_t>(*v); }
        else if (a == "--duration") { auto v = parse_fraction(val(), 0.1, 86400); need(v.has_value()); o.duration_s = *v; }
        else if (a == "--ramp-rate") { auto v = parse_fraction(val(), 0.1, 1000000); need(v.has_value()); o.ramp_rate = *v; }
        else if (a == "--drain") { auto v = parse_fraction(val(), 0.0, 86400); need(v.has_value()); o.drain_s = *v; }
        else if (a == "--seed") { auto v = parse_unsigned(val(), 0, ~0ULL >> 1); need(v.has_value()); o.seed = *v; }
        else if (a == "--reliable-hz") { auto v = parse_fraction(val(), 0.0, 10000); need(v.has_value()); o.workload.reliable_hz = *v; }
        else if (a == "--input-hz") { auto v = parse_fraction(val(), 0.0, 10000); need(v.has_value()); o.workload.input_hz = *v; }
        else if (a == "--snapshot-hz") { auto v = parse_fraction(val(), 0.0, 10000); need(v.has_value()); o.workload.snapshot_hz = *v; }
        else if (a == "--payload") { auto v = parse_unsigned(val(), 12, kMaxChannelDataSize); need(v.has_value()); o.workload.payload_bytes = static_cast<std::size_t>(*v); }
        else if (a == "--tick-ms") { auto v = parse_unsigned(val(), 1, 1000); need(v.has_value()); o.tick_ms = static_cast<int>(*v); }
        else if (a == "--loss") { auto v = parse_fraction(val(), 0, 1); need(v.has_value()); o.impairment.loss = *v; }
        else if (a == "--dup") { auto v = parse_fraction(val(), 0, 1); need(v.has_value()); o.impairment.duplicate = *v; }
        else if (a == "--reorder") { auto v = parse_fraction(val(), 0, 1); need(v.has_value()); o.impairment.reorder = *v; }
        else if (a == "--latency") { auto v = parse_fraction(val(), 0, 10000); need(v.has_value()); o.impairment.latency = std::chrono::duration_cast<Duration>(duration<double, std::milli>(*v)); }
        else if (a == "--jitter") { auto v = parse_fraction(val(), 0, 10000); need(v.has_value()); o.impairment.jitter = std::chrono::duration_cast<Duration>(duration<double, std::milli>(*v)); }
        else if (a == "--no-server-rate-limit") o.server_rate_limit = false;
        else if (a == "--no-require-clean") o.require_clean = false;
        else if (a == "--host") o.host = val();
        else if (a == "--port") { auto v = parse_port(val()); need(v.has_value()); o.port = *v; }
        else if (a == "--threads") { auto v = parse_unsigned(val(), 1, 256); need(v.has_value()); o.threads = static_cast<std::size_t>(*v); }
        else if (a == "--drop") { auto v = parse_fraction(val(), 0, 1); need(v.has_value()); o.drop = *v; }
        else if (a == "--transport") o.transport = val();
        else if (a == "--help" || a == "-h") { usage(); return 0; }
        else { std::cerr << "dwe_loadgen: unknown option " << a << "\n"; usage(); return 2; }
    }
    if (o.workload.payload_bytes < 12) o.workload.payload_bytes = 12;
    if (o.tick_ms == 0) o.tick_ms = o.mode == "sim" ? 2 : 5;
    const Duration tick = milliseconds(o.tick_ms);

    const double ramp_end = static_cast<double>(o.clients) / o.ramp_rate;
    Schedule sched{ramp_end, ramp_end + o.duration_s, ramp_end + o.duration_s + o.drain_s, ramp_end + o.duration_s + o.drain_s + 4.0};
    const auto wall0 = std::chrono::steady_clock::now();
    const std::clock_t cpu0 = std::clock();
    Totals totals;

    if (o.mode == "sim") {
        SimulatedNetwork net(o.impairment, o.seed);
        ServerConfig sc;
        sc.port = 27015;
        sc.receive_timeout_ms = 0;
        sc.connection.max_sessions = o.clients + 64;
        if (!o.server_rate_limit) sc.connection.rate = RateLimitConfig::unlimited();
        Server server(net.create_transport("10.0.0.1"), sc, [&] { return net.now(); });
        std::string error;
        if (!server.start(&error)) { std::cerr << "dwe_loadgen: " << error << "\n"; return 1; }

        std::vector<Slot> slots(o.clients);
        const TimePoint t0 = net.now();
        for (std::size_t i = 0; i < o.clients; ++i) {
            auto& s = slots[i];
            auto transport = net.create_transport("10." + std::to_string(1 + i / 65000) + "." + std::to_string((i / 250) % 256) + "." + std::to_string(i % 250 + 1));
            if (!transport->open(0, &error)) { std::cerr << "dwe_loadgen: " << error << "\n"; return 1; }
            ClientConfig cc;
            cc.host = "10.0.0.1";
            cc.port = 27015;
            cc.nonce = o.seed * 1000003ULL + i + 1;
            cc.connect_timeout = std::chrono::seconds(30);
            s.transport = std::move(transport);
            s.client = std::make_unique<ClientConnection>(*s.transport, cc, [&net] { return net.now(); });
            s.workload = std::make_unique<EchoWorkload>(*s.client, o.workload);
            s.start_at = t0 + std::chrono::duration_cast<Duration>(duration<double>(static_cast<double>(i) / o.ramp_rate));
        }
        double server_busy = 0.0;
        auto step = [&] {
            net.advance(tick);
            const auto a = std::chrono::steady_clock::now();
            server.poll();
            server_busy += duration<double>(std::chrono::steady_clock::now() - a).count();
        };
        drive(slots, [&] { return net.now(); }, step, t0, sched);
        const double sim_s = duration<double>(net.now() - t0).count();
        collect(slots, totals);
        const double wall = duration<double>(std::chrono::steady_clock::now() - wall0).count();
        return report(o, totals, wall, sim_s, &server, &net, server_busy);
    }

    // ------------------------------------------------------------------ udp mode
#ifndef _WIN32
    {   // Thousands of sockets need thousands of file descriptors.
        rlimit rl{};
        if (getrlimit(RLIMIT_NOFILE, &rl) == 0 && rl.rlim_cur < rl.rlim_max) { rl.rlim_cur = rl.rlim_max; setrlimit(RLIMIT_NOFILE, &rl); }
        if (getrlimit(RLIMIT_NOFILE, &rl) == 0 && rl.rlim_cur < o.clients + 64) {
            std::cerr << "dwe_loadgen: file descriptor limit " << rl.rlim_cur << " is too low for " << o.clients << " clients (raise it with ulimit -n)\n";
            return 1;
        }
    }
#endif
    if (!transport_available(o.transport)) { std::string e; create_transport(o.transport, &e); std::cerr << "dwe_loadgen: " << e << "\n"; return 2; }
    const TimePoint t0 = Clock::now() + std::chrono::milliseconds(50);
    const std::size_t nthreads = std::min(o.threads, o.clients);
    std::vector<std::vector<Slot>> groups(nthreads);
    for (std::size_t i = 0; i < o.clients; ++i) {
        Slot s;
        std::string error;
        auto raw = create_transport(o.transport, &error);
        if (!raw || !raw->open(0, &error)) { std::cerr << "dwe_loadgen: cannot open client socket #" << i << ": " << error << "\n"; return 1; }
        s.transport = o.drop > 0 ? std::unique_ptr<ITransport>(std::make_unique<LossyTransport>(std::move(raw), o.drop, o.seed * 7919 + i)) : std::move(raw);
        ClientConfig cc;
        cc.host = o.host;
        cc.port = o.port;
        cc.nonce = (static_cast<std::uint64_t>(std::chrono::steady_clock::now().time_since_epoch().count()) << 20) ^ (i + 1) ^ (o.seed << 40);
        if (cc.nonce == 0) cc.nonce = 1;
        cc.connect_timeout = std::chrono::seconds(30);
        s.client = std::make_unique<ClientConnection>(*s.transport, cc);
        s.workload = std::make_unique<EchoWorkload>(*s.client, o.workload);
        s.start_at = t0 + std::chrono::duration_cast<Duration>(duration<double>(static_cast<double>(i) / o.ramp_rate));
        groups[i % nthreads].push_back(std::move(s));
    }
    std::vector<std::thread> workers;
    for (std::size_t g = 0; g < nthreads; ++g) {
        workers.emplace_back([&, g] {
            drive(groups[g], [] { return Clock::now(); }, [&] { std::this_thread::sleep_for(tick); }, t0, sched);
        });
    }
    for (auto& w : workers) w.join();
    for (auto& g : groups) collect(g, totals);
    const double wall = duration<double>(std::chrono::steady_clock::now() - wall0).count();
    const double cpu = static_cast<double>(std::clock() - cpu0) / CLOCKS_PER_SEC;
    const int rc = report(o, totals, wall, wall, nullptr, nullptr, 0.0);
    std::printf("\nload generator CPU: %.2f s over %.1f s wall (%zu thread(s)). Server-side numbers are in the dwe_server shutdown statistics.\n", cpu, wall, nthreads);
    return rc;
}
