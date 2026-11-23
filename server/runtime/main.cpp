#include "dwe/core/Cli.h"
#include "dwe/network/LossyTransport.h"
#include "dwe/network/Server.h"
#include "dwe/network/TransportFactory.h"

#include <atomic>
#include <csignal>
#include <iostream>
#include <memory>
#include <string>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <utility>

namespace {
std::atomic<bool> g_stop{false};
extern "C" void on_signal(int) { g_stop = true; }

void usage() {
    std::cerr <<
        "Usage: dwe_server [port] [max-sessions] [options]\n"
        "  --transport udp|gns     datagram transport (default udp; gns needs a DWE_WITH_GNS build)\n"
        "  --verbose               log every packet-level event\n"
        "  --no-echo               do not echo application messages back\n"
        "  --rate-pps N            per-endpoint packet limit, packets/s (default 200; burst = 2x)\n"
        "  --rate-kbps N           per-endpoint byte limit, KiB/s (default 256; burst = 2x)\n"
        "  --no-rate-limit         disable all rate limiting (not for production)\n"
        "  --drop P                drop a fraction P (0-1) of datagrams in both directions (loss testing)\n"
        "  --stats-interval SEC    print a statistics line every SEC seconds (default: only at shutdown)\n";
}

void print_stats(const dwe::network::Server& server) {
    const auto& s = server.stats();
    const auto& m = server.connections().stats();
    const auto c = server.connections().aggregate_channel_stats();
    std::cout << "stats: sessions=" << server.connections().active_sessions()
              << " created=" << m.sessions_created << " expired=" << m.sessions_expired << " failed=" << m.sessions_failed
              << " | rx=" << s.datagrams_received << " tx=" << s.packets_sent
              << " malformed=" << s.malformed << " rate_limited=" << s.rate_limited
              << " (hello " << m.rate_limited_hellos << ") | delivered=" << s.messages_delivered
              << " retransmits=" << c.reliable.retransmissions << " (fast " << c.reliable.fast_retransmissions << ")"
              << " dup_dropped=" << (c.reliable.duplicates + c.duplicates_dropped)
              << " stale_dropped=" << c.stale_dropped << std::endl;
}
} // namespace

int main(int argc, char** argv) {
    using namespace dwe::core;
    dwe::network::ServerConfig config;
    std::string transport_name = "udp";
    bool verbose = false, rate_limit = true;
    double drop = 0.0;
    std::uint64_t stats_interval = 0, pps = 200, kbps = 256;

    int positional = 0;
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        auto value = [&](const char* name) -> const char* {
            if (i + 1 >= argc) { std::cerr << "DWE server: " << name << " needs a value\n"; usage(); std::exit(2); }
            return argv[++i];
        };
        if (arg == "--transport") transport_name = value("--transport");
        else if (arg == "--verbose") verbose = true;
        else if (arg == "--no-echo") config.echo = false;
        else if (arg == "--no-rate-limit") rate_limit = false;
        else if (arg == "--rate-pps") {
            const auto v = parse_unsigned(value("--rate-pps"), 1, 10000000);
            if (!v) { std::cerr << "DWE server: invalid --rate-pps\n"; return 2; }
            pps = *v;
        } else if (arg == "--rate-kbps") {
            const auto v = parse_unsigned(value("--rate-kbps"), 2, 100000000);  // burst must fit one 1200-byte datagram
            if (!v) { std::cerr << "DWE server: invalid --rate-kbps (minimum 2)\n"; return 2; }
            kbps = *v;
        } else if (arg == "--drop") {
            const auto v = parse_fraction(value("--drop"), 0.0, 1.0);
            if (!v) { std::cerr << "DWE server: invalid --drop (expected 0-1)\n"; return 2; }
            drop = *v;
        } else if (arg == "--stats-interval") {
            const auto v = parse_unsigned(value("--stats-interval"), 1, 86400);
            if (!v) { std::cerr << "DWE server: invalid --stats-interval\n"; return 2; }
            stats_interval = *v;
        } else if (arg == "--help" || arg == "-h") { usage(); return 0; }
        else if (!arg.empty() && arg[0] == '-') { std::cerr << "DWE server: unknown option " << arg << '\n'; usage(); return 2; }
        else if (positional == 0) {
            const auto port = parse_port(arg.c_str());
            if (!port) { std::cerr << "DWE server: invalid port \"" << arg << "\" (expected 1-65535)\n"; usage(); return 2; }
            config.port = *port;
            ++positional;
        } else if (positional == 1) {
            const auto v = parse_unsigned(arg.c_str(), 1, 1000000);
            if (!v) { std::cerr << "DWE server: invalid max-sessions (expected 1-1000000)\n"; return 2; }
            config.connection.max_sessions = static_cast<std::size_t>(*v);
            ++positional;
        } else { std::cerr << "DWE server: unexpected argument " << arg << '\n'; usage(); return 2; }
    }
    if (rate_limit) {
        config.connection.rate.packets = {static_cast<double>(pps), 2.0 * static_cast<double>(pps), true};
        config.connection.rate.bytes = {static_cast<double>(kbps) * 1024.0, 2.0 * static_cast<double>(kbps) * 1024.0, true};
    } else {
        config.connection.rate = dwe::network::RateLimitConfig::unlimited();
    }

    std::string error;
    std::unique_ptr<dwe::network::ITransport> transport = dwe::network::create_transport(transport_name, &error);
    if (!transport) { std::cerr << "DWE server: " << error << '\n'; return 2; }
    if (drop > 0.0) transport = std::make_unique<dwe::network::LossyTransport>(std::move(transport), drop, 1);

    dwe::network::Server server(std::move(transport), config);
    server.set_verbose(verbose);
    server.set_log([](const std::string& line) { std::cout << line << std::endl; });
    if (!server.start(&error)) { std::cerr << "DWE server: " << error << '\n'; return 1; }

    std::signal(SIGINT, on_signal);
    std::signal(SIGTERM, on_signal);
    std::cout << "DWE Foundation 0.3.0 server listening on " << transport_name << " port " << server.port()
              << " | max sessions: " << config.connection.max_sessions
              << " | rate limit: " << (rate_limit ? std::to_string(pps) + " pkt/s, " + std::to_string(kbps) + " KiB/s per endpoint" : std::string("off"))
              << (drop > 0.0 ? " | simulated loss: " + std::to_string(drop) : std::string())
              << "\nPress Ctrl+C to stop." << std::endl;

    auto last_stats = std::chrono::steady_clock::now();
    while (!g_stop) {
        server.poll();
        if (stats_interval != 0 && std::chrono::steady_clock::now() - last_stats >= std::chrono::seconds(stats_interval)) {
            last_stats = std::chrono::steady_clock::now();
            print_stats(server);
        }
    }
    std::cout << "Shutting down: notifying " << server.connections().active_sessions() << " session(s)..." << std::endl;
    server.shutdown();
    print_stats(server);
    return 0;
}
