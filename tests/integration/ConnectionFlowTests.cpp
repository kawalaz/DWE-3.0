// The same Server and ClientConnection as in the simulated tests, but over real UDP sockets on the
// loopback interface (single thread: the test alternates server.poll() and client.update()).
#include "dwe/network/ClientConnection.h"
#include "dwe/network/EchoWorkload.h"
#include "dwe/network/LossyTransport.h"
#include "dwe/network/Server.h"
#include "dwe/network/UdpTransport.h"

#include <chrono>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <utility>

using namespace dwe::core;
using namespace dwe::network;
using namespace std::chrono_literals;

namespace {
int failures = 0;
void check(bool condition, const std::string& message) {
    if (!condition) {
        std::cerr << "FAIL: " << message << '\n';
        ++failures;
    }
}

struct Run {
    Server* server;
    std::vector<ClientConnection*> clients;
    bool pump_until(std::chrono::milliseconds limit, const std::function<bool()>& done) {
        const auto end = std::chrono::steady_clock::now() + limit;
        while (std::chrono::steady_clock::now() < end) {
            server->poll();
            for (auto* c : clients) c->update();
            if (done()) return true;
            std::this_thread::sleep_for(1ms);
        }
        return done();
    }
};

std::unique_ptr<Server> make_server(double drop, std::uint64_t seed) {
    ServerConfig cfg;
    cfg.port = 0;  // ephemeral
    cfg.receive_timeout_ms = 0;
    cfg.connection.rate = RateLimitConfig::unlimited();
    std::unique_ptr<ITransport> t = std::make_unique<UdpTransport>();
    if (drop > 0.0) t = std::make_unique<LossyTransport>(std::move(t), drop, seed);
    auto server = std::make_unique<Server>(std::move(t), cfg);
    std::string error;
    check(server->start(&error), "server starts on an ephemeral UDP port: " + error);
    return server;
}
}

int main() {
    // --- 1. Clean loopback: handshake, one message on every channel, graceful disconnect.
    {
        auto server = make_server(0.0, 1);
        UdpTransport transport;
        std::string error;
        check(transport.open(0, &error), "client socket opens");
        ClientConfig cfg;
        cfg.host = "127.0.0.1";
        cfg.port = server->port();
        ClientConnection client(transport, cfg);
        client.connect();
        Run run{server.get(), {&client}};
        check(run.pump_until(3000ms, [&] { return client.connected(); }), "handshake over real UDP");
        for (std::size_t i = 0; i < kChannelCount; ++i)
            check(client.send(static_cast<ChannelId>(i), std::vector<std::uint8_t>{static_cast<std::uint8_t>(i), 42}) == SendStatus::Ok, "send");
        std::vector<ChannelMessage> got;
        run.pump_until(3000ms, [&] { client.drain_delivered(got); return got.size() >= kChannelCount; });
        check(got.size() == kChannelCount, "one echo per channel arrives over loopback (got " + std::to_string(got.size()) + ")");
        for (const auto& m : got) check(m.data.size() == 2 && m.data[1] == 42, "payload intact");
        check(run.pump_until(2000ms, [&] { return client.reliable_idle(); }), "reliable messages acknowledged");
        client.disconnect();
        check(run.pump_until(3000ms, [&] { return client.state() == ClientState::Closed; }), "Disconnect confirmed by the server");
        run.pump_until(100ms, [] { return false; });
        check(server->connections().active_sessions() == 0, "server session released");
    }

    // --- 2. ~25% loss per direction on a real UDP path (13.4% dropped at each end, applied on send and
    //        on receive: 1 - 0.866^2 = 25%): reliable channels must still be exact.
    {
        auto server = make_server(0.134, 7);
        UdpTransport raw;
        std::string error;
        check(raw.open(0, &error), "client socket opens");
        LossyTransport transport(raw, 0.134, 8);
        ClientConfig cfg;
        cfg.host = "127.0.0.1";
        cfg.port = server->port();
        cfg.connect_timeout = 20s;
        ClientConnection client(transport, cfg);
        client.connect();
        WorkloadConfig wl;
        wl.reliable_hz = 100;   // 5 channels x 100/s
        wl.input_hz = 50;
        wl.snapshot_hz = 50;
        wl.max_per_channel = 60;
        EchoWorkload workload(client, wl);
        Run run{server.get(), {&client}};
        const bool ok = run.pump_until(60s, [&] {
            workload.tick(Clock::now());
            return client.connected() && workload.channel(ChannelId::Control).sent == 60 && workload.reliable_complete();
        });
        check(ok, "all 300 reliable messages came back over a 25%-per-direction-loss real UDP path");
        check(workload.violations() == 0, "none duplicated, reordered or regressed");
        check(workload.reliable_sent() == 300, "the full workload was sent (no vacuous pass)");
        check(client.channel_stats().reliable.retransmissions > 0 || server->connections().aggregate_channel_stats().reliable.retransmissions > 0,
              "loss forced real retransmissions");
        check(transport.dropped() > 0, "datagrams really were dropped");
    }

    // --- 3. A client that talks to nobody gives up with a clear reason instead of hanging.
    {
        UdpTransport transport;
        std::string error;
        check(transport.open(0, &error), "client socket opens");
        ClientConfig cfg;
        cfg.host = "127.0.0.1";
        cfg.port = 9;  // discard port: nothing answers
        cfg.connect_timeout = 800ms;
        ClientConnection client(transport, cfg);
        client.connect();
        const auto end = std::chrono::steady_clock::now() + 3s;
        while (std::chrono::steady_clock::now() < end && !client.finished()) { client.update(); std::this_thread::sleep_for(1ms); }
        check(client.state() == ClientState::Failed && client.failure_reason() == "handshake timed out", "unanswered Hello ends in Failed: handshake timed out");
        check(client.stats().hellos_sent >= 2, "the Hello was retransmitted before giving up");
    }

    if (failures == 0) std::cout << "DWE real-UDP integration flow passed.\n";
    return failures == 0 ? EXIT_SUCCESS : EXIT_FAILURE;
}
