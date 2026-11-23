// Contract every ITransport implementation must satisfy, plus a complete DWE session over it.
// Runs for every transport that is compiled in: "udp" always, "gns" when built with DWE_WITH_GNS.
// This is the test to run on a machine that has GameNetworkingSockets: `ctest -R transport_contract`.
//
// Datagram semantics are "may be lost", so - exactly like the DWE Hello - the test repeats a send
// until the other side has seen it. (A connection-oriented transport may drop datagrams sent while
// its connection is still being established.)
#include "dwe/network/ClientConnection.h"
#include "dwe/network/Server.h"
#include "dwe/network/TransportFactory.h"

#include <chrono>
#include <cstdlib>
#include <iostream>
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

// Sends `payload` from `from` to (host, port) every 50 ms until `to` receives it; returns the datagram.
bool deliver(ITransport& from, ITransport& to, const std::string& host, std::uint16_t port,
             const std::vector<std::uint8_t>& payload, TransportDatagram& out, std::chrono::milliseconds limit = 8s) {
    const auto end = std::chrono::steady_clock::now() + limit;
    std::string error;
    while (std::chrono::steady_clock::now() < end) {
        from.send_to(host, port, payload, &error);
        const auto slice_end = std::chrono::steady_clock::now() + 50ms;
        while (std::chrono::steady_clock::now() < slice_end) {
            TransportDatagram d;
            if (to.receive(d, 5, &error) && d.bytes == payload) { out = std::move(d); return true; }
        }
    }
    return false;
}

void run_contract(const std::string& kind, std::uint16_t port) {
    const std::string tag = " [" + kind + "]";
    std::string error;

    // ---- raw datagram contract
    {
        auto server = create_transport(kind, &error);
        auto client = create_transport(kind, &error);
        check(server && client, "transport can be created" + tag + " " + error);
        if (!server || !client) return;
        check(server->open(port, &error), "server opens on a fixed port" + tag + ": " + error);
        check(client->open(0, &error), "client opens" + tag + ": " + error);
        check(server->local_port() == port, "server reports its port" + tag);

        TransportDatagram d;
        check(!server->receive(d, 50, &error) && error.empty(), "a timeout is 'false with an empty error'" + tag);

        const std::vector<std::uint8_t> ping = {'D', 'W', 'E', 1, 2, 3, 4, 5};
        TransportDatagram at_server;
        check(deliver(*client, *server, "127.0.0.1", port, ping, at_server), "client -> server datagram arrives intact" + tag);
        check(!at_server.address.empty() && at_server.port != 0, "the receiver is told who sent it (address and port)" + tag);

        // The server answers to exactly the address:port it was given - that is how DWE replies.
        TransportDatagram at_client;
        const std::vector<std::uint8_t> pong = {9, 8, 7, 6};
        const auto end = std::chrono::steady_clock::now() + 8s;
        bool got = false;
        while (!got && std::chrono::steady_clock::now() < end) {
            server->send_to(at_server.address, at_server.port, pong, &error);
            for (int i = 0; i < 10 && !got; ++i) { TransportDatagram d2; if (client->receive(d2, 5, &error) && d2.bytes == pong) { at_client = d2; got = true; } }
        }
        check(got, "server -> client reply to the reported source address arrives" + tag);
        check(got && at_client.port == port, "the client sees the server's port as the source" + tag);

        // Largest DWE datagram.
        std::vector<std::uint8_t> big(kMaxDatagramSize);
        for (std::size_t i = 0; i < big.size(); ++i) big[i] = static_cast<std::uint8_t>(i * 31);
        TransportDatagram big_at_server;
        check(deliver(*client, *server, "127.0.0.1", port, big, big_at_server), "a maximum-size (1200 byte) datagram arrives intact" + tag);

        // After close() the object can be opened again.
        client->close();
        check(client->open(0, &error), "a closed transport can be reopened" + tag + ": " + error);
    }

    // ---- a complete DWE session over this transport
    {
        ServerConfig cfg;
        cfg.port = static_cast<std::uint16_t>(port + 1);
        cfg.receive_timeout_ms = 0;
        cfg.connection.rate = RateLimitConfig::unlimited();
        auto server_transport = create_transport(kind, &error);
        auto client_transport = create_transport(kind, &error);
        if (!server_transport || !client_transport) { check(false, "create transports for the session test" + tag); return; }
        Server server(std::move(server_transport), cfg);
        check(server.start(&error), "Server starts over the transport" + tag + ": " + error);
        check(client_transport->open(0, &error), "client transport opens" + tag + ": " + error);

        ClientConfig cc;
        cc.host = "127.0.0.1";
        cc.port = cfg.port;
        cc.connect_timeout = 10s;
        ClientConnection client(*client_transport, cc);
        client.connect();
        auto pump = [&](std::chrono::milliseconds limit, auto done) {
            const auto end = std::chrono::steady_clock::now() + limit;
            while (std::chrono::steady_clock::now() < end) {
                server.poll();
                client.update();
                if (done()) return true;
                std::this_thread::sleep_for(1ms);
            }
            return done();
        };
        check(pump(10s, [&] { return client.connected(); }), "DWE handshake completes" + tag);
        for (std::size_t i = 0; i < kChannelCount; ++i)
            client.send(static_cast<ChannelId>(i), std::vector<std::uint8_t>{static_cast<std::uint8_t>(i), 7});
        std::vector<ChannelMessage> got;
        pump(10s, [&] { client.drain_delivered(got); return got.size() >= kChannelCount; });
        check(got.size() == kChannelCount, "one echo per channel comes back" + tag + " (got " + std::to_string(got.size()) + ")");
        check(pump(5s, [&] { return client.reliable_idle(); }), "reliable messages are acknowledged" + tag);
        client.disconnect();
        check(pump(5s, [&] { return client.state() == ClientState::Closed; }), "graceful disconnect" + tag);
    }
}
}

int main() {
    // Pick a port range that differs between runs so parallel test runs do not collide.
    const auto seed = static_cast<unsigned long long>(std::chrono::steady_clock::now().time_since_epoch().count());
    const auto base = static_cast<std::uint16_t>(30000 + (seed / 1000) % 20000);
    std::uint16_t port = base;
    int ran = 0;
    for (const char* kind : {"udp", "gns"}) {
        if (!transport_available(kind)) {
            std::cout << "transport '" << kind << "' is not compiled into this build: skipped\n";
            continue;
        }
        std::cout << "running transport contract for '" << kind << "'\n";
        run_contract(kind, port);
        port = static_cast<std::uint16_t>(port + 4);
        ++ran;
    }
    check(ran >= 1, "at least one transport was tested");
    if (failures == 0) std::cout << "DWE transport contract tests passed (" << ran << " transport(s)).\n";
    return failures == 0 ? EXIT_SUCCESS : EXIT_FAILURE;
}
