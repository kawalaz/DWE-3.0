#include "dwe/core/Cli.h"
#include "dwe/network/ClientConnection.h"
#include "dwe/network/LossyTransport.h"
#include "dwe/network/TransportFactory.h"

#include <chrono>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <cstdint>
#include <utility>
#include <vector>

namespace {
void usage() {
    std::cerr << "Usage: dwe_client <host> <port> [message] [options]\n"
                 "  --transport udp|gns   datagram transport (default udp)\n"
                 "  --drop P              drop a fraction P (0-1) of datagrams in both directions (loss testing)\n"
                 "  --timeout SEC         give up after SEC seconds (default 10)\n";
}
} // namespace

int main(int argc, char** argv) {
    using namespace dwe::core;
    using namespace dwe::network;
    std::string host, message = "hello DWE 0.3.0", transport_name = "udp";
    std::uint16_t port = 0;
    double drop = 0.0;
    std::uint64_t timeout_s = 10;
    int positional = 0;
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        auto value = [&](const char* name) -> const char* {
            if (i + 1 >= argc) { std::cerr << "Client: " << name << " needs a value\n"; usage(); std::exit(2); }
            return argv[++i];
        };
        if (arg == "--transport") transport_name = value("--transport");
        else if (arg == "--drop") {
            const auto v = parse_fraction(value("--drop"), 0.0, 1.0);
            if (!v) { std::cerr << "Client: invalid --drop (expected 0-1)\n"; return 2; }
            drop = *v;
        } else if (arg == "--timeout") {
            const auto v = parse_unsigned(value("--timeout"), 1, 3600);
            if (!v) { std::cerr << "Client: invalid --timeout\n"; return 2; }
            timeout_s = *v;
        } else if (arg == "--help" || arg == "-h") { usage(); return 0; }
        else if (!arg.empty() && arg[0] == '-') { std::cerr << "Client: unknown option " << arg << '\n'; usage(); return 2; }
        else if (positional == 0) { host = arg; ++positional; }
        else if (positional == 1) {
            const auto p = parse_port(arg.c_str());
            if (!p) { std::cerr << "Client: invalid port \"" << arg << "\" (expected 1-65535)\n"; return 2; }
            port = *p;
            ++positional;
        } else if (positional == 2) { message = arg; ++positional; }
        else { std::cerr << "Client: unexpected argument " << arg << '\n'; usage(); return 2; }
    }
    if (positional < 2) { usage(); return 2; }

    std::string error;
    std::unique_ptr<ITransport> transport = create_transport(transport_name, &error);
    if (!transport) { std::cerr << "Client: " << error << '\n'; return 2; }
    if (drop > 0.0) transport = std::make_unique<LossyTransport>(std::move(transport), drop, 2);
    if (!transport->open(0, &error)) { std::cerr << "Client: " << error << '\n'; return 1; }

    ClientConfig config;
    config.host = host;
    config.port = port;
    config.connect_timeout = std::chrono::seconds(timeout_s);
    ClientConnection client(*transport, config);
    client.connect();

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(timeout_s);
    bool sent = false, echoed = false;
    int rc = 1;
    while (std::chrono::steady_clock::now() < deadline && !client.finished()) {
        client.update();
        if (client.connected() && !sent) {
            std::cout << "Connected. Session ID: " << client.session_id() << '\n';
            // Control is a reliable channel: the message is retransmitted until the server acknowledges it.
            const auto status = client.send(ChannelId::Control, std::vector<std::uint8_t>(message.begin(), message.end()));
            if (status != SendStatus::Ok) { std::cerr << "Client: send failed: " << send_status_name(status) << '\n'; return 1; }
            sent = true;
        }
        std::vector<ChannelMessage> got;
        client.drain_delivered(got);
        for (const auto& m : got) {
            const std::string body(m.data.begin(), m.data.end());
            std::cout << "Channel round trip OK | channel=" << channel_name(m.channel) << " (" << delivery_name(channel_delivery(m.channel))
                      << ") | payload=\"" << body << "\"\n";
            echoed = body == message;
        }
        if (echoed) { client.disconnect(); echoed = false; rc = 0; sent = true; }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    if (client.state() == ClientState::Failed) {
        std::cerr << "Client: " << client.failure_reason() << '\n';
        return 1;
    }
    if (rc != 0) {
        std::cerr << (client.connected() ? "Client: timed out waiting for the channel response\n"
                                         : "No response within " + std::to_string(timeout_s) + " seconds. Check server and firewall.\n");
        return 1;
    }
    std::cout << (client.state() == ClientState::Closed ? "Disconnected (confirmed by server).\n" : "Disconnect sent.\n");
    const auto stats = client.channel_stats();
    if (stats.reliable.retransmissions != 0) std::cout << "(reliable channel retransmitted " << stats.reliable.retransmissions << " time(s))\n";
    return 0;
}
