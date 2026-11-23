#include "dwe/network/SimulatedNetwork.h"

#include <cstdlib>
#include <iostream>
#include <string>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <vector>

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

// Sends `n` one-byte datagrams a->b, advancing time 1 ms per datagram; returns what b received.
std::vector<int> run(const ImpairmentConfig& cfg, std::uint64_t seed, int n) {
    SimulatedNetwork net(cfg, seed);
    auto a = net.create_transport("10.0.0.1");
    auto b = net.create_transport("10.0.0.2");
    a->open(0);
    b->open(7000);
    std::vector<int> got;
    TransportDatagram d;
    for (int i = 0; i < n; ++i) {
        a->send_to("10.0.0.2", 7000, {static_cast<std::uint8_t>(i % 256)});
        net.advance(milliseconds(1));
        while (b->receive(d, 0)) got.push_back(d.bytes[0]);
    }
    net.advance(std::chrono::seconds(10));
    while (b->receive(d, 0)) got.push_back(d.bytes[0]);
    return got;
}
}

int main() {
    // Basic delivery with latency, virtual time only.
    {
        ImpairmentConfig cfg;
        cfg.latency = milliseconds(30);
        SimulatedNetwork net(cfg, 1);
        auto a = net.create_transport("10.0.0.1");
        auto b = net.create_transport("10.0.0.2");
        std::string error;
        check(a->open(0, &error) && a->local_port() >= 49152, "ephemeral port assigned");
        check(b->open(7000, &error) && b->local_port() == 7000, "explicit port bound");
        check(a->send_to("10.0.0.2", 7000, {1, 2, 3}), "send");
        TransportDatagram d;
        check(!b->receive(d, 0, &error) && error.empty(), "nothing is delivered before the latency has elapsed");
        net.advance(milliseconds(29));
        check(!b->receive(d, 0), "still in flight after 29 ms");
        net.advance(milliseconds(1));
        check(b->receive(d, 0) && d.bytes == std::vector<std::uint8_t>({1, 2, 3}), "delivered after exactly 30 ms");
        check(d.address == "10.0.0.1" && d.port == a->local_port(), "sender address and port reported");
        check(!b->receive(d, 0), "delivered once");
        check(net.in_transit() == 0, "nothing left in transit");

        // Bind conflicts, closed sockets, unbound destinations.
        auto c = net.create_transport("10.0.0.2");
        check(!c->open(7000, &error) && !error.empty(), "binding an address in use fails");
        check(!c->send_to("10.0.0.1", 1, {1}, &error), "send on a socket that is not open fails");
        a->send_to("10.0.0.9", 1234, {1});
        check(net.stats().undeliverable == 1, "datagram to nobody is silently lost (UDP semantics)");
        b->close();
        a->send_to("10.0.0.2", 7000, {1});
        check(net.stats().undeliverable == 2, "datagram to a closed port is lost");
        check(c->open(7000, &error), "a closed port can be bound again");
    }

    // Determinism: same seed => bit-identical outcome; different seed => different outcome.
    {
        ImpairmentConfig cfg;
        cfg.loss = 0.3; cfg.duplicate = 0.2; cfg.reorder = 0.5;
        cfg.latency = milliseconds(10); cfg.jitter = milliseconds(15);
        const auto r1 = run(cfg, 42, 2000);
        const auto r2 = run(cfg, 42, 2000);
        const auto r3 = run(cfg, 43, 2000);
        check(r1 == r2, "same seed reproduces the exact same delivery sequence");
        check(r1 != r3, "a different seed gives a different sequence");
    }

    // Statistics of the impairments.
    {
        ImpairmentConfig lossy;
        lossy.loss = 0.25;
        const auto got = run(lossy, 7, 20000);
        const double rate = 1.0 - static_cast<double>(got.size()) / 20000.0;
        check(rate > 0.22 && rate < 0.28, "observed loss rate is close to the configured 25% (got " + std::to_string(rate) + ")");

        ImpairmentConfig dup;
        dup.duplicate = 0.5;
        const auto d = run(dup, 7, 10000);
        check(d.size() > 14000 && d.size() < 16000, "duplication adds roughly the configured share (got " + std::to_string(d.size()) + ")");

        ImpairmentConfig reorder;
        reorder.reorder = 0.5;
        reorder.reorder_extra = milliseconds(50);
        const auto r = run(reorder, 7, 2000);
        int inversions = 0;
        for (std::size_t i = 1; i < r.size(); ++i) inversions += (r[i] != (r[i - 1] + 1) % 256);
        check(r.size() == 2000 && inversions > 100, "reordering actually reorders, and loses nothing");

        ImpairmentConfig none;
        const auto clean = run(none, 7, 1000);
        bool in_order = clean.size() == 1000;
        for (std::size_t i = 0; i < clean.size() && in_order; ++i) in_order = clean[i] == static_cast<int>(i % 256);
        check(in_order, "a clean network delivers everything, in order");
    }

    // The impairment can be changed while running (e.g. to model an outage).
    {
        SimulatedNetwork net({}, 1);
        auto a = net.create_transport("10.0.0.1");
        auto b = net.create_transport("10.0.0.2");
        a->open(0); b->open(7000);
        ImpairmentConfig outage;
        outage.loss = 1.0;
        net.set_impairment(outage);
        a->send_to("10.0.0.2", 7000, {1});
        net.set_impairment({});
        a->send_to("10.0.0.2", 7000, {2});
        TransportDatagram d;
        check(b->receive(d, 0) && d.bytes[0] == 2 && !b->receive(d, 0), "only the datagram sent after the outage arrives");
    }

    if (failures == 0) std::cout << "All DWE simulated network tests passed.\n";
    return failures == 0 ? EXIT_SUCCESS : EXIT_FAILURE;
}
