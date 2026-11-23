#pragma once

#include "dwe/network/ITransport.h"
#include "dwe/network/Time.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <queue>
#include <string>
#include <unordered_map>
#include <vector>
#include <chrono>

namespace dwe::network {

struct ImpairmentConfig {
    double loss = 0.0;        // probability that a datagram is dropped
    double duplicate = 0.0;   // probability that a datagram is delivered twice
    double reorder = 0.0;     // probability that a datagram is delayed by an extra random amount
    Duration latency = Duration::zero();        // fixed one-way delay
    Duration jitter = Duration::zero();         // extra uniform random delay in [0, jitter]
    Duration reorder_extra = std::chrono::milliseconds(50);  // upper bound of the extra delay above
};

struct SimulatedNetworkStats {
    std::uint64_t sent = 0;
    std::uint64_t delivered = 0;
    std::uint64_t lost = 0;
    std::uint64_t duplicated = 0;
    std::uint64_t delayed_for_reorder = 0;
    std::uint64_t undeliverable = 0;  // sent to an address nobody is listening on
    std::uint64_t bytes_sent = 0;
};

class SimTransport;

// An in-memory datagram network with a *virtual* clock and a seeded random generator, so that a
// run is fully reproducible (the generator does not use <random> distributions, whose output
// differs between standard libraries). Time only moves when the test calls advance().
class SimulatedNetwork {
public:
    explicit SimulatedNetwork(ImpairmentConfig config = {}, std::uint64_t seed = 1);
    ~SimulatedNetwork();
    SimulatedNetwork(const SimulatedNetwork&) = delete;
    SimulatedNetwork& operator=(const SimulatedNetwork&) = delete;

    TimePoint now() const { return now_; }
    void advance_to(TimePoint t) { if (t > now_) now_ = t; }
    void advance(Duration d) { now_ += d; }
    // Clock function for Server/ClientConnection so they see the virtual time.
    TimePoint clock() const { return now_; }

    void set_impairment(const ImpairmentConfig& config) { config_ = config; }
    const ImpairmentConfig& impairment() const { return config_; }
    const SimulatedNetworkStats& stats() const { return stats_; }

    // `address` is the IP-like host name of the new endpoint, e.g. "10.0.0.1".
    std::unique_ptr<SimTransport> create_transport(std::string address);

    // Number of datagrams still travelling (not yet received by their destination).
    std::size_t in_transit() const;

private:
    friend class SimTransport;
    struct Item {
        TimePoint deliver_at;
        std::uint64_t order;
        std::vector<std::uint8_t> bytes;
        std::string from_address;
        std::uint16_t from_port;
    };
    struct Later {
        bool operator()(const Item& a, const Item& b) const {
            return a.deliver_at != b.deliver_at ? a.deliver_at > b.deliver_at : a.order > b.order;
        }
    };
    using Mailbox = std::priority_queue<Item, std::vector<Item>, Later>;

    Mailbox* bind(const std::string& address, std::uint16_t& port);  // port 0 => ephemeral
    void unbind(const std::string& address, std::uint16_t port);
    void transmit(const std::string& from_address, std::uint16_t from_port, const std::string& host,
                  std::uint16_t port, const std::vector<std::uint8_t>& bytes);
    double uniform();
    std::uint64_t below(std::uint64_t n);
    Duration random_duration(Duration max);

    ImpairmentConfig config_;
    SimulatedNetworkStats stats_;
    TimePoint now_{};
    std::uint64_t rng_state_;
    std::uint64_t order_{0};
    std::uint16_t next_ephemeral_{49152};
    std::unordered_map<std::string, Mailbox> mailboxes_;  // key "address:port"
};

class SimTransport final : public ITransport {
public:
    SimTransport(SimulatedNetwork& network, std::string address);
    ~SimTransport() override;

    bool open(std::uint16_t local_port, std::string* error = nullptr) override;
    bool send_to(const std::string& host, std::uint16_t port, const std::vector<std::uint8_t>& bytes,
                 std::string* error = nullptr) override;
    // Never blocks: time is virtual. Returns true if a datagram is due at the network's current time.
    bool receive(TransportDatagram& datagram, int timeout_ms, std::string* error = nullptr) override;
    std::uint16_t local_port() const override { return port_; }
    void close() override;

    const std::string& address() const { return address_; }

private:
    SimulatedNetwork* network_;
    std::string address_;
    std::uint16_t port_{0};
    SimulatedNetwork::Mailbox* mailbox_{nullptr};
};

} // namespace dwe::network
