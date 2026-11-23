#include "dwe/network/SimulatedNetwork.h"

#include <utility>
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace dwe::network {

SimulatedNetwork::SimulatedNetwork(ImpairmentConfig config, std::uint64_t seed)
    : config_(config), rng_state_(seed * 0x9E3779B97F4A7C15ULL + 0xD1B54A32D192ED03ULL) {
    if (rng_state_ == 0) rng_state_ = 1;
}
SimulatedNetwork::~SimulatedNetwork() = default;

double SimulatedNetwork::uniform() {
    rng_state_ ^= rng_state_ << 13;
    rng_state_ ^= rng_state_ >> 7;
    rng_state_ ^= rng_state_ << 17;
    return static_cast<double>(rng_state_ >> 11) * (1.0 / 9007199254740992.0);
}
std::uint64_t SimulatedNetwork::below(std::uint64_t n) {
    return static_cast<std::uint64_t>(uniform() * static_cast<double>(n));
}
Duration SimulatedNetwork::random_duration(Duration max) {
    if (max <= Duration::zero()) return Duration::zero();
    return Duration(static_cast<Duration::rep>(uniform() * static_cast<double>(max.count())));
}

std::unique_ptr<SimTransport> SimulatedNetwork::create_transport(std::string address) {
    return std::make_unique<SimTransport>(*this, std::move(address));
}

std::size_t SimulatedNetwork::in_transit() const {
    std::size_t n = 0;
    for (const auto& entry : mailboxes_) n += entry.second.size();
    return n;
}

SimulatedNetwork::Mailbox* SimulatedNetwork::bind(const std::string& address, std::uint16_t& port) {
    if (port == 0) {
        for (int tries = 0; tries < 20000; ++tries) {
            const std::uint16_t candidate = next_ephemeral_;
            next_ephemeral_ = next_ephemeral_ == 65535 ? std::uint16_t{49152} : static_cast<std::uint16_t>(next_ephemeral_ + 1);
            if (mailboxes_.find(address + ":" + std::to_string(candidate)) == mailboxes_.end()) {
                port = candidate;
                break;
            }
        }
        if (port == 0) return nullptr;
    }
    const auto key = address + ":" + std::to_string(port);
    const auto result = mailboxes_.try_emplace(key);
    return result.second ? &result.first->second : nullptr;  // address already in use
}

void SimulatedNetwork::unbind(const std::string& address, std::uint16_t port) {
    mailboxes_.erase(address + ":" + std::to_string(port));
}

void SimulatedNetwork::transmit(const std::string& from_address, std::uint16_t from_port, const std::string& host,
                                std::uint16_t port, const std::vector<std::uint8_t>& bytes) {
    ++stats_.sent;
    stats_.bytes_sent += bytes.size();
    const auto destination = mailboxes_.find(host + ":" + std::to_string(port));
    if (destination == mailboxes_.end()) {
        ++stats_.undeliverable;  // UDP: nobody is told
        return;
    }
    if (uniform() < config_.loss) {
        ++stats_.lost;
        return;
    }
    const int copies = uniform() < config_.duplicate ? 2 : 1;
    if (copies == 2) ++stats_.duplicated;
    for (int i = 0; i < copies; ++i) {
        Duration delay = config_.latency + random_duration(config_.jitter);
        if (uniform() < config_.reorder) {
            delay += random_duration(config_.reorder_extra);
            ++stats_.delayed_for_reorder;
        }
        destination->second.push(Item{now_ + delay, order_++, bytes, from_address, from_port});
    }
}

// ------------------------------------------------------------------------------------------------

SimTransport::SimTransport(SimulatedNetwork& network, std::string address)
    : network_(&network), address_(std::move(address)) {}
SimTransport::~SimTransport() { close(); }

bool SimTransport::open(std::uint16_t local_port, std::string* error) {
    close();
    std::uint16_t port = local_port;
    mailbox_ = network_->bind(address_, port);
    if (!mailbox_) {
        if (error) *error = "simulated address already in use";
        return false;
    }
    port_ = port;
    return true;
}

bool SimTransport::send_to(const std::string& host, std::uint16_t port, const std::vector<std::uint8_t>& bytes,
                           std::string* error) {
    if (!mailbox_) {
        if (error) *error = "socket is not open";
        return false;
    }
    network_->transmit(address_, port_, host, port, bytes);
    return true;
}

bool SimTransport::receive(TransportDatagram& datagram, int, std::string* error) {
    if (error) error->clear();
    if (!mailbox_) {
        if (error) *error = "socket is not open";
        return false;
    }
    if (mailbox_->empty() || mailbox_->top().deliver_at > network_->now()) return false;
    // priority_queue::top() is const; copying the item out is acceptable for a test network.
    SimulatedNetwork::Item item = mailbox_->top();
    mailbox_->pop();
    datagram.bytes = std::move(item.bytes);
    datagram.address = std::move(item.from_address);
    datagram.port = item.from_port;
    ++network_->stats_.delivered;
    return true;
}

void SimTransport::close() {
    if (mailbox_) {
        network_->unbind(address_, port_);
        mailbox_ = nullptr;
        port_ = 0;
    }
}

} // namespace dwe::network
