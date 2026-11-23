#pragma once

#include "dwe/network/ITransport.h"

#include <cstdint>
#include <memory>
#include <utility>
#include <cstddef>
#include <string>
#include <vector>

namespace dwe::network {

// Decorator that randomly drops datagrams in both directions, to exercise the reliability layer
// over a real transport (loopback UDP never loses packets on its own). It adds no delay or
// reordering; use SimulatedNetwork for those.
//
// `drop_probability` applies independently to every datagram this endpoint sends AND to every
// datagram it receives. If both ends of a path are wrapped, one direction of the path loses
// 1 - (1 - p)^2 of its datagrams (p = 0.134 per end gives about 25% per direction).
class LossyTransport final : public ITransport {
public:
    // Non-owning: wraps a transport that someone else keeps alive.
    LossyTransport(ITransport& inner, double drop_probability, std::uint64_t seed = 1)
        : inner_(inner), drop_(drop_probability), state_(seed * 0x9E3779B97F4A7C15ULL + 0xD1B54A32D192ED03ULL) {}
    // Owning: lets a Server, which owns its transport, run over a lossy one.
    LossyTransport(std::unique_ptr<ITransport> inner, double drop_probability, std::uint64_t seed = 1)
        : owned_(std::move(inner)), inner_(*owned_), drop_(drop_probability),
          state_(seed * 0x9E3779B97F4A7C15ULL + 0xD1B54A32D192ED03ULL) {}

    bool open(std::uint16_t local_port, std::string* error = nullptr) override { return inner_.open(local_port, error); }
    bool send_to(const std::string& host, std::uint16_t port, const std::vector<std::uint8_t>& bytes,
                 std::string* error = nullptr) override {
        if (drop()) { ++dropped_; return true; }  // the sender cannot tell, exactly like real loss
        return inner_.send_to(host, port, bytes, error);
    }
    bool receive(TransportDatagram& datagram, int timeout_ms, std::string* error = nullptr) override {
        if (!inner_.receive(datagram, timeout_ms, error)) return false;
        if (drop()) { ++dropped_; if (error) error->clear(); return false; }
        return true;
    }
    std::uint16_t local_port() const override { return inner_.local_port(); }
    void close() override { inner_.close(); }
    void tune_buffers(std::size_t r, std::size_t w) override { inner_.tune_buffers(r, w); }
    std::uint64_t dropped() const { return dropped_; }

private:
    bool drop() {
        if (drop_ <= 0.0) return false;
        state_ ^= state_ << 13; state_ ^= state_ >> 7; state_ ^= state_ << 17;
        return static_cast<double>(state_ >> 11) * (1.0 / 9007199254740992.0) < drop_;
    }
    std::unique_ptr<ITransport> owned_;
    ITransport& inner_;
    double drop_;
    std::uint64_t state_;
    std::uint64_t dropped_{0};
};

} // namespace dwe::network
