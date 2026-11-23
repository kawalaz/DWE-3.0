#include "dwe/network/Server.h"

#include <utility>
#include <cstddef>
#include <cstdint>
#include <span>
#include <string>

namespace dwe::network {

using namespace dwe::core;

Server::Server(std::unique_ptr<ITransport> transport, ServerConfig config, ClockFn clock)
    : transport_(std::move(transport)), config_(std::move(config)),
      clock_(clock ? std::move(clock) : ClockFn([] { return Clock::now(); })),
      manager_(config_.connection) {
    if (config_.echo) {
        handler_ = [](Server& server, const ChannelMessage& message) {
            server.send(message.session_id, message.channel, message.data);
        };
    }
}

bool Server::start(std::string* error) {
    if (!transport_->open(config_.port, error)) return false;
    transport_->tune_buffers(config_.receive_buffer_bytes, config_.send_buffer_bytes);
    return true;
}

SendStatus Server::send(SessionId session, ChannelId channel, std::span<const std::uint8_t> data) {
    const auto status = manager_.send(session, channel, data, clock_());
    if (status != SendStatus::Ok) ++stats_.app_send_rejected;
    return status;
}

void Server::send_packet(const NetworkEndpoint& endpoint, const Packet& packet) {
    if (!serialize(packet, tx_buffer_)) {
        ++stats_.send_failures;
        return;
    }
    if (transport_->send_to(endpoint.address, endpoint.port, tx_buffer_)) {
        ++stats_.packets_sent;
        stats_.bytes_sent += tx_buffer_.size();
    } else {
        ++stats_.send_failures;
    }
}

void Server::flush_outgoing() {
    for (auto& out : manager_.take_outgoing()) send_packet(out.endpoint, out.packet);
}

void Server::handle_datagram(const TransportDatagram& datagram, TimePoint now) {
    ++stats_.datagrams_received;
    stats_.bytes_received += datagram.bytes.size();
    const NetworkEndpoint endpoint{datagram.address, datagram.port};

    // Flow control comes first: a flooder must cost as little as possible and learn nothing.
    if (!manager_.admit(endpoint, datagram.bytes.size(), now)) {
        ++stats_.rate_limited;
        return;
    }
    Packet packet;
    std::string error;
    if (!deserialize(datagram.bytes, packet, &error)) {
        ++stats_.malformed;
        if (verbose_) log("Dropped malformed packet from " + endpoint.address + ":" + std::to_string(endpoint.port) + ": " + error);
        return;
    }
    auto result = manager_.process(packet, endpoint, now);
    if (result.response) send_packet(endpoint, *result.response);
    if (result.session_created) {
        log("Session connected: endpoint=" + endpoint.address + ":" + std::to_string(endpoint.port) +
            " active=" + std::to_string(manager_.active_sessions()));
    }
    if (result.disconnected) {
        log("Session disconnected: " + endpoint.address + ":" + std::to_string(endpoint.port) +
            " active=" + std::to_string(manager_.active_sessions()));
    }
    if (verbose_ && !result.accepted && !result.error.empty() && !result.rate_limited) {
        log("Rejected packet from " + endpoint.address + ":" + std::to_string(endpoint.port) + ": " + result.error);
    }
    for (const auto& message : result.delivered) {
        ++stats_.messages_delivered;
        if (verbose_) {
            log(std::string("Channel data: session=") + std::to_string(message.session_id) +
                " channel=" + channel_name(message.channel) + " bytes=" + std::to_string(message.data.size()));
        }
        if (handler_) handler_(*this, message);
    }
}

void Server::poll() {
    TimePoint now = clock_();
    for (std::size_t n = 0; n < config_.max_datagrams_per_poll; ++n) {
        TransportDatagram datagram;
        std::string error;
        // Only the first receive may block; after that we just drain what is already queued.
        if (!transport_->receive(datagram, n == 0 ? config_.receive_timeout_ms : 0, &error)) {
            if (error.empty()) break;  // timeout / nothing more to read
            ++stats_.transport_errors;  // e.g. an oversized datagram was dropped: keep draining
            continue;
        }
        now = clock_();
        handle_datagram(datagram, now);
    }
    now = clock_();
    manager_.update(now);
    flush_outgoing();

    const auto& ms = manager_.stats();
    if (ms.sessions_expired != logged_expired_) {
        log(std::to_string(ms.sessions_expired - logged_expired_) + " idle session(s) expired; active=" + std::to_string(manager_.active_sessions()));
        logged_expired_ = ms.sessions_expired;
    }
    if (ms.sessions_failed != logged_failed_) {
        log(std::to_string(ms.sessions_failed - logged_failed_) + " session(s) closed: reliable delivery failed; active=" + std::to_string(manager_.active_sessions()));
        logged_failed_ = ms.sessions_failed;
    }
}

void Server::shutdown() {
    manager_.close_all(DisconnectReason::ServerShutdown);
    flush_outgoing();
}

} // namespace dwe::network
