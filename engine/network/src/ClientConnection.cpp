#include "dwe/network/ClientConnection.h"

#include <random>
#include <utility>
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <span>
#include <string>
#include <vector>

namespace dwe::network {

using namespace dwe::core;

const char* client_state_name(ClientState state) {
    switch (state) {
        case ClientState::Idle: return "Idle";
        case ClientState::Connecting: return "Connecting";
        case ClientState::Connected: return "Connected";
        case ClientState::Disconnecting: return "Disconnecting";
        case ClientState::Closed: return "Closed";
        case ClientState::Failed: return "Failed";
    }
    return "Unknown";
}

ClientConnection::ClientConnection(ITransport& transport, ClientConfig config, ClockFn clock)
    : transport_(transport), config_(std::move(config)),
      clock_(clock ? std::move(clock) : ClockFn([] { return Clock::now(); })),
      channels_(1, config_.channel) {
    nonce_ = config_.nonce;
    if (nonce_ == 0) {
        std::random_device rd;
        nonce_ = (static_cast<std::uint64_t>(rd()) << 32) ^ rd();
        if (nonce_ == 0) nonce_ = 1;
    }
}

void ClientConnection::fail(std::string reason) {
    state_ = ClientState::Failed;
    failure_reason_ = std::move(reason);
}

void ClientConnection::send_packet(const Packet& packet, TimePoint now) {
    if (!serialize(packet, tx_buffer_)) return;
    // Before the handshake completes we talk to the configured host; afterwards to the pinned endpoint.
    const bool ok = pinned_ ? transport_.send_to(pinned_address_, pinned_port_, tx_buffer_)
                            : transport_.send_to(config_.host, config_.port, tx_buffer_);
    if (ok) {
        ++stats_.packets_sent;
        last_sent_ = now;
    }
}

void ClientConnection::send_hello(TimePoint now) {
    Packet hello;
    hello.type = MessageType::Hello;
    hello.sequence = hello_sequence_;
    encode_hello_payload(nonce_, hello.payload);
    send_packet(hello, now);
    ++stats_.hellos_sent;
}

void ClientConnection::send_disconnect(TimePoint now) {
    Packet bye;
    bye.type = MessageType::Disconnect;
    bye.sequence = 1;
    encode_disconnect_payload(DisconnectMessage{session_id_, DisconnectReason::Requested}, bye.payload);
    send_packet(bye, now);
}

void ClientConnection::connect() {
    if (state_ != ClientState::Idle && state_ != ClientState::Closed && state_ != ClientState::Failed) return;
    const TimePoint now = clock_();
    state_ = ClientState::Connecting;
    failure_reason_.clear();
    server_closed_ = false;
    pinned_ = false;
    session_id_ = 0;
    connect_started_ = last_received_ = now;
    hello_backoff_ = config_.hello_interval;
    next_hello_at_ = now + hello_backoff_;
    send_hello(now);
}

SendStatus ClientConnection::send(ChannelId channel, std::span<const std::uint8_t> data) {
    if (state_ != ClientState::Connected) return SendStatus::Failed;
    return channels_.send(channel, data, clock_());
}

void ClientConnection::disconnect() {
    if (state_ == ClientState::Connecting || state_ == ClientState::Idle) {
        state_ = ClientState::Closed;
        return;
    }
    if (state_ != ClientState::Connected) return;
    const TimePoint now = clock_();
    state_ = ClientState::Disconnecting;
    disconnect_started_ = now;
    next_disconnect_at_ = now + std::chrono::milliseconds(250);
    send_disconnect(now);
}

void ClientConnection::flush_channels(TimePoint now) {
    channels_.update(now);
    std::vector<Packet> packets;
    channels_.drain_outgoing(packets);
    for (const auto& p : packets) send_packet(p, now);
}

void ClientConnection::handle_datagram(const TransportDatagram& datagram, TimePoint now) {
    // Only the server we are talking to may influence this session. Session IDs are small
    // sequential numbers and therefore guessable, so the source address is part of the check.
    if (pinned_) {
        if (datagram.address != pinned_address_ || datagram.port != pinned_port_) {
            ++stats_.foreign_source_dropped;
            return;
        }
    } else if (datagram.port != config_.port) {
        ++stats_.foreign_source_dropped;
        return;
    }

    Packet packet;
    if (!deserialize(datagram.bytes, packet)) {
        ++stats_.malformed;
        return;
    }
    ++stats_.packets_received;

    if (state_ == ClientState::Connecting) {
        SessionId id = 0;
        if (packet.type == MessageType::Welcome && packet.sequence == hello_sequence_ &&
            decode_session_id(packet.payload, id)) {
            session_id_ = id;
            pinned_address_ = datagram.address;
            pinned_port_ = datagram.port;
            pinned_ = true;
            channels_ = ChannelManager(id, config_.channel);
            state_ = ClientState::Connected;
            last_received_ = last_sent_ = now;
            stats_.handshake_latency = now - connect_started_;
        } else if (packet.type == MessageType::Error) {
            ++stats_.errors_received;  // e.g. "server is full"; keep retrying until the timeout
        }
        return;
    }

    if (state_ == ClientState::Disconnecting) {
        SessionId id = 0;
        if (packet.type == MessageType::DisconnectAck && decode_session_id(packet.payload, id) && id == session_id_) {
            state_ = ClientState::Closed;
        }
        return;
    }
    if (state_ != ClientState::Connected) return;

    switch (packet.type) {
        case MessageType::ChannelData: {
            ChannelMessage message;
            if (!decode_channel_payload(packet.payload, message) || message.session_id != session_id_) return;
            last_received_ = now;
            channels_.on_channel_data(message.channel, packet.sequence, message.data, now);
            break;
        }
        case MessageType::Ack: {
            AckMessage ack;
            if (!decode_ack_payload(packet.payload, ack) || ack.session_id != session_id_) return;
            last_received_ = now;
            channels_.on_ack(ack, now);
            break;
        }
        case MessageType::Echo: {
            SessionId id = 0;
            if (!decode_session_id(packet.payload, id) || id != session_id_) return;
            last_received_ = now;
            ++stats_.echoes_received;
            const auto it = pending_pings_.find(packet.sequence);
            if (it != pending_pings_.end()) {
                channels_.add_rtt_sample(now - it->second);
                pending_pings_.erase(it);
            }
            break;
        }
        case MessageType::Disconnect: {
            DisconnectMessage message;
            if (!decode_disconnect_payload(packet.payload, message) || message.session_id != session_id_) return;
            server_closed_ = true;
            state_ = ClientState::Closed;
            failure_reason_ = message.reason == DisconnectReason::ServerShutdown ? "server shut down"
                              : message.reason == DisconnectReason::Timeout      ? "server closed the session (timeout)"
                                                                                  : "server closed the session";
            break;
        }
        case MessageType::Error:
            ++stats_.errors_received;
            break;
        default:
            break;  // Welcome retransmissions, DisconnectAck, anything a client does not expect
    }
}

void ClientConnection::update() {
    if (state_ == ClientState::Idle || finished()) return;
    TimePoint now = clock_();

    for (int i = 0; i < 1024; ++i) {
        TransportDatagram datagram;
        std::string error;
        if (!transport_.receive(datagram, 0, &error)) {
            if (error.empty()) break;
            continue;  // a datagram was dropped by the transport (e.g. oversized): keep draining
        }
        now = clock_();
        handle_datagram(datagram, now);
        if (finished()) return;
    }
    now = clock_();

    switch (state_) {
        case ClientState::Connecting:
            if (now - connect_started_ >= config_.connect_timeout) {
                fail("handshake timed out");
            } else if (now >= next_hello_at_) {
                send_hello(now);
                hello_backoff_ = std::min(hello_backoff_ * 2, std::chrono::milliseconds(1000));
                next_hello_at_ = now + hello_backoff_;
            }
            break;

        case ClientState::Connected:
            flush_channels(now);
            if (channels_.failed()) {
                fail("reliable delivery to the server failed (no acknowledgement)");
                break;
            }
            if (now - last_received_ >= config_.server_timeout) {
                fail("no packets from the server (server timeout)");
                break;
            }
            if (now - last_sent_ >= config_.keepalive_interval) {
                Packet ping;
                ping.type = MessageType::Ping;
                ping.sequence = ++ping_sequence_;
                encode_session_id(session_id_, ping.payload);
                if (pending_pings_.size() >= 8) pending_pings_.clear();  // bounded: unanswered probes are forgotten
                pending_pings_[ping.sequence] = now;
                send_packet(ping, now);
                ++stats_.pings_sent;
            }
            break;

        case ClientState::Disconnecting:
            if (now - disconnect_started_ >= config_.disconnect_timeout) {
                state_ = ClientState::Closed;  // the server will expire the session on its own
            } else if (now >= next_disconnect_at_) {
                send_disconnect(now);
                next_disconnect_at_ = now + std::chrono::milliseconds(250);
            }
            break;

        default:
            break;
    }
}

} // namespace dwe::network
