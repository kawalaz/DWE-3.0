#pragma once

#include "dwe/network/ChannelManager.h"
#include "dwe/network/ITransport.h"
#include "dwe/network/Time.h"

#include <chrono>
#include <cstdint>
#include <functional>
#include <string>
#include <unordered_map>
#include <vector>
#include <span>

namespace dwe::network {

enum class ClientState { Idle, Connecting, Connected, Disconnecting, Closed, Failed };
const char* client_state_name(ClientState state);

struct ClientConfig {
    std::string host = "127.0.0.1";
    std::uint16_t port = 27015;
    std::uint64_t nonce = 0;  // identifies this connection attempt; 0 = pick a random one
    std::chrono::milliseconds connect_timeout = std::chrono::seconds(5);
    std::chrono::milliseconds hello_interval = std::chrono::milliseconds(250);  // doubles up to 1 s
    // A Ping is sent whenever nothing was sent for this long. Over a lossy link each probe can be
    // lost in either direction, and server_timeout / keepalive_interval probes are the only chances
    // to hear from the server. With loss p per direction the chance of a false timeout is
    //     (1 - (1-p)^2) ^ (server_timeout / keepalive_interval)
    // 10 s / 0.5 s = 20 probes keeps it below 1e-7 per window at p = 25% (see docs/architecture/reliability.md).
    std::chrono::milliseconds keepalive_interval = std::chrono::milliseconds(500);
    std::chrono::milliseconds server_timeout = std::chrono::seconds(10);  // no packet from the server at all
    std::chrono::milliseconds disconnect_timeout = std::chrono::seconds(2);
    ChannelConfig channel;
};

struct ClientStats {
    Duration handshake_latency{};
    std::uint64_t hellos_sent = 0;
    std::uint64_t packets_sent = 0;
    std::uint64_t packets_received = 0;
    std::uint64_t malformed = 0;
    std::uint64_t foreign_source_dropped = 0;  // datagrams from an address other than the server's
    std::uint64_t pings_sent = 0;
    std::uint64_t echoes_received = 0;
    std::uint64_t errors_received = 0;
};

// Client side of a DWE session: handshake with retransmission, keep-alive, channels, disconnect.
// Like Server it is driven by update() with an injected transport and clock. The transport must
// already be open (open(0) for an ephemeral port).
class ClientConnection {
public:
    using ClockFn = std::function<TimePoint()>;

    ClientConnection(ITransport& transport, ClientConfig config, ClockFn clock = {});

    void connect();
    // Receives everything pending, runs timers, transmits whatever is due. Never blocks.
    void update();
    // Queues a message. Reliable channels are transmitted (and retransmitted) by update().
    SendStatus send(dwe::core::ChannelId channel, std::span<const std::uint8_t> data);
    // Asks the server to close the session. update() keeps retrying until confirmed or timed out.
    void disconnect();
    void drain_delivered(std::vector<dwe::core::ChannelMessage>& out) { channels_.drain_delivered(out); }

    ClientState state() const { return state_; }
    bool connected() const { return state_ == ClientState::Connected; }
    // Closed and Failed are final states.
    bool finished() const { return state_ == ClientState::Closed || state_ == ClientState::Failed; }
    dwe::core::SessionId session_id() const { return session_id_; }
    const std::string& failure_reason() const { return failure_reason_; }
    bool server_closed() const { return server_closed_; }
    bool reliable_idle() const { return channels_.reliable_idle(); }
    ChannelStats channel_stats() const { return channels_.stats(); }
    const RttEstimator& rtt() const { return channels_.rtt(); }
    const ClientStats& stats() const { return stats_; }

private:
    void handle_datagram(const TransportDatagram& datagram, TimePoint now);
    void send_packet(const dwe::core::Packet& packet, TimePoint now);
    void send_hello(TimePoint now);
    void send_disconnect(TimePoint now);
    void fail(std::string reason);
    void flush_channels(TimePoint now);

    ITransport& transport_;
    ClientConfig config_;
    ClockFn clock_;
    ClientState state_{ClientState::Idle};
    std::uint64_t nonce_{0};
    std::uint32_t hello_sequence_{1};  // transaction id: the server echoes it in Welcome
    dwe::core::SessionId session_id_{0};
    ChannelManager channels_{1};       // replaced once the session ID is known
    std::string pinned_address_;       // set from the first valid Welcome; later packets must match
    std::uint16_t pinned_port_{0};
    bool pinned_{false};

    TimePoint connect_started_{}, next_hello_at_{}, last_sent_{}, last_received_{}, disconnect_started_{}, next_disconnect_at_{};
    std::chrono::milliseconds hello_backoff_{250};
    std::uint32_t ping_sequence_{0};
    std::unordered_map<std::uint32_t, TimePoint> pending_pings_;
    std::string failure_reason_;
    bool server_closed_{false};
    std::vector<std::uint8_t> tx_buffer_;
    ClientStats stats_;
};

} // namespace dwe::network
