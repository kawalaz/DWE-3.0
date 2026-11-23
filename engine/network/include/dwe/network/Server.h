#pragma once

#include "dwe/network/ConnectionManager.h"
#include "dwe/network/ITransport.h"
#include "dwe/network/Time.h"

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>
#include <cstddef>
#include <span>
#include <utility>

namespace dwe::network {

struct ServerConfig {
    std::uint16_t port = 27015;
    ConnectionConfig connection;
    int receive_timeout_ms = 5;                // how long poll() may block waiting for the first datagram
    std::size_t max_datagrams_per_poll = 512;  // keeps one busy socket from starving timers
    bool echo = true;                          // demo handler: send every delivered message back on its channel
    std::size_t receive_buffer_bytes = 4u << 20;  // requested OS receive buffer (the OS may cap it)
    std::size_t send_buffer_bytes = 1u << 20;
};

struct ServerStats {
    std::uint64_t datagrams_received = 0;
    std::uint64_t bytes_received = 0;
    std::uint64_t malformed = 0;
    std::uint64_t rate_limited = 0;
    std::uint64_t transport_errors = 0;
    std::uint64_t packets_sent = 0;
    std::uint64_t bytes_sent = 0;
    std::uint64_t send_failures = 0;
    std::uint64_t messages_delivered = 0;
    std::uint64_t app_send_rejected = 0;  // application sends refused (queue full, unknown session, ...)
};

// The server runtime: owns the transport and the ConnectionManager and wires them together.
// It is driven by poll(); both the transport and the clock are injected, so the same code runs on
// real UDP sockets and inside the deterministic simulator.
class Server {
public:
    using ClockFn = std::function<TimePoint()>;
    using LogFn = std::function<void(const std::string&)>;
    using MessageHandler = std::function<void(Server&, const dwe::core::ChannelMessage&)>;

    Server(std::unique_ptr<ITransport> transport, ServerConfig config, ClockFn clock = {});

    bool start(std::string* error = nullptr);
    // One iteration: receive (blocks up to receive_timeout_ms for the first datagram), process,
    // run timers, send everything that is due.
    void poll();
    // Tells every client the server is going away and flushes.
    void shutdown();

    // Application -> client.
    SendStatus send(dwe::core::SessionId session, dwe::core::ChannelId channel, std::span<const std::uint8_t> data);

    void set_message_handler(MessageHandler handler) { handler_ = std::move(handler); }
    void set_log(LogFn log) { log_ = std::move(log); }
    void set_verbose(bool verbose) { verbose_ = verbose; }

    ConnectionManager& connections() { return manager_; }
    const ConnectionManager& connections() const { return manager_; }
    const ServerStats& stats() const { return stats_; }
    std::uint16_t port() const { return transport_->local_port(); }

private:
    void handle_datagram(const TransportDatagram& datagram, TimePoint now);
    void send_packet(const NetworkEndpoint& endpoint, const dwe::core::Packet& packet);
    void flush_outgoing();
    void log(const std::string& line) const { if (log_) log_(line); }

    std::unique_ptr<ITransport> transport_;
    ServerConfig config_;
    ClockFn clock_;
    ConnectionManager manager_;
    MessageHandler handler_;
    LogFn log_;
    bool verbose_{false};
    ServerStats stats_;
    std::vector<std::uint8_t> tx_buffer_;
    std::uint64_t logged_expired_{0}, logged_failed_{0};
};

} // namespace dwe::network
