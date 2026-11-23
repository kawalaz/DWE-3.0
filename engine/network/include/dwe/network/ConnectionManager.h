#pragma once

#include "dwe/core/Packet.h"
#include "dwe/network/ChannelManager.h"
#include "dwe/network/RateLimiter.h"
#include "dwe/network/Time.h"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <unordered_map>
#include <vector>
#include <utility>

namespace dwe::network {

struct NetworkEndpoint {
    std::string address;
    std::uint16_t port{0};
    bool operator==(const NetworkEndpoint&) const = default;
};

struct RateLimitConfig {
    TokenBucketConfig packets{200.0, 400.0, true};                 // per endpoint, datagrams per second
    TokenBucketConfig bytes{256.0 * 1024, 512.0 * 1024, true};     // per endpoint, bytes per second (burst >= 1200)
    TokenBucketConfig hello_per_endpoint{2.0, 6.0, true};          // Hello packets per endpoint
    TokenBucketConfig hello_global{1000.0, 2000.0, true};          // new sessions per second, whole server
    std::size_t max_tracked_endpoints = 65536;                     // bounds limiter memory under spoofed floods

    static RateLimitConfig unlimited() {
        RateLimitConfig c;
        c.packets = c.bytes = c.hello_per_endpoint = c.hello_global = TokenBucketConfig::unlimited();
        return c;
    }
};

struct ConnectionConfig {
    std::size_t max_sessions = 128;
    std::chrono::milliseconds idle_timeout = std::chrono::seconds(15);
    ChannelConfig channel;
    RateLimitConfig rate;
    std::chrono::milliseconds timer_interval = std::chrono::milliseconds(5);     // retransmission timer scan
    std::chrono::milliseconds expire_interval = std::chrono::milliseconds(250);  // idle-session sweep
};

struct ConnectionResult {
    bool accepted{false};
    bool session_created{false};
    bool disconnected{false};
    bool rate_limited{false};
    std::optional<dwe::core::Packet> response;               // direct reply to the sender, if any
    std::vector<dwe::core::ChannelMessage> delivered;        // application messages ready for the game
    std::string error;
};

// A packet the manager wants sent that is not a direct reply (acks, retransmissions, ...).
struct OutgoingPacket {
    NetworkEndpoint endpoint;
    dwe::core::Packet packet;
};

struct ManagerStats {
    std::uint64_t sessions_created = 0;
    std::uint64_t sessions_replaced = 0;   // same endpoint, new client incarnation (different nonce)
    std::uint64_t sessions_closed = 0;     // by Disconnect
    std::uint64_t sessions_expired = 0;    // idle
    std::uint64_t sessions_failed = 0;     // reliable delivery exhausted its retransmissions
    std::uint64_t rejected_full = 0;
    std::uint64_t rate_limited_packets = 0;
    std::uint64_t rate_limited_hellos = 0;
    std::uint64_t packets_processed = 0;
    std::uint64_t error_replies = 0;
};

// Session table plus everything bound to a session: channels (reliability), rate limits, liveness.
//
// Typical server loop:
//   if (!manager.admit(endpoint, datagram_size, now)) drop;          // rate limit, before parsing
//   result = manager.process(packet, endpoint, now);                  // send result.response; use result.delivered
//   manager.update(now);                                              // timers, acks, retransmissions
//   for (auto& out : manager.take_outgoing()) send(out);
class ConnectionManager {
public:
    using Clock = dwe::network::Clock;
    using TimePoint = dwe::network::TimePoint;

    explicit ConnectionManager(ConnectionConfig config);
    explicit ConnectionManager(std::size_t max_sessions = 128,
                               std::chrono::milliseconds idle_timeout = std::chrono::seconds(15));

    // Per-endpoint flow control; call before deserializing. false = drop silently (no reply, so a
    // flooder or a spoofed victim gets nothing back).
    bool admit(const NetworkEndpoint& endpoint, std::size_t datagram_bytes, TimePoint now);

    ConnectionResult process(const dwe::core::Packet& packet, const NetworkEndpoint& endpoint,
                             TimePoint now = Clock::now());

    // Application -> session. Reliable messages are queued; the next update() transmits them.
    SendStatus send(dwe::core::SessionId session_id, dwe::core::ChannelId channel,
                    std::span<const std::uint8_t> data, TimePoint now = Clock::now());

    // Drives time-based work: flushes acks/transmissions for sessions with news, runs the
    // retransmission timers, fails dead sessions, sweeps idle sessions.
    void update(TimePoint now = Clock::now());
    std::vector<OutgoingPacket> take_outgoing();

    // Closes every session, notifying each peer (ServerShutdown) via take_outgoing().
    void close_all(dwe::core::DisconnectReason reason);

    std::size_t expire(TimePoint now = Clock::now());
    std::size_t active_sessions() const noexcept;
    bool has_session(dwe::core::SessionId session_id) const noexcept;
    std::optional<NetworkEndpoint> endpoint_of(dwe::core::SessionId session_id) const;
    std::optional<ChannelStats> channel_stats(dwe::core::SessionId session_id) const;
    std::optional<Duration> rtt_of(dwe::core::SessionId session_id) const;

    const ManagerStats& stats() const noexcept { return stats_; }
    // Summed over all sessions, including those that have already ended (their counters are kept).
    ChannelStats aggregate_channel_stats() const;
    std::size_t tracked_endpoints() const noexcept { return packet_limiter_.tracked(); }

private:
    struct Session {
        Session(dwe::core::SessionId session_id, NetworkEndpoint ep, std::string ep_key, std::uint64_t nonce,
                TimePoint now, const ChannelConfig& channel_config)
            : id(session_id), endpoint(std::move(ep)), key(std::move(ep_key)), client_nonce(nonce),
              last_seen(now), channels(session_id, channel_config) {}
        dwe::core::SessionId id;
        NetworkEndpoint endpoint;
        std::string key;
        std::uint64_t client_nonce;
        TimePoint last_seen;
        ChannelManager channels;
        bool dirty{false};
    };

    ConnectionResult handle_hello(const dwe::core::Packet& packet, const NetworkEndpoint& endpoint,
                                  const std::string& key, TimePoint now);
    // Looks a session up and checks endpoint and idleness; nullptr when it must be treated as unknown.
    Session* find_valid(dwe::core::SessionId id, const NetworkEndpoint& endpoint, TimePoint now);
    void mark_dirty(Session& session);
    void service(Session& session, TimePoint now, std::vector<dwe::core::SessionId>& failed);
    void queue_disconnect(const Session& session, dwe::core::DisconnectReason reason);
    void remove_session(dwe::core::SessionId session_id);
    dwe::core::SessionId allocate_session_id();
    static std::string endpoint_key(const NetworkEndpoint& endpoint);
    static dwe::core::Packet make_error(std::uint32_t sequence, const std::string& message);
    ConnectionResult error_result(const dwe::core::Packet& packet, std::string message);

    ConnectionConfig config_;
    dwe::core::SessionId next_session_id_{1};
    std::unordered_map<dwe::core::SessionId, Session> sessions_;
    std::unordered_map<std::string, dwe::core::SessionId> endpoint_to_session_;
    std::vector<dwe::core::SessionId> dirty_;
    std::vector<OutgoingPacket> outgoing_;
    ChannelStats retired_stats_;  // counters of sessions that no longer exist

    EndpointRateLimiter packet_limiter_;
    EndpointRateLimiter byte_limiter_;
    EndpointRateLimiter hello_limiter_;
    TokenBucket hello_global_;
    bool hello_global_ready_{false};

    TimePoint last_timer_scan_{};
    TimePoint last_expire_{};
    TimePoint last_prune_{};
    bool timer_scanned_{false};
    bool expired_once_{false};
    bool pruned_once_{false};
    ManagerStats stats_;
};

} // namespace dwe::network
