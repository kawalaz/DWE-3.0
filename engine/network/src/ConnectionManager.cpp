#include "dwe/network/ConnectionManager.h"

#include <algorithm>
#include <utility>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
#include <vector>

namespace dwe::network {

using namespace dwe::core;

namespace {
constexpr auto kEndpointMismatch = "unknown session or endpoint mismatch";
}

ConnectionManager::ConnectionManager(ConnectionConfig config)
    : config_(std::move(config)),
      packet_limiter_(config_.rate.packets, config_.rate.max_tracked_endpoints),
      byte_limiter_(config_.rate.bytes, config_.rate.max_tracked_endpoints),
      hello_limiter_(config_.rate.hello_per_endpoint, config_.rate.max_tracked_endpoints) {
    if (config_.idle_timeout <= std::chrono::milliseconds::zero()) config_.idle_timeout = std::chrono::seconds(15);
}

ConnectionManager::ConnectionManager(std::size_t max_sessions, std::chrono::milliseconds idle_timeout)
    : ConnectionManager([&] {
          ConnectionConfig c;
          c.max_sessions = max_sessions;
          c.idle_timeout = idle_timeout;
          return c;
      }()) {}

std::string ConnectionManager::endpoint_key(const NetworkEndpoint& endpoint) {
    return endpoint.address + ":" + std::to_string(endpoint.port);
}

Packet ConnectionManager::make_error(std::uint32_t sequence, const std::string& message) {
    Packet packet;
    packet.type = MessageType::Error;
    packet.sequence = sequence;
    packet.payload.assign(message.begin(), message.end());
    return packet;
}

ConnectionResult ConnectionManager::error_result(const Packet& packet, std::string message) {
    ConnectionResult result;
    result.response = make_error(packet.sequence, message);
    result.error = std::move(message);
    ++stats_.error_replies;
    return result;
}

SessionId ConnectionManager::allocate_session_id() {
    // IDs are unique within this process lifetime, but intentionally are not authentication tokens.
    for (;;) {
        const auto candidate = next_session_id_++;
        if (next_session_id_ == 0) next_session_id_ = 1;
        if (candidate != 0 && sessions_.find(candidate) == sessions_.end()) return candidate;
    }
}

void ConnectionManager::remove_session(SessionId session_id) {
    const auto it = sessions_.find(session_id);
    if (it == sessions_.end()) return;
    retired_stats_ += it->second.channels.stats();
    endpoint_to_session_.erase(it->second.key);
    sessions_.erase(it);
}

void ConnectionManager::mark_dirty(Session& session) {
    if (session.dirty) return;
    session.dirty = true;
    dirty_.push_back(session.id);
}

void ConnectionManager::queue_disconnect(const Session& session, DisconnectReason reason) {
    Packet packet;
    packet.type = MessageType::Disconnect;
    packet.sequence = 0;
    if (!encode_disconnect_payload(DisconnectMessage{session.id, reason}, packet.payload)) return;
    outgoing_.push_back({session.endpoint, std::move(packet)});
}

// ------------------------------------------------------------------------------------------ admission

bool ConnectionManager::admit(const NetworkEndpoint& endpoint, std::size_t datagram_bytes, TimePoint now) {
    const auto key = endpoint_key(endpoint);
    if (!packet_limiter_.allow(key, 1.0, now) ||
        !byte_limiter_.allow(key, static_cast<double>(datagram_bytes), now)) {
        ++stats_.rate_limited_packets;
        return false;
    }
    return true;
}

ConnectionManager::Session* ConnectionManager::find_valid(SessionId id, const NetworkEndpoint& endpoint,
                                                          TimePoint now) {
    const auto it = sessions_.find(id);
    if (it == sessions_.end() || !(it->second.endpoint == endpoint)) return nullptr;
    // Idle sessions are dead even if the periodic sweep has not reached them yet.
    if (now - it->second.last_seen >= config_.idle_timeout) {
        remove_session(id);
        ++stats_.sessions_expired;
        return nullptr;
    }
    return &it->second;
}

// ------------------------------------------------------------------------------------------- process

ConnectionResult ConnectionManager::process(const Packet& packet, const NetworkEndpoint& endpoint, TimePoint now) {
    ++stats_.packets_processed;
    if (endpoint.address.empty() || endpoint.port == 0) return error_result(packet, "invalid remote endpoint");

    switch (packet.type) {
        case MessageType::Hello:
            return handle_hello(packet, endpoint, endpoint_key(endpoint), now);

        case MessageType::ChannelData: {
            ChannelMessage message;
            std::string error;
            if (!decode_channel_payload(packet.payload, message, &error)) return error_result(packet, error);
            Session* session = find_valid(message.session_id, endpoint, now);
            if (!session) return error_result(packet, kEndpointMismatch);
            session->last_seen = now;
            session->channels.on_channel_data(message.channel, packet.sequence, message.data, now);
            ConnectionResult result;
            session->channels.drain_delivered(result.delivered);
            mark_dirty(*session);  // acknowledgement due
            result.accepted = true;
            return result;
        }

        case MessageType::Ack: {
            ConnectionResult result;
            AckMessage ack;
            if (!decode_ack_payload(packet.payload, ack, &result.error)) return result;  // never answered
            Session* session = find_valid(ack.session_id, endpoint, now);
            if (!session) {
                result.error = kEndpointMismatch;  // acks are not answered: they would be trivially reflectable
                return result;
            }
            session->last_seen = now;
            session->channels.on_ack(ack, now);
            mark_dirty(*session);  // window may have opened: send queued messages
            result.accepted = true;
            return result;
        }

        case MessageType::Ping: {
            SessionId id = 0;
            std::string error;
            if (!decode_session_id(packet.payload, id, &error)) return error_result(packet, error);
            Session* session = find_valid(id, endpoint, now);
            if (!session) return error_result(packet, kEndpointMismatch);
            session->last_seen = now;
            ConnectionResult result;
            Packet echo;
            echo.type = MessageType::Echo;
            echo.sequence = packet.sequence;
            echo.payload = packet.payload;
            result.response = std::move(echo);
            result.accepted = true;
            return result;
        }

        case MessageType::Disconnect: {
            DisconnectMessage message;
            std::string error;
            if (!decode_disconnect_payload(packet.payload, message, &error)) return error_result(packet, error);
            ConnectionResult result;
            const auto it = sessions_.find(message.session_id);
            if (it != sessions_.end()) {
                if (!(it->second.endpoint == endpoint)) return error_result(packet, kEndpointMismatch);
                remove_session(message.session_id);
                ++stats_.sessions_closed;
                result.disconnected = true;
            }
            // Idempotent: a retransmitted Disconnect for an already closed session is confirmed too.
            Packet confirm;
            confirm.type = MessageType::DisconnectAck;
            confirm.sequence = packet.sequence;
            encode_session_id(message.session_id, confirm.payload);
            result.response = std::move(confirm);
            result.accepted = true;
            return result;
        }

        case MessageType::Echo:
        case MessageType::Error:
        case MessageType::Welcome:
        case MessageType::DisconnectAck: {
            // Reply-class messages are never answered, so two peers can never bounce packets forever.
            ConnectionResult result;
            result.error = "unexpected reply-type message ignored";
            return result;
        }
    }
    return error_result(packet, "message type is not supported by the connection manager");
}

ConnectionResult ConnectionManager::handle_hello(const Packet& packet, const NetworkEndpoint& endpoint,
                                                 const std::string& key, TimePoint now) {
    std::uint64_t nonce = 0;
    std::string error;
    if (!decode_hello_payload(packet.payload, nonce, &error)) return error_result(packet, error);

    // Hello is the only packet that creates server state, so it has its own, much tighter limits.
    if (!hello_limiter_.allow(key, 1.0, now)) {
        ++stats_.rate_limited_hellos;
        ConnectionResult limited;
        limited.rate_limited = true;
        limited.error = "Hello rate limit exceeded";
        return limited;  // silent: no reply to a flooder
    }

    ConnectionResult result;
    SessionId session_id = 0;
    const auto existing = endpoint_to_session_.find(key);
    if (existing != endpoint_to_session_.end()) {
        const auto it = sessions_.find(existing->second);
        if (it == sessions_.end()) {
            endpoint_to_session_.erase(existing);
        } else if (it->second.client_nonce == nonce && now - it->second.last_seen < config_.idle_timeout) {
            // Same client retransmitting its Hello (our Welcome was lost): answer idempotently.
            it->second.last_seen = now;
            session_id = it->second.id;
        } else {
            // A different nonce means the client process restarted on the same endpoint. Its
            // sequence numbers start from scratch, so the old channel state must not be reused.
            if (it->second.client_nonce != nonce) ++stats_.sessions_replaced;
            else ++stats_.sessions_expired;
            remove_session(it->second.id);
        }
    }

    if (session_id == 0) {
        if (sessions_.size() >= config_.max_sessions &&
            (!expired_once_ || now - last_expire_ >= std::chrono::milliseconds(50))) {
            expire(now);  // free capacity held by dead sessions; throttled so a full table is not a CPU attack
        }
        if (sessions_.size() >= config_.max_sessions) {
            ++stats_.rejected_full;
            return error_result(packet, "server is full");
        }
        if (!hello_global_ready_) {
            hello_global_ = TokenBucket(config_.rate.hello_global, now);
            hello_global_ready_ = true;
        }
        if (!hello_global_.try_consume(1.0, now)) {
            ++stats_.rate_limited_hellos;
            result.rate_limited = true;
            result.error = "global session-creation rate limit exceeded";
            return result;  // silent
        }
        session_id = allocate_session_id();
        sessions_.try_emplace(session_id, session_id, endpoint, key, nonce, now, config_.channel);
        endpoint_to_session_[key] = session_id;
        ++stats_.sessions_created;
        result.session_created = true;
    }

    Packet welcome;
    welcome.type = MessageType::Welcome;
    welcome.sequence = packet.sequence;
    if (!encode_session_id(session_id, welcome.payload, &result.error)) {
        remove_session(session_id);
        result.response = make_error(packet.sequence, result.error);
        return result;
    }
    result.accepted = true;
    result.response = std::move(welcome);
    return result;
}

// ------------------------------------------------------------------------------------ application API

SendStatus ConnectionManager::send(SessionId session_id, ChannelId channel, std::span<const std::uint8_t> data,
                                   TimePoint now) {
    const auto it = sessions_.find(session_id);
    if (it == sessions_.end()) return SendStatus::UnknownSession;
    const auto status = it->second.channels.send(channel, data, now);
    if (status == SendStatus::Ok) mark_dirty(it->second);
    return status;
}

// ------------------------------------------------------------------------------------------- timers

void ConnectionManager::service(Session& session, TimePoint now, std::vector<SessionId>& failed) {
    session.channels.update(now);
    std::vector<Packet> packets;
    session.channels.drain_outgoing(packets);
    for (auto& p : packets) outgoing_.push_back({session.endpoint, std::move(p)});
    if (session.channels.failed()) failed.push_back(session.id);
}

void ConnectionManager::update(TimePoint now) {
    std::vector<SessionId> failed;

    // 1. Sessions that just received something (ack due) or have new data to send: act immediately.
    for (const auto id : dirty_) {
        const auto it = sessions_.find(id);
        if (it == sessions_.end()) continue;
        it->second.dirty = false;
        service(it->second, now, failed);
    }
    dirty_.clear();

    // 2. Retransmission timers: scanning every session each call would cost O(sessions) per
    // packet, so the scan runs on a fixed interval instead.
    if (!timer_scanned_ || now - last_timer_scan_ >= config_.timer_interval) {
        timer_scanned_ = true;
        last_timer_scan_ = now;
        for (auto& entry : sessions_) service(entry.second, now, failed);
    }

    // 3. A session whose reliable channel ran out of retransmissions cannot be repaired: close it.
    for (const auto id : failed) {
        const auto it = sessions_.find(id);
        if (it == sessions_.end()) continue;
        queue_disconnect(it->second, DisconnectReason::Timeout);
        remove_session(id);
        ++stats_.sessions_failed;
    }

    // 4. Housekeeping.
    if (!expired_once_ || now - last_expire_ >= config_.expire_interval) expire(now);
    if (!pruned_once_ || now - last_prune_ >= std::chrono::seconds(1)) {
        pruned_once_ = true;
        last_prune_ = now;
        packet_limiter_.prune(now);
        byte_limiter_.prune(now);
        hello_limiter_.prune(now);
    }
}

std::size_t ConnectionManager::expire(TimePoint now) {
    expired_once_ = true;
    last_expire_ = now;
    std::vector<SessionId> expired;
    for (const auto& entry : sessions_) {
        if (now - entry.second.last_seen >= config_.idle_timeout) expired.push_back(entry.first);
    }
    for (const auto id : expired) {
        queue_disconnect(sessions_.at(id), DisconnectReason::Timeout);  // best effort
        remove_session(id);
    }
    stats_.sessions_expired += expired.size();
    return expired.size();
}

std::vector<OutgoingPacket> ConnectionManager::take_outgoing() {
    return std::exchange(outgoing_, {});
}

void ConnectionManager::close_all(DisconnectReason reason) {
    for (const auto& entry : sessions_) {
        queue_disconnect(entry.second, reason);
        retired_stats_ += entry.second.channels.stats();
    }
    sessions_.clear();
    endpoint_to_session_.clear();
    dirty_.clear();
}

// ------------------------------------------------------------------------------------------ queries

std::size_t ConnectionManager::active_sessions() const noexcept { return sessions_.size(); }

bool ConnectionManager::has_session(SessionId session_id) const noexcept {
    return sessions_.find(session_id) != sessions_.end();
}

std::optional<NetworkEndpoint> ConnectionManager::endpoint_of(SessionId session_id) const {
    const auto it = sessions_.find(session_id);
    if (it == sessions_.end()) return std::nullopt;
    return it->second.endpoint;
}

std::optional<ChannelStats> ConnectionManager::channel_stats(SessionId session_id) const {
    const auto it = sessions_.find(session_id);
    if (it == sessions_.end()) return std::nullopt;
    return it->second.channels.stats();
}

std::optional<Duration> ConnectionManager::rtt_of(SessionId session_id) const {
    const auto it = sessions_.find(session_id);
    if (it == sessions_.end() || !it->second.channels.rtt().has_sample()) return std::nullopt;
    return it->second.channels.rtt().srtt();
}

ChannelStats ConnectionManager::aggregate_channel_stats() const {
    ChannelStats total = retired_stats_;
    for (const auto& entry : sessions_) total += entry.second.channels.stats();
    return total;
}

} // namespace dwe::network
