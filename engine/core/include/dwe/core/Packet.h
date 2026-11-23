#pragma once

#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
#include <vector>

namespace dwe::core {

inline constexpr std::uint32_t kPacketMagic = 0x44574531; // "DWE1"
inline constexpr std::uint8_t kProtocolVersion = 2;
inline constexpr std::size_t kHeaderSize = 12;
inline constexpr std::size_t kMaxDatagramSize = 1200;
inline constexpr std::size_t kMaxPayloadSize = kMaxDatagramSize - kHeaderSize;
inline constexpr std::size_t kSessionIdSize = 8;
inline constexpr std::size_t kChannelHeaderSize = kSessionIdSize + 1;
inline constexpr std::size_t kMaxChannelDataSize = kMaxPayloadSize - kChannelHeaderSize;
inline constexpr std::size_t kChannelCount = 7;
inline constexpr std::size_t kClientNonceSize = 8;
inline constexpr std::size_t kDisconnectPayloadSize = kSessionIdSize + 1;
// Ack payload = session_id (8) + entry count (1) + count * entry (channel 1, next_expected 4, bits 8).
inline constexpr std::size_t kAckHeaderSize = kSessionIdSize + 1;
inline constexpr std::size_t kAckEntrySize = 13;

enum class MessageType : std::uint8_t {
    Ping = 1,          // Session keep-alive / RTT probe (payload: session ID)
    Echo = 2,          // Reply to Ping (same payload and sequence)
    Error = 3,         // Diagnostic text; never answered
    Hello = 4,         // Client -> server connection request (payload: 8-byte client nonce)
    Welcome = 5,       // Server -> client session assignment
    Disconnect = 6,    // Either direction: close a session (payload: session ID + reason)
    ChannelData = 7,   // Session-scoped application data; sequence = per-channel message sequence
    Ack = 8,           // Acknowledgement of reliable channel messages
    DisconnectAck = 9  // Reply to Disconnect; never answered
};

enum class DisconnectReason : std::uint8_t {
    Requested = 0,       // Peer asked to leave
    Timeout = 1,         // Peer unresponsive or session idle
    ServerShutdown = 2
};

enum class ChannelId : std::uint8_t {
    Control = 0,
    Input = 1,
    Snapshot = 2,
    Combat = 3,
    Inventory = 4,
    Destruction = 5,
    WorldSync = 6
};

// How a logical channel delivers messages. The mapping is a protocol constant (see channel_delivery()).
enum class ChannelDelivery : std::uint8_t {
    Unreliable = 0,          // duplicates and too-old datagrams are dropped; arrival order, no retransmission
    UnreliableSequenced = 1, // only messages newer than the newest delivered one are delivered; no retransmission
    ReliableOrdered = 2      // acknowledged, retransmitted, delivered exactly once and in order
};

using SessionId = std::uint64_t;

struct Packet {
    std::uint8_t version{kProtocolVersion};
    MessageType type{MessageType::Ping};
    std::uint32_t sequence{0};
    std::vector<std::uint8_t> payload;
};

struct ChannelMessage {
    SessionId session_id{0};
    ChannelId channel{ChannelId::Control};
    std::vector<std::uint8_t> data;
};

bool serialize(const Packet& packet, std::vector<std::uint8_t>& output, std::string* error = nullptr);
bool deserialize(std::span<const std::uint8_t> bytes, Packet& output, std::string* error = nullptr);

// Disconnect payload = session_id (8 bytes) + reason (1 byte).
struct DisconnectMessage {
    SessionId session_id{0};
    DisconnectReason reason{DisconnectReason::Requested};
};
bool encode_disconnect_payload(const DisconnectMessage& message, std::vector<std::uint8_t>& output,
                               std::string* error = nullptr);
bool decode_disconnect_payload(std::span<const std::uint8_t> bytes, DisconnectMessage& message,
                               std::string* error = nullptr);

// Hello payload = client nonce (8 bytes, non-zero). A repeated Hello with the same nonce is a
// retransmission; a different nonce from the same endpoint is a new client incarnation.
bool encode_hello_payload(std::uint64_t nonce, std::vector<std::uint8_t>& output,
                          std::string* error = nullptr);
bool decode_hello_payload(std::span<const std::uint8_t> bytes, std::uint64_t& nonce,
                          std::string* error = nullptr);

// One acknowledgement per reliable channel. next_expected acknowledges every message with a
// smaller sequence (serial arithmetic); bit i of received_bits acknowledges next_expected + 1 + i.
struct AckEntry {
    ChannelId channel{ChannelId::Control};
    std::uint32_t next_expected{0};
    std::uint64_t received_bits{0};
};
struct AckMessage {
    SessionId session_id{0};
    std::vector<AckEntry> entries;
};
bool encode_ack_payload(const AckMessage& message, std::vector<std::uint8_t>& output,
                        std::string* error = nullptr);
bool decode_ack_payload(std::span<const std::uint8_t> bytes, AckMessage& message,
                        std::string* error = nullptr);

// Session IDs are encoded as unsigned 64-bit big-endian values.
bool encode_session_id(SessionId session_id, std::vector<std::uint8_t>& output,
                       std::string* error = nullptr);
bool decode_session_id(std::span<const std::uint8_t> bytes, SessionId& session_id,
                       std::string* error = nullptr);

// Channel payload = session_id (8 bytes) + channel_id (1 byte) + application data.
bool encode_channel_payload(const ChannelMessage& message, std::vector<std::uint8_t>& output,
                            std::string* error = nullptr);
bool decode_channel_payload(std::span<const std::uint8_t> bytes, ChannelMessage& message,
                            std::string* error = nullptr);
const char* channel_name(ChannelId channel);

// Delivery guarantee of each logical channel. Fixed by the protocol so both peers agree.
//   Control, Combat, Inventory, Destruction, WorldSync -> ReliableOrdered
//   Input                                              -> Unreliable (every distinct message counts;
//                                                         duplicates and too-old ones are dropped)
//   Snapshot                                           -> UnreliableSequenced (only the newest counts)
ChannelDelivery channel_delivery(ChannelId channel);
const char* delivery_name(ChannelDelivery delivery);
inline bool is_reliable(ChannelId channel) {
    return channel_delivery(channel) == ChannelDelivery::ReliableOrdered;
}
bool is_known_channel_id(std::uint8_t raw);

} // namespace dwe::core
