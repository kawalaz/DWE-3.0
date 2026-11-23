#include "dwe/core/Packet.h"

#include <utility>
#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
#include <vector>

namespace dwe::core {
namespace {
void write_u16(std::vector<std::uint8_t>& out, std::uint16_t v) {
    out.push_back(static_cast<std::uint8_t>((v >> 8) & 0xff));
    out.push_back(static_cast<std::uint8_t>(v & 0xff));
}
void write_u32(std::vector<std::uint8_t>& out, std::uint32_t v) {
    out.push_back(static_cast<std::uint8_t>((v >> 24) & 0xff));
    out.push_back(static_cast<std::uint8_t>((v >> 16) & 0xff));
    out.push_back(static_cast<std::uint8_t>((v >> 8) & 0xff));
    out.push_back(static_cast<std::uint8_t>(v & 0xff));
}
void write_u64(std::vector<std::uint8_t>& out, std::uint64_t v) {
    for (int shift = 56; shift >= 0; shift -= 8) {
        out.push_back(static_cast<std::uint8_t>((v >> shift) & 0xff));
    }
}
std::uint16_t read_u16(const std::uint8_t* p) {
    return static_cast<std::uint16_t>((static_cast<std::uint16_t>(p[0]) << 8) | p[1]);
}
std::uint32_t read_u32(const std::uint8_t* p) {
    return (static_cast<std::uint32_t>(p[0]) << 24) |
           (static_cast<std::uint32_t>(p[1]) << 16) |
           (static_cast<std::uint32_t>(p[2]) << 8) |
            static_cast<std::uint32_t>(p[3]);
}
std::uint64_t read_u64(const std::uint8_t* p) {
    std::uint64_t value = 0;
    for (std::size_t i = 0; i < 8; ++i) value = (value << 8) | p[i];
    return value;
}
void set_error(std::string* out, const char* message) { if (out) *out = message; }
bool is_known_message_type(std::uint8_t raw) {
    return raw >= static_cast<std::uint8_t>(MessageType::Ping) &&
           raw <= static_cast<std::uint8_t>(MessageType::DisconnectAck);
}
bool is_known_channel(std::uint8_t raw) {
    return raw <= static_cast<std::uint8_t>(ChannelId::WorldSync);
}
bool is_known_reason(std::uint8_t raw) {
    return raw <= static_cast<std::uint8_t>(DisconnectReason::ServerShutdown);
}
} // namespace

bool is_known_channel_id(std::uint8_t raw) { return is_known_channel(raw); }

bool serialize(const Packet& packet, std::vector<std::uint8_t>& output, std::string* error) {
    if (packet.version != kProtocolVersion) {
        set_error(error, "unsupported protocol version");
        return false;
    }
    if (!is_known_message_type(static_cast<std::uint8_t>(packet.type))) {
        set_error(error, "unknown message type");
        return false;
    }
    if (packet.payload.size() > kMaxPayloadSize) {
        set_error(error, "payload exceeds datagram limit");
        return false;
    }
    output.clear();
    output.reserve(kHeaderSize + packet.payload.size());
    write_u32(output, kPacketMagic);
    output.push_back(packet.version);
    output.push_back(static_cast<std::uint8_t>(packet.type));
    write_u32(output, packet.sequence);
    write_u16(output, static_cast<std::uint16_t>(packet.payload.size()));
    output.insert(output.end(), packet.payload.begin(), packet.payload.end());
    return true;
}

bool deserialize(std::span<const std::uint8_t> bytes, Packet& output, std::string* error) {
    if (bytes.size() < kHeaderSize) {
        set_error(error, "packet shorter than header");
        return false;
    }
    if (bytes.size() > kMaxDatagramSize) {
        set_error(error, "packet exceeds datagram limit");
        return false;
    }
    if (read_u32(bytes.data()) != kPacketMagic) {
        set_error(error, "invalid packet magic");
        return false;
    }
    const auto version = bytes[4];
    if (version != kProtocolVersion) {
        set_error(error, "unsupported protocol version");
        return false;
    }
    const auto payload_size = read_u16(bytes.data() + 10);
    if (payload_size > kMaxPayloadSize || bytes.size() != kHeaderSize + payload_size) {
        set_error(error, "invalid payload length");
        return false;
    }
    const auto raw_type = bytes[5];
    if (!is_known_message_type(raw_type)) {
        set_error(error, "unknown message type");
        return false;
    }
    output.version = version;
    output.type = static_cast<MessageType>(raw_type);
    output.sequence = read_u32(bytes.data() + 6);
    output.payload.assign(bytes.begin() + static_cast<std::ptrdiff_t>(kHeaderSize), bytes.end());
    return true;
}

bool encode_session_id(SessionId session_id, std::vector<std::uint8_t>& output, std::string* error) {
    if (session_id == 0) {
        set_error(error, "session ID must not be zero");
        return false;
    }
    output.clear();
    output.reserve(kSessionIdSize);
    write_u64(output, session_id);
    return true;
}

bool decode_session_id(std::span<const std::uint8_t> bytes, SessionId& session_id, std::string* error) {
    if (bytes.size() != kSessionIdSize) {
        set_error(error, "session ID payload must be exactly 8 bytes");
        return false;
    }
    session_id = read_u64(bytes.data());
    if (session_id == 0) {
        set_error(error, "session ID must not be zero");
        return false;
    }
    return true;
}

bool encode_channel_payload(const ChannelMessage& message, std::vector<std::uint8_t>& output,
                            std::string* error) {
    if (message.session_id == 0) {
        set_error(error, "session ID must not be zero");
        return false;
    }
    if (!is_known_channel(static_cast<std::uint8_t>(message.channel))) {
        set_error(error, "unknown channel ID");
        return false;
    }
    if (message.data.size() > kMaxPayloadSize - kChannelHeaderSize) {
        set_error(error, "channel data exceeds packet payload limit");
        return false;
    }
    output.clear();
    output.reserve(kChannelHeaderSize + message.data.size());
    write_u64(output, message.session_id);
    output.push_back(static_cast<std::uint8_t>(message.channel));
    output.insert(output.end(), message.data.begin(), message.data.end());
    return true;
}

bool decode_channel_payload(std::span<const std::uint8_t> bytes, ChannelMessage& message,
                            std::string* error) {
    if (bytes.size() < kChannelHeaderSize) {
        set_error(error, "channel payload shorter than its header");
        return false;
    }
    const auto raw_channel = bytes[kSessionIdSize];
    if (!is_known_channel(raw_channel)) {
        set_error(error, "unknown channel ID");
        return false;
    }
    const auto session_id = read_u64(bytes.data());
    if (session_id == 0) {
        set_error(error, "session ID must not be zero");
        return false;
    }
    message.session_id = session_id;
    message.channel = static_cast<ChannelId>(raw_channel);
    message.data.assign(bytes.begin() + static_cast<std::ptrdiff_t>(kChannelHeaderSize), bytes.end());
    return true;
}

bool encode_disconnect_payload(const DisconnectMessage& message, std::vector<std::uint8_t>& output,
                               std::string* error) {
    if (message.session_id == 0) {
        set_error(error, "session ID must not be zero");
        return false;
    }
    if (!is_known_reason(static_cast<std::uint8_t>(message.reason))) {
        set_error(error, "unknown disconnect reason");
        return false;
    }
    output.clear();
    output.reserve(kDisconnectPayloadSize);
    write_u64(output, message.session_id);
    output.push_back(static_cast<std::uint8_t>(message.reason));
    return true;
}

bool decode_disconnect_payload(std::span<const std::uint8_t> bytes, DisconnectMessage& message,
                               std::string* error) {
    if (bytes.size() != kDisconnectPayloadSize) {
        set_error(error, "disconnect payload must be exactly 9 bytes");
        return false;
    }
    const auto session_id = read_u64(bytes.data());
    if (session_id == 0) {
        set_error(error, "session ID must not be zero");
        return false;
    }
    if (!is_known_reason(bytes[kSessionIdSize])) {
        set_error(error, "unknown disconnect reason");
        return false;
    }
    message.session_id = session_id;
    message.reason = static_cast<DisconnectReason>(bytes[kSessionIdSize]);
    return true;
}

bool encode_hello_payload(std::uint64_t nonce, std::vector<std::uint8_t>& output, std::string* error) {
    if (nonce == 0) {
        set_error(error, "client nonce must not be zero");
        return false;
    }
    output.clear();
    output.reserve(kClientNonceSize);
    write_u64(output, nonce);
    return true;
}

bool decode_hello_payload(std::span<const std::uint8_t> bytes, std::uint64_t& nonce, std::string* error) {
    if (bytes.size() != kClientNonceSize) {
        set_error(error, "Hello payload must be exactly 8 bytes (client nonce)");
        return false;
    }
    const auto value = read_u64(bytes.data());
    if (value == 0) {
        set_error(error, "client nonce must not be zero");
        return false;
    }
    nonce = value;
    return true;
}

bool encode_ack_payload(const AckMessage& message, std::vector<std::uint8_t>& output, std::string* error) {
    if (message.session_id == 0) {
        set_error(error, "session ID must not be zero");
        return false;
    }
    if (message.entries.empty() || message.entries.size() > kChannelCount) {
        set_error(error, "ack must carry 1 to 7 entries");
        return false;
    }
    std::uint32_t seen_channels = 0;
    for (const auto& entry : message.entries) {
        const auto raw = static_cast<std::uint8_t>(entry.channel);
        if (!is_known_channel(raw) || !is_reliable(entry.channel)) {
            set_error(error, "ack entry for a channel that is not reliable");
            return false;
        }
        if (seen_channels & (1u << raw)) {
            set_error(error, "duplicate channel in ack");
            return false;
        }
        seen_channels |= (1u << raw);
    }
    output.clear();
    output.reserve(kAckHeaderSize + message.entries.size() * kAckEntrySize);
    write_u64(output, message.session_id);
    output.push_back(static_cast<std::uint8_t>(message.entries.size()));
    for (const auto& entry : message.entries) {
        output.push_back(static_cast<std::uint8_t>(entry.channel));
        write_u32(output, entry.next_expected);
        write_u64(output, entry.received_bits);
    }
    return true;
}

bool decode_ack_payload(std::span<const std::uint8_t> bytes, AckMessage& message, std::string* error) {
    if (bytes.size() < kAckHeaderSize) {
        set_error(error, "ack payload shorter than its header");
        return false;
    }
    const auto session_id = read_u64(bytes.data());
    if (session_id == 0) {
        set_error(error, "session ID must not be zero");
        return false;
    }
    const std::size_t count = bytes[kSessionIdSize];
    if (count == 0 || count > kChannelCount) {
        set_error(error, "ack must carry 1 to 7 entries");
        return false;
    }
    if (bytes.size() != kAckHeaderSize + count * kAckEntrySize) {
        set_error(error, "ack payload length does not match entry count");
        return false;
    }
    AckMessage decoded;
    decoded.session_id = session_id;
    decoded.entries.reserve(count);
    std::uint32_t seen_channels = 0;
    const std::uint8_t* p = bytes.data() + kAckHeaderSize;
    for (std::size_t i = 0; i < count; ++i, p += kAckEntrySize) {
        if (!is_known_channel(p[0]) || !is_reliable(static_cast<ChannelId>(p[0]))) {
            set_error(error, "ack entry for a channel that is not reliable");
            return false;
        }
        if (seen_channels & (1u << p[0])) {
            set_error(error, "duplicate channel in ack");
            return false;
        }
        seen_channels |= (1u << p[0]);
        decoded.entries.push_back(AckEntry{static_cast<ChannelId>(p[0]), read_u32(p + 1), read_u64(p + 5)});
    }
    message = std::move(decoded);
    return true;
}

ChannelDelivery channel_delivery(ChannelId channel) {
    switch (channel) {
        case ChannelId::Input:
            return ChannelDelivery::Unreliable;
        case ChannelId::Snapshot:
            return ChannelDelivery::UnreliableSequenced;
        case ChannelId::Control:
        case ChannelId::Combat:
        case ChannelId::Inventory:
        case ChannelId::Destruction:
        case ChannelId::WorldSync:
            return ChannelDelivery::ReliableOrdered;
    }
    return ChannelDelivery::Unreliable;  // unknown IDs are never reliable
}

const char* delivery_name(ChannelDelivery delivery) {
    switch (delivery) {
        case ChannelDelivery::Unreliable: return "Unreliable";
        case ChannelDelivery::UnreliableSequenced: return "UnreliableSequenced";
        case ChannelDelivery::ReliableOrdered: return "ReliableOrdered";
    }
    return "Unknown";
}

const char* channel_name(ChannelId channel) {
    switch (channel) {
        case ChannelId::Control: return "Control";
        case ChannelId::Input: return "Input";
        case ChannelId::Snapshot: return "Snapshot";
        case ChannelId::Combat: return "Combat";
        case ChannelId::Inventory: return "Inventory";
        case ChannelId::Destruction: return "Destruction";
        case ChannelId::WorldSync: return "WorldSync";
    }
    return "Unknown";
}

} // namespace dwe::core
