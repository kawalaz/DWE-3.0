#include "dwe/core/Packet.h"

#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>

namespace {
int failures = 0;
void check(bool condition, const char* message) {
    if (!condition) {
        std::cerr << "FAIL: " << message << '\n';
        ++failures;
    }
}
}

int main() {
    using namespace dwe::core;
    Packet source;
    source.type = MessageType::Echo;
    source.sequence = 0x12345678;
    source.payload = {'D', 'W', 'E'};
    std::vector<std::uint8_t> bytes;
    std::string error;
    check(serialize(source, bytes, &error), "serialize valid packet");

    Packet decoded;
    check(deserialize(bytes, decoded, &error), "deserialize valid packet");
    check(decoded.sequence == source.sequence, "sequence round trip");
    check(decoded.type == source.type, "type round trip");
    check(decoded.payload == source.payload, "payload round trip");

    // Exact wire layout (big endian), as documented in docs/protocols/dwep-v1.md.
    const std::vector<std::uint8_t> expected_wire = {
        'D', 'W', 'E', '1',      // magic
        2,                       // version (DWEP v2)
        2,                       // type: Echo
        0x12, 0x34, 0x56, 0x78,  // sequence
        0x00, 0x03,              // payload length
        'D', 'W', 'E'};          // payload
    check(bytes == expected_wire, "wire layout matches DWEP v2");

    auto malformed = bytes;
    malformed[0] = 0;
    check(!deserialize(malformed, decoded, &error), "reject invalid magic");

    Packet too_large;
    too_large.payload.resize(kMaxPayloadSize + 1);
    check(!serialize(too_large, bytes, &error), "reject oversized payload");
    bytes = expected_wire;

    // Boundaries.
    Packet empty;
    std::vector<std::uint8_t> empty_bytes;
    check(serialize(empty, empty_bytes, &error), "serialize empty payload");
    check(empty_bytes.size() == kHeaderSize, "empty packet is exactly the header");
    check(deserialize(empty_bytes, decoded, &error) && decoded.payload.empty(), "empty payload round trip");

    Packet largest;
    largest.payload.assign(kMaxPayloadSize, 0xAB);
    std::vector<std::uint8_t> largest_bytes;
    check(serialize(largest, largest_bytes, &error), "serialize max payload");
    check(largest_bytes.size() == kMaxDatagramSize, "max packet is exactly kMaxDatagramSize");
    check(deserialize(largest_bytes, decoded, &error) && decoded.payload == largest.payload,
          "max payload round trip");

    // Datagram one byte over the limit, even with an internally consistent header.
    auto over_limit = largest_bytes;
    over_limit.push_back(0);
    over_limit[10] = static_cast<std::uint8_t>((kMaxPayloadSize + 1) >> 8);
    over_limit[11] = static_cast<std::uint8_t>((kMaxPayloadSize + 1) & 0xff);
    check(!deserialize(over_limit, decoded, &error), "reject datagram larger than kMaxDatagramSize");

    // Malformed input.
    check(!deserialize(std::vector<std::uint8_t>{}, decoded, &error), "reject empty input");
    check(!deserialize(std::vector<std::uint8_t>(bytes.begin(), bytes.begin() + 11), decoded, &error),
          "reject input shorter than header");

    auto bad_version = bytes;
    bad_version[4] = 1;  // v1 peers are rejected, not silently misparsed
    check(!deserialize(bad_version, decoded, &error), "reject unsupported version");

    for (const std::uint8_t bad_type : {std::uint8_t{0}, std::uint8_t{10}, std::uint8_t{255}}) {
        auto bad = bytes;
        bad[5] = bad_type;
        check(!deserialize(bad, decoded, &error), "reject unknown message type on decode");
    }

    auto len_too_big = bytes;
    len_too_big[11] = 4;  // declares 4 payload bytes, only 3 present
    check(!deserialize(len_too_big, decoded, &error), "reject declared length > actual");

    auto trailing = bytes;
    trailing.push_back('X');  // 4 payload bytes present, only 3 declared
    check(!deserialize(trailing, decoded, &error), "reject declared length < actual (trailing bytes)");

    // serialize() must be symmetric with deserialize().
    Packet bad_type_packet;
    bad_type_packet.type = static_cast<MessageType>(99);
    check(!serialize(bad_type_packet, empty_bytes, &error), "reject unknown message type on encode");
    Packet bad_version_packet;
    bad_version_packet.version = 1;
    check(!serialize(bad_version_packet, empty_bytes, &error), "reject unsupported version on encode");

    ChannelMessage channel;
    channel.session_id = 0x0102030405060708ULL;
    channel.channel = ChannelId::Combat;
    channel.data = {0xAA, 0xBB, 0xCC};
    std::vector<std::uint8_t> channel_bytes;
    check(encode_channel_payload(channel, channel_bytes, &error), "encode channel payload");
    check(channel_bytes.size() == kChannelHeaderSize + channel.data.size(), "channel payload includes 9-byte envelope");
    check(channel_bytes[0] == 1 && channel_bytes[7] == 8, "session ID uses big-endian encoding");
    ChannelMessage channel_decoded;
    check(decode_channel_payload(channel_bytes, channel_decoded, &error), "decode channel payload");
    check(channel_decoded.session_id == channel.session_id && channel_decoded.channel == channel.channel &&
          channel_decoded.data == channel.data, "channel payload round trip");
    auto bad_channel = channel_bytes;
    bad_channel[8] = 99;
    check(!decode_channel_payload(bad_channel, channel_decoded, &error), "reject unknown channel ID");
    check(!encode_session_id(0, channel_bytes, &error), "reject zero session ID");


    // ---- DWEP v2 additions -------------------------------------------------------------------
    for (const auto type : {MessageType::Ack, MessageType::DisconnectAck}) {
        Packet p;
        p.type = type;
        std::vector<std::uint8_t> wire;
        check(serialize(p, wire, &error) && deserialize(wire, decoded, &error) && decoded.type == type,
              "Ack/DisconnectAck message types round trip");
    }

    // Channel policy is a protocol constant.
    check(channel_delivery(ChannelId::Control) == ChannelDelivery::ReliableOrdered, "Control is reliable");
    check(channel_delivery(ChannelId::Combat) == ChannelDelivery::ReliableOrdered, "Combat is reliable");
    check(channel_delivery(ChannelId::Inventory) == ChannelDelivery::ReliableOrdered, "Inventory is reliable");
    check(channel_delivery(ChannelId::Destruction) == ChannelDelivery::ReliableOrdered, "Destruction is reliable");
    check(channel_delivery(ChannelId::WorldSync) == ChannelDelivery::ReliableOrdered, "WorldSync is reliable");
    check(channel_delivery(ChannelId::Input) == ChannelDelivery::Unreliable, "Input is unreliable with duplicate detection");
    check(channel_delivery(ChannelId::Snapshot) == ChannelDelivery::UnreliableSequenced, "Snapshot is sequenced");
    check(kMaxChannelDataSize == kMaxDatagramSize - kHeaderSize - kChannelHeaderSize, "channel data limit");

    // Ack codec.
    AckMessage ack;
    ack.session_id = 0x1122334455667788ULL;
    ack.entries = {{ChannelId::Control, 0x0A0B0C0D, 0x0102030405060708ULL},
                   {ChannelId::Combat, 7, 0}};
    std::vector<std::uint8_t> ack_bytes;
    check(encode_ack_payload(ack, ack_bytes, &error), "encode ack");
    check(ack_bytes.size() == kAckHeaderSize + 2 * kAckEntrySize, "ack size");
    const std::vector<std::uint8_t> ack_prefix = {0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 2,
                                                  0, 0x0A, 0x0B, 0x0C, 0x0D,
                                                  1, 2, 3, 4, 5, 6, 7, 8};
    check(std::vector<std::uint8_t>(ack_bytes.begin(), ack_bytes.begin() + 22) == ack_prefix,
          "ack wire layout is big-endian");
    AckMessage ack_decoded;
    check(decode_ack_payload(ack_bytes, ack_decoded, &error), "decode ack");
    check(ack_decoded.session_id == ack.session_id && ack_decoded.entries.size() == 2 &&
              ack_decoded.entries[0].next_expected == 0x0A0B0C0D &&
              ack_decoded.entries[0].received_bits == 0x0102030405060708ULL &&
              ack_decoded.entries[1].channel == ChannelId::Combat,
          "ack round trip");

    auto bad_ack = ack_bytes;
    bad_ack.pop_back();
    check(!decode_ack_payload(bad_ack, ack_decoded, &error), "reject ack with truncated entry");
    bad_ack = ack_bytes;
    bad_ack.push_back(0);
    check(!decode_ack_payload(bad_ack, ack_decoded, &error), "reject ack with trailing bytes");
    bad_ack = ack_bytes;
    bad_ack[8] = 0;
    check(!decode_ack_payload(bad_ack, ack_decoded, &error), "reject ack with zero entries");
    bad_ack[8] = 8;
    check(!decode_ack_payload(bad_ack, ack_decoded, &error), "reject ack with too many entries");
    bad_ack = ack_bytes;
    bad_ack[9] = static_cast<std::uint8_t>(ChannelId::Input);
    check(!decode_ack_payload(bad_ack, ack_decoded, &error), "reject ack for an unreliable channel");
    bad_ack = ack_bytes;
    bad_ack[9] = 99;
    check(!decode_ack_payload(bad_ack, ack_decoded, &error), "reject ack for an unknown channel");
    bad_ack = ack_bytes;
    bad_ack[9 + kAckEntrySize] = bad_ack[9];  // second entry repeats the first channel
    check(!decode_ack_payload(bad_ack, ack_decoded, &error), "reject duplicate channel in ack");
    bad_ack = ack_bytes;
    for (int i = 0; i < 8; ++i) bad_ack[static_cast<std::size_t>(i)] = 0;
    check(!decode_ack_payload(bad_ack, ack_decoded, &error), "reject ack with zero session ID");
    check(!decode_ack_payload(std::vector<std::uint8_t>(5, 1), ack_decoded, &error), "reject short ack");
    AckMessage empty_ack;
    empty_ack.session_id = 1;
    check(!encode_ack_payload(empty_ack, ack_bytes, &error), "reject encoding an empty ack");
    AckMessage unreliable_ack;
    unreliable_ack.session_id = 1;
    unreliable_ack.entries = {{ChannelId::Snapshot, 1, 0}};
    check(!encode_ack_payload(unreliable_ack, ack_bytes, &error), "reject encoding an ack for an unreliable channel");

    // Disconnect and Hello codecs.
    DisconnectMessage bye{42, DisconnectReason::Timeout};
    std::vector<std::uint8_t> bye_bytes;
    check(encode_disconnect_payload(bye, bye_bytes, &error) && bye_bytes.size() == kDisconnectPayloadSize,
          "encode disconnect");
    DisconnectMessage bye_decoded;
    check(decode_disconnect_payload(bye_bytes, bye_decoded, &error) && bye_decoded.session_id == 42 &&
              bye_decoded.reason == DisconnectReason::Timeout,
          "disconnect round trip");
    bye_bytes[8] = 77;
    check(!decode_disconnect_payload(bye_bytes, bye_decoded, &error), "reject unknown disconnect reason");
    check(!decode_disconnect_payload(std::vector<std::uint8_t>(8, 1), bye_decoded, &error),
          "reject disconnect without reason byte");
    check(!encode_disconnect_payload(DisconnectMessage{0, DisconnectReason::Requested}, bye_bytes, &error),
          "reject disconnect with zero session ID");

    std::vector<std::uint8_t> hello_bytes;
    std::uint64_t nonce = 0;
    check(encode_hello_payload(0xDEADBEEF12345678ULL, hello_bytes, &error) && hello_bytes.size() == kClientNonceSize,
          "encode hello nonce");
    check(decode_hello_payload(hello_bytes, nonce, &error) && nonce == 0xDEADBEEF12345678ULL, "hello nonce round trip");
    check(!encode_hello_payload(0, hello_bytes, &error), "reject zero nonce on encode");
    check(!decode_hello_payload(std::vector<std::uint8_t>{}, nonce, &error), "reject Hello without nonce");
    check(!decode_hello_payload(std::vector<std::uint8_t>(8, 0), nonce, &error), "reject zero nonce on decode");
    check(!decode_hello_payload(std::vector<std::uint8_t>(9, 1), nonce, &error), "reject oversized Hello payload");

    check(failures == 0, "all tests pass");
    if (failures == 0) std::cout << "All DWE packet tests passed.\n";
    return failures == 0 ? EXIT_SUCCESS : EXIT_FAILURE;
}
