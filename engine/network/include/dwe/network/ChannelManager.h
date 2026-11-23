#pragma once

#include "dwe/core/Packet.h"
#include "dwe/core/Sequence.h"
#include "dwe/network/ReliableChannel.h"
#include "dwe/network/RttEstimator.h"
#include "dwe/network/Time.h"

#include <array>
#include <cstdint>
#include <span>
#include <vector>

namespace dwe::network {

struct ChannelConfig {
    ReliableConfig reliable;
    RttConfig rtt;
};

enum class SendStatus {
    Ok,
    QueueFull,       // reliable backlog is full: back off and retry later
    TooLarge,        // message does not fit into one datagram (no fragmentation in DWEP v2)
    Failed,          // the session's reliable delivery has failed; it is being closed
    UnknownChannel,
    UnknownSession   // used by ConnectionManager::send
};
const char* send_status_name(SendStatus status);

struct ChannelStats {
    std::uint64_t unreliable_sent = 0;
    std::uint64_t unreliable_delivered = 0;
    std::uint64_t duplicates_dropped = 0;  // unreliable channels: same sequence seen again
    std::uint64_t stale_dropped = 0;       // unreliable channels: older than the window / newest delivered
    std::uint64_t invalid_channel_data = 0;
    std::uint64_t acks_sent = 0;
    std::uint64_t acks_received = 0;
    ReliableStats reliable;                // summed over the reliable channels

    ChannelStats& operator+=(const ChannelStats& other);
};

// All channels of one session: per-channel sequencing, acknowledgement and retransmission.
// Pure state machine like ReliableChannel: no sockets, time is passed in.
//
//   application --send()--> ChannelManager --drain_outgoing()--> packets for the peer
//   peer packets --on_channel_data()/on_ack()--> ChannelManager --drain_delivered()--> application
//
// update() must be called regularly (and after receiving packets) to emit first transmissions,
// retransmissions and acknowledgements.
class ChannelManager {
public:
    ChannelManager(dwe::core::SessionId session_id, ChannelConfig config = {});

    SendStatus send(dwe::core::ChannelId channel, std::span<const std::uint8_t> data, TimePoint now);

    // Inbound. `sequence` is the envelope sequence of the ChannelData packet.
    void on_channel_data(dwe::core::ChannelId channel, std::uint32_t sequence,
                         std::span<const std::uint8_t> data, TimePoint now);
    void on_ack(const dwe::core::AckMessage& ack, TimePoint now);
    // Feeds an RTT measurement obtained outside the ack path (Ping/Echo).
    void add_rtt_sample(Duration rtt) { rtt_.add_sample(rtt); }

    void update(TimePoint now);

    // Both append to `out` and empty the internal queue.
    void drain_outgoing(std::vector<dwe::core::Packet>& out);
    void drain_delivered(std::vector<dwe::core::ChannelMessage>& out);

    bool failed() const { return failed_; }
    bool has_outgoing() const { return !outgoing_.empty(); }
    // True when every reliable message handed to send() has been acknowledged.
    bool reliable_idle() const;
    const RttEstimator& rtt() const { return rtt_; }
    ChannelStats stats() const;
    const ReliableChannel* reliable_channel(dwe::core::ChannelId channel) const;

private:
    struct State {
        dwe::core::ChannelDelivery delivery{dwe::core::ChannelDelivery::Unreliable};
        ReliableChannel reliable;                  // used by ReliableOrdered channels
        std::uint32_t next_send_sequence{1};       // used by unreliable channels
        dwe::core::SequenceWindow window;          // Unreliable: duplicate / too-old detection
        dwe::core::NewestSequence newest;          // UnreliableSequenced: newest wins
        std::uint64_t sent{0}, delivered{0}, duplicates{0}, stale{0};
    };

    State* state_for(dwe::core::ChannelId channel);
    void push_data_packet(dwe::core::ChannelId channel, std::uint32_t sequence, std::span<const std::uint8_t> data);

    dwe::core::SessionId session_id_;
    ChannelConfig config_;
    RttEstimator rtt_;
    std::array<State, dwe::core::kChannelCount> channels_;
    std::vector<dwe::core::Packet> outgoing_;
    std::vector<dwe::core::ChannelMessage> delivered_;
    std::vector<ReliableChannel::Transmission> scratch_;
    std::vector<std::vector<std::uint8_t>> scratch_delivered_;
    std::uint32_t ack_packet_counter_{0};
    std::uint64_t acks_sent_{0}, acks_received_{0}, invalid_channel_data_{0};
    bool failed_{false};
};

} // namespace dwe::network
