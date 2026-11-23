#include "dwe/network/ChannelManager.h"

#include <utility>
#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>

namespace dwe::network {

using namespace dwe::core;

const char* send_status_name(SendStatus status) {
    switch (status) {
        case SendStatus::Ok: return "ok";
        case SendStatus::QueueFull: return "queue full";
        case SendStatus::TooLarge: return "message too large";
        case SendStatus::Failed: return "reliable delivery failed";
        case SendStatus::UnknownChannel: return "unknown channel";
        case SendStatus::UnknownSession: return "unknown session";
    }
    return "unknown";
}

ChannelStats& ChannelStats::operator+=(const ChannelStats& o) {
    unreliable_sent += o.unreliable_sent;
    unreliable_delivered += o.unreliable_delivered;
    duplicates_dropped += o.duplicates_dropped;
    stale_dropped += o.stale_dropped;
    invalid_channel_data += o.invalid_channel_data;
    acks_sent += o.acks_sent;
    acks_received += o.acks_received;
    reliable.first_transmissions += o.reliable.first_transmissions;
    reliable.retransmissions += o.reliable.retransmissions;
    reliable.fast_retransmissions += o.reliable.fast_retransmissions;
    reliable.acked += o.reliable.acked;
    reliable.delivered += o.reliable.delivered;
    reliable.duplicates += o.reliable.duplicates;
    reliable.out_of_window += o.reliable.out_of_window;
    reliable.buffer_full += o.reliable.buffer_full;
    reliable.invalid_acks += o.reliable.invalid_acks;
    reliable.stale_acks += o.reliable.stale_acks;
    return *this;
}

ChannelManager::ChannelManager(SessionId session_id, ChannelConfig config)
    : session_id_(session_id), config_(config), rtt_(config.rtt) {
    for (std::size_t i = 0; i < channels_.size(); ++i) {
        channels_[i].delivery = channel_delivery(static_cast<ChannelId>(i));
        channels_[i].reliable = ReliableChannel(config_.reliable);
        channels_[i].next_send_sequence = config_.reliable.initial_sequence;
    }
}

ChannelManager::State* ChannelManager::state_for(ChannelId channel) {
    const auto index = static_cast<std::size_t>(channel);
    return index < channels_.size() ? &channels_[index] : nullptr;
}

const ReliableChannel* ChannelManager::reliable_channel(ChannelId channel) const {
    const auto index = static_cast<std::size_t>(channel);
    return index < channels_.size() ? &channels_[index].reliable : nullptr;
}

void ChannelManager::push_data_packet(ChannelId channel, std::uint32_t sequence, std::span<const std::uint8_t> data) {
    Packet packet;
    packet.type = MessageType::ChannelData;
    packet.sequence = sequence;
    ChannelMessage message;
    message.session_id = session_id_;
    message.channel = channel;
    message.data.assign(data.begin(), data.end());
    encode_channel_payload(message, packet.payload);  // size was validated by send()
    outgoing_.push_back(std::move(packet));
}

SendStatus ChannelManager::send(ChannelId channel, std::span<const std::uint8_t> data, TimePoint) {
    State* state = state_for(channel);
    if (!state) return SendStatus::UnknownChannel;
    if (failed_) return SendStatus::Failed;
    if (data.size() > kMaxChannelDataSize) return SendStatus::TooLarge;

    if (state->delivery == ChannelDelivery::ReliableOrdered) {
        // Sequence assignment and transmission happen in update(), where the window is enforced.
        return state->reliable.enqueue(std::vector<std::uint8_t>(data.begin(), data.end()))
                   ? SendStatus::Ok
                   : SendStatus::QueueFull;
    }
    push_data_packet(channel, state->next_send_sequence++, data);
    ++state->sent;
    return SendStatus::Ok;
}

void ChannelManager::on_channel_data(ChannelId channel, std::uint32_t sequence,
                                     std::span<const std::uint8_t> data, TimePoint) {
    State* state = state_for(channel);
    if (!state || failed_) {
        ++invalid_channel_data_;
        return;
    }
    auto deliver = [&](std::span<const std::uint8_t> bytes) {
        ChannelMessage message;
        message.session_id = session_id_;
        message.channel = channel;
        message.data.assign(bytes.begin(), bytes.end());
        delivered_.push_back(std::move(message));
        ++state->delivered;
    };

    switch (state->delivery) {
        case ChannelDelivery::ReliableOrdered: {
            scratch_delivered_.clear();
            state->reliable.receive(sequence, data, scratch_delivered_);
            for (auto& bytes : scratch_delivered_) {
                ChannelMessage message;
                message.session_id = session_id_;
                message.channel = channel;
                message.data = std::move(bytes);
                delivered_.push_back(std::move(message));
            }
            break;
        }
        case ChannelDelivery::Unreliable:
            switch (state->window.check_and_mark(sequence)) {
                case SequenceWindow::Result::Accepted: deliver(data); break;
                case SequenceWindow::Result::Duplicate: ++state->duplicates; break;
                case SequenceWindow::Result::TooOld: ++state->stale; break;
            }
            break;
        case ChannelDelivery::UnreliableSequenced:
            if (state->newest.accept(sequence)) deliver(data);
            else ++state->stale;
            break;
    }
}

void ChannelManager::on_ack(const AckMessage& ack, TimePoint now) {
    if (failed_) return;
    ++acks_received_;
    for (const auto& entry : ack.entries) {
        State* state = state_for(entry.channel);
        if (!state || state->delivery != ChannelDelivery::ReliableOrdered) continue;
        state->reliable.on_ack(entry.next_expected, entry.received_bits, now, rtt_);
    }
}

void ChannelManager::update(TimePoint now) {
    if (failed_) return;

    for (std::size_t i = 0; i < channels_.size(); ++i) {
        State& state = channels_[i];
        if (state.delivery != ChannelDelivery::ReliableOrdered) continue;
        scratch_.clear();
        state.reliable.collect(now, rtt_, scratch_);
        for (const auto& tx : scratch_) {
            push_data_packet(static_cast<ChannelId>(i), tx.sequence, *tx.data);
        }
        if (state.reliable.failed()) {
            failed_ = true;
            return;
        }
    }

    // One Ack packet carries the state of every reliable channel that received something.
    AckMessage ack;
    ack.session_id = session_id_;
    for (std::size_t i = 0; i < channels_.size(); ++i) {
        State& state = channels_[i];
        if (state.delivery != ChannelDelivery::ReliableOrdered || !state.reliable.ack_pending()) continue;
        ack.entries.push_back(AckEntry{static_cast<ChannelId>(i), state.reliable.next_expected(),
                                       state.reliable.received_bits()});
        state.reliable.ack_sent();
    }
    if (!ack.entries.empty()) {
        Packet packet;
        packet.type = MessageType::Ack;
        packet.sequence = ++ack_packet_counter_;
        encode_ack_payload(ack, packet.payload);
        outgoing_.push_back(std::move(packet));
        ++acks_sent_;
    }
}

void ChannelManager::drain_outgoing(std::vector<Packet>& out) {
    for (auto& p : outgoing_) out.push_back(std::move(p));
    outgoing_.clear();
}

void ChannelManager::drain_delivered(std::vector<ChannelMessage>& out) {
    for (auto& m : delivered_) out.push_back(std::move(m));
    delivered_.clear();
}

bool ChannelManager::reliable_idle() const {
    for (const auto& state : channels_) {
        if (state.delivery == ChannelDelivery::ReliableOrdered && !state.reliable.send_idle()) return false;
    }
    return true;
}

ChannelStats ChannelManager::stats() const {
    ChannelStats total;
    for (const auto& state : channels_) {
        if (state.delivery == ChannelDelivery::ReliableOrdered) {
            ChannelStats one;
            one.reliable = state.reliable.stats();
            total += one;
        } else {
            total.unreliable_sent += state.sent;
            total.unreliable_delivered += state.delivered;
            total.duplicates_dropped += state.duplicates;
            total.stale_dropped += state.stale;
        }
    }
    total.acks_sent = acks_sent_;
    total.acks_received = acks_received_;
    total.invalid_channel_data = invalid_channel_data_;
    return total;
}

} // namespace dwe::network
