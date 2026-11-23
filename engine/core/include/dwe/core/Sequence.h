#pragma once

#include <cstdint>

namespace dwe::core {

// Serial-number arithmetic (RFC 1982 style) for 32-bit wrapping sequence numbers.
// a is "before" b when it is within the half-range behind b. The comparison is only meaningful
// for numbers less than 2^31 apart; DWE windows are at most 64 wide, far below that.
constexpr bool seq_before(std::uint32_t a, std::uint32_t b) noexcept {
    return static_cast<std::int32_t>(a - b) < 0;
}
constexpr bool seq_after(std::uint32_t a, std::uint32_t b) noexcept { return seq_before(b, a); }

// Duplicate / stale detection for unordered datagrams: a 64-wide sliding window anchored at the
// highest sequence seen (the same idea as an IPsec anti-replay window).
class SequenceWindow {
public:
    static constexpr std::uint32_t kWidth = 64;
    enum class Result { Accepted, Duplicate, TooOld };

    // Accepts each sequence at most once. A sequence older than the window is rejected because
    // it can no longer be told apart from a duplicate.
    Result check_and_mark(std::uint32_t seq) noexcept {
        if (!initialized_) {
            initialized_ = true;
            highest_ = seq;
            bits_ = 1;
            return Result::Accepted;
        }
        if (seq_after(seq, highest_)) {
            const std::uint32_t shift = seq - highest_;
            bits_ = shift >= kWidth ? 1 : ((bits_ << shift) | 1);
            highest_ = seq;
            return Result::Accepted;
        }
        const std::uint32_t age = highest_ - seq;  // 0 == the highest itself
        if (age >= kWidth) return Result::TooOld;
        const std::uint64_t mask = std::uint64_t{1} << age;
        if (bits_ & mask) return Result::Duplicate;
        bits_ |= mask;
        return Result::Accepted;
    }

    bool initialized() const noexcept { return initialized_; }
    std::uint32_t highest() const noexcept { return highest_; }

private:
    bool initialized_{false};
    std::uint32_t highest_{0};
    std::uint64_t bits_{0};  // bit i set => (highest_ - i) was seen
};

// "Newest wins" filter for sequenced-unreliable channels: delivers a message only if it is
// newer than every message delivered before; older and duplicate messages are dropped.
class NewestSequence {
public:
    bool accept(std::uint32_t seq) noexcept {
        if (initialized_ && !seq_after(seq, newest_)) return false;
        initialized_ = true;
        newest_ = seq;
        return true;
    }
    bool initialized() const noexcept { return initialized_; }
    std::uint32_t newest() const noexcept { return newest_; }

private:
    bool initialized_{false};
    std::uint32_t newest_{0};
};

} // namespace dwe::core
