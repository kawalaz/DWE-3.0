#pragma once

#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <optional>

namespace dwe::core {

// Strict number parsing for command-line arguments: digits only, no sign, no leading space,
// no trailing characters, range-checked. (A bare strtoul + cast would silently turn "70000" into
// a different port and "abc" into 0.)
inline std::optional<std::uint64_t> parse_unsigned(const char* text, std::uint64_t min, std::uint64_t max) {
    if (text == nullptr || *text < '0' || *text > '9') return std::nullopt;
    char* end = nullptr;
    errno = 0;
    const unsigned long long value = std::strtoull(text, &end, 10);
    if (errno == ERANGE || *end != '\0' || value < min || value > max) return std::nullopt;
    return static_cast<std::uint64_t>(value);
}

inline std::optional<std::uint16_t> parse_port(const char* text) {
    const auto value = parse_unsigned(text, 1, 65535);
    if (!value) return std::nullopt;
    return static_cast<std::uint16_t>(*value);
}

// Decimal number such as "0.25" or "30"; no sign, no exponent.
inline std::optional<double> parse_fraction(const char* text, double min, double max) {
    if (text == nullptr || *text < '0' || *text > '9') return std::nullopt;
    for (const char* p = text; *p; ++p) {
        if ((*p < '0' || *p > '9') && *p != '.') return std::nullopt;
    }
    char* end = nullptr;
    errno = 0;
    const double value = std::strtod(text, &end);
    if (errno == ERANGE || *end != '\0' || !(value >= min && value <= max)) return std::nullopt;
    return value;
}

} // namespace dwe::core
