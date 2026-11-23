#include "dwe/core/Cli.h"

#include <cstdlib>
#include <iostream>
#include <string>
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
    check(parse_port("27015") == std::uint16_t{27015}, "valid port");
    check(parse_port("1") == std::uint16_t{1} && parse_port("65535") == std::uint16_t{65535}, "port bounds");
    for (const char* bad : {"0", "65536", "70000", "99999999999999999999", "-1", "+5", " 80", "80 ", "12ab", "abc", "", "0x50", "1e3"}) {
        check(!parse_port(bad).has_value(), (std::string("rejects port '") + bad + "'").c_str());
    }
    check(!parse_port(nullptr).has_value(), "rejects null");
    check(parse_unsigned("128", 1, 1000) == 128u && !parse_unsigned("1001", 1, 1000) && !parse_unsigned("0", 1, 1000), "unsigned range");
    check(parse_fraction("0.25", 0, 1) == 0.25 && parse_fraction("30", 0, 100) == 30.0, "fractions");
    check(!parse_fraction("1.5", 0, 1) && !parse_fraction("-0.1", 0, 1) && !parse_fraction("0.1.2", 0, 1) &&
              !parse_fraction("nan", 0, 1) && !parse_fraction("1e-1", 0, 1) && !parse_fraction("", 0, 1),
          "rejects malformed or out-of-range fractions");
    if (failures == 0) std::cout << "All DWE CLI parsing tests passed.\n";
    return failures == 0 ? EXIT_SUCCESS : EXIT_FAILURE;
}
