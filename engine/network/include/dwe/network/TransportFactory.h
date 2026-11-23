#pragma once

#include "dwe/network/ITransport.h"

#include <memory>
#include <string>

namespace dwe::network {

// Creates a transport by name. "udp" is always available; "gns" only when the library was built
// with DWE_WITH_GNS (see docs/decisions/ADR-0004-gns-adapter.md). Returns nullptr and sets
// `error` for an unknown or unavailable transport.
std::unique_ptr<ITransport> create_transport(const std::string& kind, std::string* error = nullptr);
bool transport_available(const std::string& kind);

} // namespace dwe::network
