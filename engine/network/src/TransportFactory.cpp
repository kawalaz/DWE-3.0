#include "dwe/network/TransportFactory.h"

#include "dwe/network/UdpTransport.h"
#ifdef DWE_WITH_GNS
#include "dwe/network/GnsTransport.h"

#include <string>
#endif

namespace dwe::network {

bool transport_available(const std::string& kind) {
    if (kind == "udp") return true;
#ifdef DWE_WITH_GNS
    if (kind == "gns") return true;
#endif
    return false;
}

std::unique_ptr<ITransport> create_transport(const std::string& kind, std::string* error) {
    if (kind == "udp") return std::make_unique<UdpTransport>();
#ifdef DWE_WITH_GNS
    if (kind == "gns") return std::make_unique<GnsTransport>();
#endif
    if (error) {
        *error = kind == "gns" ? "transport 'gns' is not available: this build was made without DWE_WITH_GNS"
                               : "unknown transport '" + kind + "' (expected 'udp' or 'gns')";
    }
    return nullptr;
}

} // namespace dwe::network
