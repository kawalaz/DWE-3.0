// EXPERIMENTAL: see GnsTransport.h. Requires GameNetworkingSockets (build with -DDWE_WITH_GNS=ON).
#include "dwe/network/GnsTransport.h"

#include "dwe/core/Packet.h"

#include <steam/steamnetworkingsockets.h>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <utility>
#include <string>
#include <vector>

namespace dwe::network {

namespace {
std::mutex g_mutex;      // guards g_instances and g_listeners (GNS callbacks carry no user pointer)
int g_instances = 0;     // GameNetworkingSockets_Init / _Kill are process-wide: reference-count them
std::unordered_map<HSteamListenSocket, GnsTransport::Impl*> g_listeners;

void OnStatusChanged(SteamNetConnectionStatusChangedCallback_t* info);

std::string endpoint_key(const std::string& address, std::uint16_t port) {
    return address + ":" + std::to_string(port);
}

std::string address_text(const SteamNetworkingIPAddr& addr) {
    char buffer[SteamNetworkingIPAddr::k_cchMaxString];
    addr.ToString(buffer, sizeof(buffer), false);  // without the port
    return buffer;
}
} // namespace

struct GnsTransport::Impl {
    ISteamNetworkingSockets* gns{nullptr};
    HSteamListenSocket listen{k_HSteamListenSocket_Invalid};
    HSteamNetPollGroup group{k_HSteamNetPollGroup_Invalid};
    std::uint16_t port{0};
    bool server{false};
    std::unordered_map<std::string, HSteamNetConnection> by_endpoint;
    struct Peer { std::string address; std::uint16_t port; };
    std::unordered_map<HSteamNetConnection, Peer> peers;

    void remember(HSteamNetConnection conn, const SteamNetworkingIPAddr& remote) {
        Peer peer{address_text(remote), remote.m_port};
        by_endpoint[endpoint_key(peer.address, peer.port)] = conn;
        peers[conn] = std::move(peer);
    }
    void forget(HSteamNetConnection conn) {
        const auto it = peers.find(conn);
        if (it == peers.end()) return;
        by_endpoint.erase(endpoint_key(it->second.address, it->second.port));
        peers.erase(it);
    }

    void on_status(SteamNetConnectionStatusChangedCallback_t* info) {
        switch (info->m_info.m_eState) {
            case k_ESteamNetworkingConnectionState_Connecting:
                // Incoming connection on our listen socket: accept everything. Whether a peer is
                // allowed to do anything is decided by the DWE session layer (Hello / rate limits).
                if (server && info->m_info.m_hListenSocket == listen) {
                    if (gns->AcceptConnection(info->m_hConn) != k_EResultOK) {
                        gns->CloseConnection(info->m_hConn, 0, nullptr, false);
                        break;
                    }
                    gns->SetConnectionPollGroup(info->m_hConn, group);
                    remember(info->m_hConn, info->m_info.m_addrRemote);
                }
                break;
            case k_ESteamNetworkingConnectionState_ClosedByPeer:
            case k_ESteamNetworkingConnectionState_ProblemDetectedLocally:
                gns->CloseConnection(info->m_hConn, 0, nullptr, false);
                forget(info->m_hConn);
                break;
            default:
                break;
        }
    }
};

namespace {
void OnStatusChanged(SteamNetConnectionStatusChangedCallback_t* info) {
    GnsTransport::Impl* impl = nullptr;
    if (info->m_info.m_hListenSocket != k_HSteamListenSocket_Invalid) {
        std::lock_guard<std::mutex> lock(g_mutex);
        const auto it = g_listeners.find(info->m_info.m_hListenSocket);
        if (it != g_listeners.end()) impl = it->second;
    } else {
        // Connections we initiated carry the owning Impl in their user data (set in send_to()).
        impl = reinterpret_cast<GnsTransport::Impl*>(static_cast<std::intptr_t>(info->m_info.m_nUserData));
    }
    if (impl) impl->on_status(info);
}
} // namespace

GnsTransport::GnsTransport() = default;
GnsTransport::~GnsTransport() { close(); }

bool GnsTransport::open(std::uint16_t local_port, std::string* error) {
    close();
    {
        std::lock_guard<std::mutex> lock(g_mutex);
        if (g_instances == 0) {
            SteamNetworkingErrMsg message;
            if (!GameNetworkingSockets_Init(nullptr, message)) {
                if (error) *error = std::string("GameNetworkingSockets init failed: ") + message;
                return false;
            }
        }
        ++g_instances;
    }
    impl_ = std::make_unique<Impl>();
    impl_->gns = SteamNetworkingSockets();

    if (local_port != 0) {
        SteamNetworkingIPAddr address;
        address.Clear();
        address.m_port = local_port;
        SteamNetworkingConfigValue_t option;
        option.SetPtr(k_ESteamNetworkingConfig_Callback_ConnectionStatusChanged,
                      reinterpret_cast<void*>(&OnStatusChanged));
        impl_->listen = impl_->gns->CreateListenSocketIP(address, 1, &option);
        impl_->group = impl_->gns->CreatePollGroup();
        if (impl_->listen == k_HSteamListenSocket_Invalid || impl_->group == k_HSteamNetPollGroup_Invalid) {
            if (error) *error = "GameNetworkingSockets could not listen on port " + std::to_string(local_port);
            close();
            return false;
        }
        impl_->server = true;
        impl_->port = local_port;
        std::lock_guard<std::mutex> lock(g_mutex);
        g_listeners[impl_->listen] = impl_.get();
    }
    return true;
}

bool GnsTransport::send_to(const std::string& host, std::uint16_t port, const std::vector<std::uint8_t>& bytes,
                           std::string* error) {
    if (!impl_) {
        if (error) *error = "socket is not open";
        return false;
    }
    SteamNetworkingIPAddr address;
    if (!address.ParseString(host.c_str())) {
        if (error) *error = "the GNS transport needs a numeric IP address, got '" + host + "'";
        return false;
    }
    address.m_port = port;
    const std::string key = endpoint_key(address_text(address), port);

    auto it = impl_->by_endpoint.find(key);
    if (it == impl_->by_endpoint.end()) {
        if (impl_->server) {
            if (error) *error = "no GNS connection from " + key + " (a server only talks to peers that connected)";
            return false;
        }
        SteamNetworkingConfigValue_t options[2];
        options[0].SetPtr(k_ESteamNetworkingConfig_Callback_ConnectionStatusChanged, reinterpret_cast<void*>(&OnStatusChanged));
        options[1].SetInt64(k_ESteamNetworkingConfig_ConnectionUserData,
                            static_cast<std::int64_t>(reinterpret_cast<std::intptr_t>(impl_.get())));
        const HSteamNetConnection conn = impl_->gns->ConnectByIPAddress(address, 2, options);
        if (conn == k_HSteamNetConnection_Invalid) {
            if (error) *error = "GameNetworkingSockets could not connect to " + key;
            return false;
        }
        impl_->remember(conn, address);
        it = impl_->by_endpoint.find(key);
    }

    const EResult result = impl_->gns->SendMessageToConnection(
        it->second, bytes.data(), static_cast<std::uint32_t>(bytes.size()),
        k_nSteamNetworkingSend_UnreliableNoNagle, nullptr);
    if (result != k_EResultOK) {
        if (error) *error = "GameNetworkingSockets send failed (EResult " + std::to_string(static_cast<int>(result)) + ")";
        return false;
    }
    return true;
}

bool GnsTransport::receive(TransportDatagram& datagram, int timeout_ms, std::string* error) {
    if (error) error->clear();
    if (!impl_) {
        if (error) *error = "socket is not open";
        return false;
    }
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms < 0 ? 0 : timeout_ms);
    for (;;) {
        impl_->gns->RunCallbacks();  // delivers connection state changes (accept / close) on this thread

        SteamNetworkingMessage_t* message = nullptr;
        int received = 0;
        if (impl_->server) {
            received = impl_->gns->ReceiveMessagesOnPollGroup(impl_->group, &message, 1);
        } else {
            for (const auto& entry : impl_->peers) {
                received = impl_->gns->ReceiveMessagesOnConnection(entry.first, &message, 1);
                if (received > 0) break;
            }
        }
        if (received > 0 && message) {
            const auto peer = impl_->peers.find(message->m_conn);
            if (peer != impl_->peers.end() && message->m_cbSize >= 0 &&
                static_cast<std::size_t>(message->m_cbSize) <= dwe::core::kMaxDatagramSize + 1) {
                const auto* data = static_cast<const std::uint8_t*>(message->m_pData);
                datagram.bytes.assign(data, data + message->m_cbSize);
                datagram.address = peer->second.address;
                datagram.port = peer->second.port;
                message->Release();
                return true;
            }
            message->Release();  // unknown peer or oversized: drop it, keep polling
            if (error) *error = "datagram from an unknown peer or exceeding the maximum size was dropped";
            return false;
        }
        if (std::chrono::steady_clock::now() >= deadline) return false;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));  // GNS has no blocking wait: poll
    }
}

std::uint16_t GnsTransport::local_port() const { return impl_ ? impl_->port : 0; }

void GnsTransport::close() {
    if (!impl_) return;
    if (impl_->listen != k_HSteamListenSocket_Invalid) {
        {
            std::lock_guard<std::mutex> lock(g_mutex);
            g_listeners.erase(impl_->listen);
        }
        for (const auto& entry : impl_->peers) impl_->gns->CloseConnection(entry.first, 0, nullptr, false);
        impl_->gns->CloseListenSocket(impl_->listen);
        if (impl_->group != k_HSteamNetPollGroup_Invalid) impl_->gns->DestroyPollGroup(impl_->group);
    } else {
        for (const auto& entry : impl_->peers) impl_->gns->CloseConnection(entry.first, 0, nullptr, false);
    }
    impl_.reset();
    std::lock_guard<std::mutex> lock(g_mutex);
    if (--g_instances == 0) GameNetworkingSockets_Kill();
}

} // namespace dwe::network
