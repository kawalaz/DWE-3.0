#include "dwe/network/UdpSocket.h"

#include "dwe/core/Packet.h"

#include <algorithm>

#include <array>
#include <utility>

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <winsock2.h>
#include <ws2tcpip.h>
#else
#include <arpa/inet.h>
#include <netdb.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>
#endif

namespace dwe::network {
namespace {
void set_error(std::string* out, const char* message) { if (out) *out = message; }
#ifdef _WIN32
using NativeSocket = SOCKET;
constexpr NativeSocket kInvalidSocket = INVALID_SOCKET;
NativeSocket native(std::uintptr_t value) { return static_cast<NativeSocket>(value); }
#else
using NativeSocket = int;
constexpr NativeSocket kInvalidSocket = -1;
NativeSocket native(int value) { return value; }
#endif
}

UdpSocket::UdpSocket() = default;
UdpSocket::~UdpSocket() { close(); }

UdpSocket::UdpSocket(UdpSocket&& other) noexcept {
    socket_ = other.socket_;
    initialized_ = other.initialized_;
#ifdef _WIN32
    other.socket_ = static_cast<std::uintptr_t>(~0ULL);
#else
    other.socket_ = -1;
#endif
    other.initialized_ = false;
}
UdpSocket& UdpSocket::operator=(UdpSocket&& other) noexcept {
    if (this != &other) {
        close();
        socket_ = other.socket_;
        initialized_ = other.initialized_;
#ifdef _WIN32
        other.socket_ = static_cast<std::uintptr_t>(~0ULL);
#else
        other.socket_ = -1;
#endif
        other.initialized_ = false;
    }
    return *this;
}

bool UdpSocket::open(std::uint16_t port, std::string* error) {
    if (error) error->clear();
    close();
#ifdef _WIN32
    WSADATA data{};
    if (WSAStartup(MAKEWORD(2, 2), &data) != 0) {
        set_error(error, "WSAStartup failed");
        return false;
    }
    initialized_ = true;
#endif
    const auto s = ::socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
    if (s == kInvalidSocket) {
        set_error(error, "socket creation failed");
        close();
        return false;
    }
    socket_ = static_cast<decltype(socket_)>(s);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(port);
    if (::bind(s, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        set_error(error, "UDP bind failed");
        close();
        return false;
    }
#ifdef _WIN32
    // Without this, an ICMP "port unreachable" caused by an earlier send makes a later
    // recvfrom() fail with WSAECONNRESET on Windows.
#ifndef SIO_UDP_CONNRESET
#define SIO_UDP_CONNRESET _WSAIOW(IOC_VENDOR, 12)
#endif
    BOOL report_connreset = FALSE;
    DWORD bytes_returned = 0;
    WSAIoctl(s, SIO_UDP_CONNRESET, &report_connreset, sizeof(report_connreset),
             nullptr, 0, &bytes_returned, nullptr, nullptr);
#endif
    return true;
}

void UdpSocket::set_buffer_sizes(std::size_t receive_bytes, std::size_t send_bytes) {
    if (socket_ == static_cast<decltype(socket_)>(kInvalidSocket)) return;
    const int recv_value = static_cast<int>(std::min<std::size_t>(receive_bytes, 1u << 30));
    const int send_value = static_cast<int>(std::min<std::size_t>(send_bytes, 1u << 30));
#ifdef _WIN32
    setsockopt(native(socket_), SOL_SOCKET, SO_RCVBUF, reinterpret_cast<const char*>(&recv_value), sizeof(recv_value));
    setsockopt(native(socket_), SOL_SOCKET, SO_SNDBUF, reinterpret_cast<const char*>(&send_value), sizeof(send_value));
#else
    setsockopt(native(socket_), SOL_SOCKET, SO_RCVBUF, &recv_value, sizeof(recv_value));
    setsockopt(native(socket_), SOL_SOCKET, SO_SNDBUF, &send_value, sizeof(send_value));
#endif
}

int UdpSocket::receive_buffer_bytes() const {
    if (socket_ == static_cast<decltype(socket_)>(kInvalidSocket)) return 0;
    int value = 0;
#ifdef _WIN32
    int len = sizeof(value);
    if (getsockopt(native(socket_), SOL_SOCKET, SO_RCVBUF, reinterpret_cast<char*>(&value), &len) != 0) return 0;
#else
    socklen_t len = sizeof(value);
    if (getsockopt(native(socket_), SOL_SOCKET, SO_RCVBUF, &value, &len) != 0) return 0;
#endif
    return value;
}

std::uint16_t UdpSocket::local_port() const {
    if (socket_ == static_cast<decltype(socket_)>(kInvalidSocket)) return 0;
    sockaddr_in addr{};
#ifdef _WIN32
    int len = sizeof(addr);
#else
    socklen_t len = sizeof(addr);
#endif
    if (getsockname(native(socket_), reinterpret_cast<sockaddr*>(&addr), &len) != 0) return 0;
    return ntohs(addr.sin_port);
}

bool UdpSocket::send_to(const std::string& host, std::uint16_t port,
                        const std::vector<std::uint8_t>& bytes, std::string* error) {
    if (error) error->clear();
    if (port == 0) {
        set_error(error, "destination port must be between 1 and 65535");
        return false;
    }
    if (bytes.size() > core::kMaxDatagramSize) {
        set_error(error, "datagram exceeds maximum size");
        return false;
    }
    if (socket_ == static_cast<decltype(socket_)>(kInvalidSocket)) {
        set_error(error, "socket is not open");
        return false;
    }
    addrinfo hints{};
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_DGRAM;
    addrinfo* result = nullptr;
    const auto service = std::to_string(port);
    if (getaddrinfo(host.c_str(), service.c_str(), &hints, &result) != 0 || !result) {
        set_error(error, "could not resolve destination");
        return false;
    }
    const auto sent = ::sendto(native(socket_), reinterpret_cast<const char*>(bytes.data()),
#ifdef _WIN32
        static_cast<int>(bytes.size()), 0, result->ai_addr, static_cast<int>(result->ai_addrlen));
#else
        bytes.size(), 0, result->ai_addr, static_cast<socklen_t>(result->ai_addrlen));
#endif
    freeaddrinfo(result);
    if (sent < 0 || static_cast<std::size_t>(sent) != bytes.size()) {
        set_error(error, "UDP send failed");
        return false;
    }
    return true;
}

bool UdpSocket::receive(Datagram& datagram, int timeout_ms, std::string* error) {
    if (error) error->clear();  // lets callers tell a plain timeout from a real failure
    if (socket_ == static_cast<decltype(socket_)>(kInvalidSocket)) {
        set_error(error, "socket is not open");
        return false;
    }
#ifdef _WIN32
    fd_set read_set;
    FD_ZERO(&read_set);
    FD_SET(native(socket_), &read_set);
    timeval timeout{timeout_ms / 1000, (timeout_ms % 1000) * 1000};
    const int ready = select(0, &read_set, nullptr, nullptr, &timeout);
    if (ready <= 0) {
        if (ready < 0) set_error(error, "socket select failed");
        return false;
    }
#else
    pollfd pfd{native(socket_), POLLIN, 0};
    const int ready = poll(&pfd, 1, timeout_ms);
    if (ready <= 0) {
        if (ready < 0) set_error(error, "socket poll failed");
        return false;
    }
#endif
    // One byte more than the protocol maximum: a datagram that fills the whole buffer is
    // therefore known to be oversized. Reading into an exact-size buffer would silently
    // truncate it (POSIX) and make e.g. a 1500-byte datagram look like a valid 1200-byte one.
    std::array<std::uint8_t, core::kMaxDatagramSize + 1> buffer{};
    sockaddr_in from{};
#ifdef _WIN32
    int from_len = sizeof(from);
    const int count = recvfrom(native(socket_), reinterpret_cast<char*>(buffer.data()),
                               static_cast<int>(buffer.size()), 0,
                               reinterpret_cast<sockaddr*>(&from), &from_len);
    if (count == SOCKET_ERROR) {
        // Winsock reports a datagram larger than the buffer as WSAEMSGSIZE (and discards it).
        set_error(error, WSAGetLastError() == WSAEMSGSIZE ? "datagram exceeds maximum size"
                                                          : "UDP receive failed");
        return false;
    }
#else
    socklen_t from_len = sizeof(from);
    const auto count = recvfrom(native(socket_), buffer.data(), buffer.size(), 0,
                                reinterpret_cast<sockaddr*>(&from), &from_len);
    if (count < 0) {
        set_error(error, "UDP receive failed");
        return false;
    }
#endif
    if (static_cast<std::size_t>(count) > core::kMaxDatagramSize) {
        set_error(error, "datagram exceeds maximum size");
        return false;
    }
    char ip[INET_ADDRSTRLEN]{};
    inet_ntop(AF_INET, &from.sin_addr, ip, sizeof(ip));
    datagram.address = ip;
    datagram.port = ntohs(from.sin_port);
    datagram.bytes.assign(buffer.data(), buffer.data() + count);
    return true;
}

void UdpSocket::close() {
    if (socket_ != static_cast<decltype(socket_)>(kInvalidSocket)) {
#ifdef _WIN32
        closesocket(native(socket_));
        socket_ = static_cast<std::uintptr_t>(~0ULL);
#else
        ::close(native(socket_));
        socket_ = -1;
#endif
    }
#ifdef _WIN32
    if (initialized_) {
        WSACleanup();
        initialized_ = false;
    }
#endif
}
} // namespace dwe::network
