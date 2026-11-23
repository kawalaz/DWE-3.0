cmake_minimum_required(VERSION 3.20)
project(DWEFoundation VERSION 0.3.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(DWE_WITH_GNS "Build the EXPERIMENTAL GameNetworkingSockets transport adapter (needs the library)" OFF)

if(MSVC)
    add_compile_options(/W4 /permissive- /EHsc)
else()
    add_compile_options(-Wall -Wextra -Wpedantic)
endif()

find_package(Threads REQUIRED)

# ---------------------------------------------------------------- libraries
add_library(dwe_core
    engine/core/src/Packet.cpp
)
target_include_directories(dwe_core PUBLIC engine/core/include)

add_library(dwe_network
    engine/network/src/UdpSocket.cpp
    engine/network/src/UdpTransport.cpp
    engine/network/src/ReliableChannel.cpp
    engine/network/src/ChannelManager.cpp
    engine/network/src/RateLimiter.cpp
    engine/network/src/ConnectionManager.cpp
    engine/network/src/SimulatedNetwork.cpp
    engine/network/src/Server.cpp
    engine/network/src/ClientConnection.cpp
    engine/network/src/TransportFactory.cpp
)
target_include_directories(dwe_network PUBLIC engine/network/include)
target_link_libraries(dwe_network PUBLIC dwe_core)

if(WIN32)
    target_link_libraries(dwe_network PUBLIC ws2_32)
    target_compile_definitions(dwe_network PUBLIC NOMINMAX)
endif()

if(DWE_WITH_GNS)
    # Experimental and untested against the real library: see docs/decisions/ADR-0004-gns-adapter.md.
    find_package(GameNetworkingSockets CONFIG REQUIRED)
    target_sources(dwe_network PRIVATE engine/network/src/GnsTransport.cpp)
    target_compile_definitions(dwe_network PUBLIC DWE_WITH_GNS=1)
    if(TARGET GameNetworkingSockets::GameNetworkingSockets)
        target_link_libraries(dwe_network PUBLIC GameNetworkingSockets::GameNetworkingSockets)
    elseif(TARGET GameNetworkingSockets::shared)
        target_link_libraries(dwe_network PUBLIC GameNetworkingSockets::shared)
    elseif(TARGET GameNetworkingSockets::GameNetworkingSockets_s)
        target_link_libraries(dwe_network PUBLIC GameNetworkingSockets::GameNetworkingSockets_s)
    else()
        message(FATAL_ERROR "GameNetworkingSockets was found but exports none of the expected targets")
    endif()
endif()

# --------------------------------------------------------------- programs
add_executable(dwe_server server/runtime/main.cpp)
target_link_libraries(dwe_server PRIVATE dwe_network)

add_executable(dwe_client client/console/main.cpp)
target_link_libraries(dwe_client PRIVATE dwe_network)

add_executable(dwe_loadgen tools/loadgen/main.cpp)
target_link_libraries(dwe_loadgen PRIVATE dwe_network Threads::Threads)

# ------------------------------------------------------------------ tests
enable_testing()

add_executable(dwe_tests tests/unit/PacketTests.cpp)
target_link_libraries(dwe_tests PRIVATE dwe_core)
add_test(NAME dwe_tests COMMAND dwe_tests)

add_executable(dwe_sequence_tests tests/unit/SequenceTests.cpp)
target_link_libraries(dwe_sequence_tests PRIVATE dwe_core)
add_test(NAME dwe_sequence_tests COMMAND dwe_sequence_tests)

add_executable(dwe_cli_tests tests/unit/CliTests.cpp)
target_link_libraries(dwe_cli_tests PRIVATE dwe_core)
add_test(NAME dwe_cli_tests COMMAND dwe_cli_tests)

add_executable(dwe_reliable_tests tests/unit/ReliableChannelTests.cpp)
target_link_libraries(dwe_reliable_tests PRIVATE dwe_network)
add_test(NAME dwe_reliable_tests COMMAND dwe_reliable_tests)

add_executable(dwe_channel_tests tests/unit/ChannelManagerTests.cpp)
target_link_libraries(dwe_channel_tests PRIVATE dwe_network)
add_test(NAME dwe_channel_tests COMMAND dwe_channel_tests)

add_executable(dwe_ratelimit_tests tests/unit/RateLimiterTests.cpp)
target_link_libraries(dwe_ratelimit_tests PRIVATE dwe_network)
add_test(NAME dwe_ratelimit_tests COMMAND dwe_ratelimit_tests)

add_executable(dwe_simnet_tests tests/unit/SimulatedNetworkTests.cpp)
target_link_libraries(dwe_simnet_tests PRIVATE dwe_network)
add_test(NAME dwe_simnet_tests COMMAND dwe_simnet_tests)

add_executable(dwe_network_tests tests/unit/UdpSocketTests.cpp)
target_link_libraries(dwe_network_tests PRIVATE dwe_network)
add_test(NAME dwe_network_tests COMMAND dwe_network_tests)

add_executable(dwe_connection_tests tests/unit/ConnectionManagerTests.cpp)
target_link_libraries(dwe_connection_tests PRIVATE dwe_network)
add_test(NAME dwe_connection_tests COMMAND dwe_connection_tests)

add_executable(dwe_reliability_tests tests/integration/ReliabilityTests.cpp)
target_link_libraries(dwe_reliability_tests PRIVATE dwe_network)
add_test(NAME dwe_reliability_tests COMMAND dwe_reliability_tests)

add_executable(dwe_integration_tests tests/integration/ConnectionFlowTests.cpp)
target_link_libraries(dwe_integration_tests PRIVATE dwe_network)
add_test(NAME dwe_integration_tests COMMAND dwe_integration_tests)

add_executable(dwe_transport_contract tests/integration/TransportContractTests.cpp)
target_link_libraries(dwe_transport_contract PRIVATE dwe_network)
add_test(NAME dwe_transport_contract COMMAND dwe_transport_contract)

# Small end-to-end load run: 200 clients through a lossy, duplicating, reordering simulated network.
# Fails (non-zero exit) if any reliable message is lost, duplicated or reordered.
add_test(NAME dwe_loadgen_smoke
    COMMAND dwe_loadgen --mode sim --clients 200 --duration 5 --loss 0.1 --latency 30 --jitter 10 --dup 0.05 --reorder 0.2)

set_tests_properties(dwe_reliability_tests dwe_integration_tests dwe_transport_contract dwe_loadgen_smoke
    PROPERTIES TIMEOUT 180)
