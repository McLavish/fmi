cmake_minimum_required(VERSION 3.10)
project(FMI)
set(CMAKE_CXX_STANDARD 17)

option(FMI_ENABLE_S3 "Enable the S3 backend" ON)
option(FMI_ENABLE_REDIS "Enable the Redis backend" ON)
option(FMI_ENABLE_TCPUNCH "Enable the Direct backend (TCP NAT hole punching via TCPunch)" ON)
option(FMI_USE_STATIC_BOOST "Prefer static Boost libraries" ON)

# Direct needs TCPunch and DirectTCP needs Redis for its peer registry; with both off there is
# no peer-to-peer backend left and only the ClientServer ones (S3) can carry traffic.
if(NOT FMI_ENABLE_TCPUNCH AND NOT FMI_ENABLE_REDIS)
    message(WARNING "FMI_ENABLE_TCPUNCH=OFF and FMI_ENABLE_REDIS=OFF: no peer-to-peer backend will be available")
endif()

set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(Boost_USE_STATIC_LIBS ${FMI_USE_STATIC_BOOST})

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

get_directory_property(hasParent PARENT_DIRECTORY)
if (hasParent)
    set(FMI_INCLUDE_DIRS "${CMAKE_CURRENT_SOURCE_DIR}/include/" PARENT_SCOPE)
endif()

find_package(Boost COMPONENTS log REQUIRED)
# DrainTCP runs a control thread per channel; every backend links against the same library.
find_package(Threads REQUIRED)

if(NOT FMI_USE_STATIC_BOOST)
    add_compile_definitions(BOOST_LOG_DYN_LINK BOOST_ALL_DYN_LINK)
endif()

if(FMI_ENABLE_TCPUNCH)
    add_subdirectory(extern/TCPunch/client/)
endif()

set(FMI_SOURCES
    src/Communicator.cpp
    src/utils/Configuration.cpp
    src/utils/Signals.cpp
    src/comm/Channel.cpp
    src/comm/ClientServer.cpp
    src/comm/RecoverableClientServer.cpp
    src/utils/ChannelPolicy.cpp
    src/comm/PeerToPeer.cpp
    src/comm/OperationScope.cpp
    src/comm/SequencedLink.cpp
    src/comm/TcpChannelBase.cpp
    src/comm/TcpEndpoint.cpp
)

set(FMI_LINK_LIBS
    ${Boost_Libraries}
    Boost::log
    Threads::Threads
)

if(FMI_ENABLE_TCPUNCH)
    list(APPEND FMI_SOURCES src/comm/Direct.cpp)
    list(APPEND FMI_LINK_LIBS tcpunch)
endif()

if(FMI_ENABLE_S3)
    find_package(AWSSDK COMPONENTS s3 REQUIRED)
    list(APPEND FMI_SOURCES src/comm/S3.cpp)
    list(APPEND FMI_LINK_LIBS ${AWSSDK_LINK_LIBRARIES})
endif()

if(FMI_ENABLE_REDIS)
    find_package(hiredis REQUIRED)
    list(APPEND FMI_SOURCES src/comm/Redis.cpp src/comm/DirectTCP.cpp src/comm/DrainTCP.cpp src/comm/DrainCoordinator.cpp src/comm/PeerRegistry.cpp src/utils/MigrationTrigger.cpp)
    list(APPEND FMI_LINK_LIBS ${HIREDIS_LIBRARIES})
endif()

add_library(FMI STATIC ${FMI_SOURCES})
add_library(FMI::FMI ALIAS FMI)

target_include_directories(FMI
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    PRIVATE
        ${Boost_INCLUDE_DIRS}
)
if(FMI_ENABLE_TCPUNCH)
    target_include_directories(FMI PRIVATE ${TCPUNCH_INCLUDE_DIRS})
endif()
target_compile_definitions(FMI PRIVATE FMI_ENABLE_S3=$<BOOL:${FMI_ENABLE_S3}> FMI_ENABLE_REDIS=$<BOOL:${FMI_ENABLE_REDIS}> FMI_ENABLE_TCPUNCH=$<BOOL:${FMI_ENABLE_TCPUNCH}>)

target_link_libraries(FMI PUBLIC ${FMI_LINK_LIBS})

if(FMI_ENABLE_REDIS)
    target_include_directories(FMI PRIVATE ${HIREDIS_INCLUDE_DIRS})
endif()

#find_package(OpenMP REQUIRED)

#add_executable(client tests/client.cpp)

#target_link_libraries(client FMI OpenMP::OpenMP_CXX)

option(FMI_BUILD_TESTS "Build FMI tests" OFF)

if (FMI_BUILD_TESTS)
    add_subdirectory(tests)
endif()

# The transparent-checkpoint subject is a plain FMI application driven by criu from outside, so
# it needs no CRIU support compiled in — only a DirectTCP-capable build (hence Redis).
if (FMI_ENABLE_REDIS AND NOT hasParent)
    add_subdirectory(runbooks/criu-transparent-checkpoint)
endif()
