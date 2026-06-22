cmake_minimum_required(VERSION 3.10)
project(FMI)
set(CMAKE_CXX_STANDARD 17)

option(FMI_ENABLE_S3 "Enable the S3 backend" ON)
option(FMI_ENABLE_REDIS "Enable the Redis backend" ON)
option(FMI_ENABLE_CRIU "Build experimental CRIU checkpoint/restore support" OFF)
option(FMI_USE_STATIC_BOOST "Prefer static Boost libraries" ON)

# The CRIU single-rank state-transfer path uses the Redis-backed FT control plane; the
# Coordinator throws without it. Fail at configure time rather than letting the CRIU
# control-plane calls silently no-op when Redis is disabled.
if(FMI_ENABLE_CRIU AND NOT FMI_ENABLE_REDIS)
    message(FATAL_ERROR "FMI_ENABLE_CRIU=ON requires FMI_ENABLE_REDIS=ON (the CRIU control plane uses Redis)")
endif()

set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(Boost_USE_STATIC_LIBS ${FMI_USE_STATIC_BOOST})

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

get_directory_property(hasParent PARENT_DIRECTORY)
if (hasParent)
    set(FMI_INCLUDE_DIRS "${CMAKE_CURRENT_SOURCE_DIR}/include/" PARENT_SCOPE)
endif()

if (hasParent)
    set(FMI_BUILD_TOOLS_DEFAULT OFF)
else()
    set(FMI_BUILD_TOOLS_DEFAULT ON)
endif()

find_package(Boost COMPONENTS log REQUIRED)
find_package(ZLIB REQUIRED)

if(NOT FMI_USE_STATIC_BOOST)
    add_compile_definitions(BOOST_LOG_DYN_LINK BOOST_ALL_DYN_LINK)
endif()

add_subdirectory(extern/TCPunch/client/)

set(FMI_SOURCES
    src/Communicator.cpp
    src/utils/Configuration.cpp
    src/comm/Channel.cpp
    src/comm/ClientServer.cpp
    src/utils/ChannelPolicy.cpp
    src/comm/PeerToPeer.cpp
    src/comm/Direct.cpp
    src/ft/Coordinator.cpp
    src/ft/TransparentMigrationRuntime.cpp
)

set(FMI_LINK_LIBS
    ${Boost_Libraries}
    Boost::log
    tcpunch
)

if(FMI_ENABLE_S3)
    find_package(AWSSDK COMPONENTS s3 REQUIRED)
    list(APPEND FMI_SOURCES src/comm/S3.cpp)
    list(APPEND FMI_LINK_LIBS ${AWSSDK_LINK_LIBRARIES})
endif()

if(FMI_ENABLE_REDIS)
    find_package(hiredis REQUIRED)
    list(APPEND FMI_SOURCES src/comm/Redis.cpp)
    list(APPEND FMI_LINK_LIBS ${HIREDIS_LIBRARIES})
endif()

if(FMI_ENABLE_CRIU)
    list(APPEND FMI_SOURCES
        src/ft/experimental/CriuExec.cpp
        src/ft/experimental/LocalRankAgent.cpp
    )
endif()

add_library(FMI STATIC ${FMI_SOURCES})
add_library(FMI::FMI ALIAS FMI)

target_include_directories(FMI
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    PRIVATE
        ${Boost_INCLUDE_DIRS}
        ${TCPUNCH_INCLUDE_DIRS}
)
target_compile_definitions(FMI PRIVATE FMI_ENABLE_S3=$<BOOL:${FMI_ENABLE_S3}> FMI_ENABLE_REDIS=$<BOOL:${FMI_ENABLE_REDIS}>)

if(FMI_ENABLE_CRIU)
    target_compile_definitions(FMI PUBLIC FMI_ENABLE_CRIU)
endif()

target_link_libraries(FMI PUBLIC ${FMI_LINK_LIBS})

if(FMI_ENABLE_REDIS)
    target_include_directories(FMI PRIVATE ${HIREDIS_INCLUDE_DIRS})
endif()

#find_package(OpenMP REQUIRED)

#add_executable(client tests/client.cpp)

#target_link_libraries(client FMI OpenMP::OpenMP_CXX)

option(FMI_BUILD_TESTS "Build FMI tests" OFF)
option(FMI_BUILD_TOOLS "Build FMI tools" ${FMI_BUILD_TOOLS_DEFAULT})

if (FMI_BUILD_TESTS)
    add_subdirectory(tests)
endif()

if (FMI_BUILD_TOOLS)
    add_subdirectory(tools)
endif()
