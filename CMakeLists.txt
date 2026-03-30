cmake_minimum_required(VERSION 3.10)
project(FMI)
set(CMAKE_CXX_STANDARD 17)

option(FMI_ENABLE_S3 "Enable the S3 backend" ON)
option(FMI_ENABLE_REDIS "Enable the Redis backend" ON)
option(FMI_USE_STATIC_BOOST "Prefer static Boost libraries" ON)

set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(Boost_USE_STATIC_LIBS ${FMI_USE_STATIC_BOOST})

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

get_directory_property(hasParent PARENT_DIRECTORY)
if (hasParent)
    set(FMI_INCLUDE_DIRS "${CMAKE_CURRENT_SOURCE_DIR}/include/" PARENT_SCOPE)
endif()

find_package(Boost COMPONENTS log REQUIRED)
find_package(ZLIB REQUIRED)

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
    src/ft/Session.cpp
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

add_library(FMI STATIC ${FMI_SOURCES})

target_include_directories(FMI PRIVATE ${Boost_INCLUDE_DIRS} ${TCPUNCH_INCLUDE_DIRS})
target_compile_definitions(FMI PRIVATE FMI_ENABLE_S3=$<BOOL:${FMI_ENABLE_S3}> FMI_ENABLE_REDIS=$<BOOL:${FMI_ENABLE_REDIS}>)

target_link_libraries(FMI ${FMI_LINK_LIBS})

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
