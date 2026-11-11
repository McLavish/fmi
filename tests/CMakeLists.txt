set (Boost_USE_STATIC_LIBS OFF)
find_package (Boost REQUIRED COMPONENTS unit_test_framework)
find_package(OpenMP REQUIRED)
include_directories (${Boost_INCLUDE_DIRS})

set(FMI_TEST_SOURCES channels.cpp communicator.cpp clientserver_recovery.cpp s3_backend.cpp local_drain.cpp)

add_executable (Boost_Tests_run ${FMI_TEST_SOURCES})
target_link_libraries (Boost_Tests_run ${Boost_LIBRARIES} FMI OpenMP::OpenMP_CXX)
target_compile_definitions (Boost_Tests_run PRIVATE FMI_ENABLE_S3=$<BOOL:${FMI_ENABLE_S3}> FMI_ENABLE_REDIS=$<BOOL:${FMI_ENABLE_REDIS}> FMI_ENABLE_TCPUNCH=$<BOOL:${FMI_ENABLE_TCPUNCH}>)
if(FMI_ENABLE_TCPUNCH)
    target_include_directories (Boost_Tests_run PRIVATE ${TCPUNCH_INCLUDE_DIRS})
endif()
