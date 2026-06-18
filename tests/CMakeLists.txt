set (Boost_USE_STATIC_LIBS OFF)
find_package (Boost REQUIRED COMPONENTS unit_test_framework)
find_package(OpenMP REQUIRED)
include_directories (${Boost_INCLUDE_DIRS})

set(FMI_TEST_SOURCES channels.cpp communicator.cpp fault_tolerance.cpp)
if(FMI_ENABLE_CRIU)
    list(APPEND FMI_TEST_SOURCES criu_fault_tolerance.cpp)
endif()

add_executable (Boost_Tests_run ${FMI_TEST_SOURCES})
target_link_libraries (Boost_Tests_run ${Boost_LIBRARIES} FMI OpenMP::OpenMP_CXX)
target_compile_definitions (Boost_Tests_run PRIVATE FMI_ENABLE_S3=$<BOOL:${FMI_ENABLE_S3}> FMI_ENABLE_REDIS=$<BOOL:${FMI_ENABLE_REDIS}>)

if(FMI_ENABLE_CRIU)
    add_executable (criu_checkpoint_demo criu_checkpoint_demo.cpp)
    target_link_libraries (criu_checkpoint_demo FMI)

    add_executable (transparent_state_transfer_demo transparent_state_transfer_demo.cpp)
    target_link_libraries (transparent_state_transfer_demo FMI)
endif()
