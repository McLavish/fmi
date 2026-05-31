set (Boost_USE_STATIC_LIBS OFF)
find_package (Boost REQUIRED COMPONENTS unit_test_framework)
find_package(OpenMP REQUIRED)
include_directories (${Boost_INCLUDE_DIRS})


add_executable (Boost_Tests_run channels.cpp communicator.cpp fault_tolerance.cpp criu_fault_tolerance.cpp)
target_link_libraries (Boost_Tests_run ${Boost_LIBRARIES} FMI OpenMP::OpenMP_CXX)
target_compile_definitions (Boost_Tests_run PRIVATE FMI_ENABLE_S3=$<BOOL:${FMI_ENABLE_S3}> FMI_ENABLE_REDIS=$<BOOL:${FMI_ENABLE_REDIS}>)

add_executable (criu_checkpoint_demo criu_checkpoint_demo.cpp)
target_link_libraries (criu_checkpoint_demo FMI)
