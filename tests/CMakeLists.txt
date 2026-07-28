set (Boost_USE_STATIC_LIBS OFF)
find_package (Boost REQUIRED COMPONENTS unit_test_framework)
find_package(OpenMP REQUIRED)
include_directories (${Boost_INCLUDE_DIRS})

set(FMI_TEST_SOURCES channels.cpp clientserver_keys.cpp communicator.cpp fault_tolerance.cpp)
if(FMI_ENABLE_CRIU)
    list(APPEND FMI_TEST_SOURCES criu_fault_tolerance.cpp)
endif()

add_executable (Boost_Tests_run ${FMI_TEST_SOURCES})
target_link_libraries (Boost_Tests_run ${Boost_LIBRARIES} FMI OpenMP::OpenMP_CXX)
target_compile_definitions (Boost_Tests_run PRIVATE FMI_ENABLE_S3=$<BOOL:${FMI_ENABLE_S3}> FMI_ENABLE_REDIS=$<BOOL:${FMI_ENABLE_REDIS}> FMI_ENABLE_TCPUNCH=$<BOOL:${FMI_ENABLE_TCPUNCH}>)
if(FMI_ENABLE_TCPUNCH)
    target_include_directories (Boost_Tests_run PRIVATE ${TCPUNCH_INCLUDE_DIRS})
endif()

# Opt-in executable that demonstrates why equal operation indices do not by themselves
# establish a channel-empty migration cut. It is deliberately not part of Boost_Tests_run:
# while the protocol bug exists, the executable reports the silent payload substitution and
# exits non-zero. Build and run it explicitly when reviewing the cut protocol.
if(FMI_ENABLE_CRIU)
    find_package(Threads REQUIRED)
    add_executable(fmi_migration_p2p_cut_counterexample migration_p2p_cut_counterexample.cpp)
    target_link_libraries(fmi_migration_p2p_cut_counterexample FMI Threads::Threads)
    target_compile_definitions(
        fmi_migration_p2p_cut_counterexample
        PRIVATE FMI_SOURCE_DIR="${CMAKE_SOURCE_DIR}"
    )

endif()

# The CRIU transparent state-transfer demo is not a test; it lives with its runbook and is
# built from runbooks/local-criu-state-transfer/ (see the top-level CMakeLists.txt).

if(FMI_ENABLE_CRIU)
    find_package(Threads REQUIRED)
    # Behavioural acceptance corpus.  One translation unit per axis; each self-registers via
    # FMI_REGISTER_SCENARIOS, so authors never edit a shared list.  Add new axes here only.
    set(FMI_MIGRATION_CORPUS_SOURCES
        corpus/corpus_collectives.cpp
        corpus/corpus_divergent_p2p.cpp
        corpus/corpus_timing_targets.cpp
        corpus/corpus_sizes_framing.cpp
        corpus/corpus_failure_recovery.cpp
        corpus/corpus_backends_policy.cpp
    )

    add_executable(
        fmi_migration_counterexamples
        migration_counterexample_runner.cpp
        migration_counterexample_scenarios.cpp
        migration_cut_model.cpp
        ${FMI_MIGRATION_CORPUS_SOURCES}
    )
    target_link_libraries(fmi_migration_counterexamples FMI Threads::Threads)
    target_compile_definitions(
        fmi_migration_counterexamples
        PRIVATE FMI_SOURCE_DIR="${CMAKE_SOURCE_DIR}"
    )
endif()
