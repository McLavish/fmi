# Standalone, locally runnable ports of the FMI example applications.
# Every example is one self-contained .cpp file that builds into its own executable and links
# directly against FMI; the shared launcher (CLI parsing, local multi-rank spawning, teardown)
# and the comparison helpers live in common/.

function(add_fmi_example name)
    add_executable(fmi_example_${name} ${name}.cpp)
    set_target_properties(fmi_example_${name} PROPERTIES OUTPUT_NAME ${name})
    target_include_directories(fmi_example_${name} PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/common
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
    )
    target_link_libraries(fmi_example_${name} PRIVATE FMI)
endfunction()

set(FMI_EXAMPLE_NAMES
    avg
    communicating
    crashing
    ring
    jacobi
    npb_ep
    mantevo_hpccg
    mixed_workload
    checkpoint_workload
    long_communicating_checkpoint
    fixed_size_ckpt_comm
    fixed_size_ckpt_sleep
)

foreach(example_name ${FMI_EXAMPLE_NAMES})
    add_fmi_example(${example_name})
endforeach()
