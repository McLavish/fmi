# Standalone, locally runnable ports of the FMI example applications.
# Every example is one .cpp file that builds into its own executable; the shared harness
# lives in common/. Sources land here incrementally, so each target is guarded by an
# EXISTS check and picked up on the next configure.

find_package(Threads REQUIRED)

function(add_fmi_example name)
    add_executable(fmi_example_${name} ${name}.cpp)
    set_target_properties(fmi_example_${name} PROPERTIES OUTPUT_NAME ${name})
    target_include_directories(fmi_example_${name} PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/common
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
    )
    target_link_libraries(fmi_example_${name} PRIVATE FMI Threads::Threads)
endfunction()

set(FMI_EXAMPLE_NAMES
    minimal
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
    if(EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/${example_name}.cpp)
        add_fmi_example(${example_name})
    else()
        message(STATUS "FMI examples: skipping ${example_name} (${example_name}.cpp not present yet)")
    endif()
endforeach()

find_package(OpenCV QUIET COMPONENTS core imgproc imgcodecs)
if(EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/apply_blur.cpp)
    if(OpenCV_FOUND)
        add_fmi_example(apply_blur)
        target_include_directories(fmi_example_apply_blur PRIVATE ${OpenCV_INCLUDE_DIRS})
        target_link_libraries(fmi_example_apply_blur PRIVATE opencv_core opencv_imgproc opencv_imgcodecs)
    else()
        message(STATUS "FMI examples: skipping apply_blur (OpenCV core/imgproc/imgcodecs not found)")
    endif()
else()
    message(STATUS "FMI examples: skipping apply_blur (apply_blur.cpp not present yet)")
endif()
