# Standalone CRIU transparent state-transfer demo for this runbook.
#
# This is the C++ program the runbook's run-demo.sh drives end-to-end. It is not a Boost test;
# it lives with the runbook (alongside README.md, run-demo.sh, fmi.json) rather than under
# tests/. Wired into the build only when the experimental CRIU support is enabled (see the
# add_subdirectory guard in the top-level CMakeLists.txt).
add_executable(transparent_state_transfer_demo transparent_state_transfer_demo.cpp)
target_link_libraries(transparent_state_transfer_demo FMI)
