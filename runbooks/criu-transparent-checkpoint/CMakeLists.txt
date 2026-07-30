# The subject program for the transparent-checkpoint runbook.
#
# Deliberately an ORDINARY FMI application: it links FMI, constructs a Communicator, and calls
# collectives and point-to-point sends. It contains no checkpoint API, no migration hook and no
# annotation of any kind, because the claim under test is that it does not need any. Everything
# that makes a criu checkpoint survivable happens below this file, inside the library.
#
# Built as a top-level project only (see the add_subdirectory guard in the top-level
# CMakeLists.txt). It needs no CRIU support compiled into FMI — criu is applied to the process
# from outside, which is the whole point.
add_executable(fmi_checkpoint_subject checkpoint_subject.cpp)
target_link_libraries(fmi_checkpoint_subject FMI)
