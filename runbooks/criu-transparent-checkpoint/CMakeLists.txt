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
# One app shape per file under shapes/, each self-registering via FMI_REGISTER_SHAPE. Globbed on
# purpose: adding a shape must mean adding one file and editing nothing else. CONFIGURE_DEPENDS
# makes the build re-glob when the directory changes (it needs CMake 3.12; on older CMake a new
# shape file only appears after a manual re-run of cmake).
if (CMAKE_VERSION VERSION_LESS 3.12)
    file(GLOB FMI_CHECKPOINT_SHAPES "${CMAKE_CURRENT_SOURCE_DIR}/shapes/*.cpp")
else()
    file(GLOB FMI_CHECKPOINT_SHAPES CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/shapes/*.cpp")
endif()
if (NOT FMI_CHECKPOINT_SHAPES)
    message(FATAL_ERROR "no app shapes found in ${CMAKE_CURRENT_SOURCE_DIR}/shapes/")
endif()

# The shape files are compiled straight into the executable rather than into a static library:
# nothing references their registrars, so a library would let the linker drop them.
add_executable(fmi_checkpoint_subject
    checkpoint_subject.cpp
    shapes.cpp
    ${FMI_CHECKPOINT_SHAPES})
target_link_libraries(fmi_checkpoint_subject FMI)
