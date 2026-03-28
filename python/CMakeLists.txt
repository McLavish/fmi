cmake_minimum_required(VERSION 3.10)
project(fmi)
set(CMAKE_CXX_STANDARD 17)

add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/.." libfmi)

find_package(Python3 COMPONENTS Interpreter Development REQUIRED)
set(FMI_BOOST_PYTHON_COMPONENT "python${Python3_VERSION_MAJOR}${Python3_VERSION_MINOR}")
find_package(Boost COMPONENTS system ${FMI_BOOST_PYTHON_COMPONENT} QUIET)
if(Boost_FOUND)
    set(FMI_BOOST_PYTHON_TARGET Boost::${FMI_BOOST_PYTHON_COMPONENT})
else()
    message(WARNING "Falling back to generic Boost.Python component 'python3'. Ensure it matches Python ${Python3_VERSION_MAJOR}.${Python3_VERSION_MINOR}; distro Boost.Python packages often only support the distro default Python minor version.")
    find_package(Boost COMPONENTS system python3 REQUIRED)
    set(FMI_BOOST_PYTHON_TARGET Boost::python3)
endif()

set(CMAKE_SHARED_MODULE_PREFIX "")
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

add_library(fmi MODULE fmi_python.cpp PythonCommunicator.cpp)

target_include_directories(fmi PRIVATE ${Python3_INCLUDE_DIRS} ${FMI_INCLUDE_DIRS})

target_link_libraries(fmi ${FMI_BOOST_PYTHON_TARGET} ${Python3_LIBRARIES} FMI)
