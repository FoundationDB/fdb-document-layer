cmake_minimum_required(VERSION 3.11)
project(fdb_doc_layer VERSION 1.6.3 LANGUAGES C CXX)

set(default_build_type "RelWithDbgInfo")
if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    message(STATUS "Setting build type to '${default_build_type}' as none was specified.")
    set(CMAKE_BUILD_TYPE "${default_build_type}" CACHE
            STRING "Choose the type of build." FORCE)
    # Set the possible values of build type for cmake-gui
    set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS
            "Debug" "Release" "MinSizeRel" "RelWithDebInfo")
endif()

if(DEFINED ENV{RELEASE})
    message(STATUS "Running Release build")
    set(BUILD_PRERELEASE OFF)
else()
    message(STATUS "Running Pre-release build")
    set(BUILD_PRERELEASE ON)
endif()

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

set(Boost_USE_STATIC_LIBS ON)

set(FDB_VERSION 6.0.17)

if(DEFINED ENV{IDE_BUILD})
    message(STATUS "Doing IDE build")
    set(DO_IDE_BUILD ON)
else()
    message(STATUS "Doing Normal build")
    set(DO_IDE_BUILD OFF)
endif()

if(DEFINED ENV{TLS_DISABLED})
    message(STATUS "TLS disabled")
    set(TLS_DISABLED ON)
    add_definitions(-DTLS_DISABLED)
else()
    message(STATUS "TLS enabled")
    set(TLS_DISABLED OFF)
endif()

find_package(Boost 1.67 EXACT REQUIRED COMPONENTS filesystem system)
find_package(Threads)


add_subdirectory(src)

include(cmake/CPackConfig.cmake)
include(cmake/CLangFormat.cmake)
