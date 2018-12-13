find_program(Mono_EXECUTABLE_PATH mono)
if(NOT Mono_EXECUTABLE_PATH)
    message(FATAL_ERROR "Mono executable is not found!")
endif()

if(NOT DEFINED FDB_VERSION)
    message(WARNING "FDB_VERSION not set, using 5.2.5")
    set(FDB_VERSION "5.2.5")
endif()

if(NOT TLS_DISABLED)
    message(STATUS Building FDB with TLS bits)
    set(FDB_BUILD_COMMAND BOOSTDIR=${Boost_INCLUDE_DIRS} $(MAKE) flow fdb_flow fdb_c fdbmonitor FDBLibTLS fdbrpc)
    set(FDB_SERVER_BUILD_COMMAND $(MAKE) fdbserver fdbcli)
else()
    set(FDB_BUILD_COMMAND BOOSTDIR=${Boost_INCLUDE_DIRS} $(MAKE) flow fdb_flow fdb_c fdbmonitor TLS_DISABLED=1)
    set(FDB_SERVER_BUILD_COMMAND $(MAKE) fdbserver fdbcli TLS_DISABLED=1)
endif()

include(ExternalProject)
ExternalProject_Add(FoundationDB
        GIT_REPOSITORY "https://github.com/apple/foundationdb.git"
        GIT_TAG ${FDB_VERSION}

        UPDATE_COMMAND ""
        PATCH_COMMAND ""

        CONFIGURE_COMMAND ""
        BUILD_COMMAND ${FDB_BUILD_COMMAND}
        BUILD_IN_SOURCE true
        INSTALL_COMMAND ""
)

ExternalProject_Add_Step(FoundationDB server
        WORKING_DIRECTORY <SOURCE_DIR>
        EXCLUDE_FROM_MAIN 1
        COMMAND ${FDB_SERVER_BUILD_COMMAND})
ExternalProject_Add_StepTargets(FoundationDB server)

ExternalProject_Get_Property(FoundationDB source_dir)
set(ActorCompiler_EXECUTABLE_PATH ${source_dir}/bin/actorcompiler.exe)
set(Fdbserver_EXECUTABLE_PATH ${source_dir}/bin/fdbserver)
set(Fdbcli_EXECUTABLE_PATH ${source_dir}/bin/fdbcli)
set(FdbMonitor_EXECUTABLE_PATH ${source_dir}/bin/fdbmonitor)
set(Flow_INCLUDE_DIRS ${source_dir})
set(Flow_LIBRARY ${source_dir}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}flow${CMAKE_STATIC_LIBRARY_SUFFIX})
set(FdbFlow_LIBRARY ${source_dir}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}fdb_flow${CMAKE_STATIC_LIBRARY_SUFFIX})
set(FDB_C_LIBRARY ${source_dir}/lib/${CMAKE_SHARED_LIBRARY_PREFIX}fdb_c${CMAKE_SHARED_LIBRARY_SUFFIX})
if(NOT TLS_DISABLED)
    set(FDBTLS_LIBRARY ${source_dir}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}FDBLibTLS${CMAKE_STATIC_LIBRARY_SUFFIX})
    set(FDB_RPC_LIBRARY ${source_dir}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}fdbrpc${CMAKE_STATIC_LIBRARY_SUFFIX})
endif()

add_dependencies(fdbdoc FoundationDB)

function(FLOW_RUN_ACTOR_COMPILER ACTOR_G_FILES)
    cmake_parse_arguments(flow "HEADER_FILES" "" "" ${ARGN})

    set(ACTOR_FILES "${flow_UNPARSED_ARGUMENTS}")
    if(NOT ACTOR_FILES)
        message(SEND_ERROR "Error: FLOW_RUN_ACTOR_COMPILER() called without any actor files")
        return()
    endif()

    set(${ACTOR_G_FILES})
    foreach(FIL ${ACTOR_FILES})
        if(NOT ${FIL} MATCHES "\.actor\.(cpp|h|hpp)$")
            message(FATAL_ERROR "Actor files should be of format *.actor.(cpp|h|hpp). But found ${FIL}")
        endif()

        get_filename_component(ABS_FIL ${FIL} ABSOLUTE)
        get_filename_component(FIL_WE ${FIL} NAME_WE)

        if(${FIL} MATCHES "\.actor\.cpp$")
            set(_flow_actor_g_file "${CMAKE_CURRENT_BINARY_DIR}/${FIL_WE}.actor.g.cpp")
        else()
            set(_flow_actor_g_file "${CMAKE_CURRENT_BINARY_DIR}/${FIL_WE}.actor.g.h")
        endif()
        list(APPEND ${ACTOR_G_FILES} "${_flow_actor_g_file}")

        add_custom_command(
                OUTPUT "${_flow_actor_g_file}"
                COMMAND ${Mono_EXECUTABLE_PATH}
                ${ActorCompiler_EXECUTABLE_PATH}
                ${ABS_FIL} ${_flow_actor_g_file}
                DEPENDS ${ABS_FIL}
                COMMENT "Running Flow actor compiler on ${FIL}"
                VERBATIM )
    endforeach()

    set(${ACTOR_G_FILES} "${${ACTOR_G_FILES}}" PARENT_SCOPE)
endfunction()
