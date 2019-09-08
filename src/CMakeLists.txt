add_executable(fdbdoc
        BufferedConnection.h
        Cursor.h
        ConsoleMetric.h
        DocLayer.h
        DocumentError.h
        error_definitions.h
        Ext.h
        ExtCmd.h
        ExtOperator.h
        ExtStructs.h
        FPUUtils.h
        IDispatched.h
        IMetric.h
        IMetric.cpp
        Knobs.cpp
        Knobs.h
        MetadataManager.h
        Oplogger.h
        Oplogger.cpp
        OplogMonitor.h
        QLContext.h
        QLExpression.h
        QLOperations.h
        QLPredicate.h
        QLTypes.cpp
        QLTypes.h
        version.cpp Constants.cpp Constants.h)

set(ACTOR_FILES
        BufferedConnection.actor.cpp
        Cursor.actor.cpp
        ConsoleMetric.actor.cpp
        DocLayer.actor.cpp
        ExtCmd.actor.cpp
        ExtMsg.actor.cpp
        ExtOperator.actor.cpp
        ExtStructs.actor.cpp
        ExtUtil.actor.cpp
        MetadataManager.actor.cpp
        OplogMonitor.actor.cpp
        QLContext.actor.cpp
        QLExpression.actor.cpp
        QLPlan.actor.cpp
        QLPredicate.actor.cpp
        QLProjection.actor.cpp
        StatusService.actor.cpp
        ExtMsg.actor.h
        ExtUtil.actor.h
        StatusService.actor.h
        QLPlan.actor.h
        QLProjection.actor.h
        )

add_subdirectory(../thirdparty ${CMAKE_BINARY_DIR}/thirdparty)
include(${CMAKE_SOURCE_DIR}/cmake/FlowConfig.cmake)
include(${CMAKE_SOURCE_DIR}/cmake/GitVersion.cmake)
include(${CMAKE_SOURCE_DIR}/cmake/Packaging.cmake)

if(DO_IDE_BUILD)
    target_sources(fdbdoc PRIVATE ${ACTOR_FILES})
else()
    flow_run_actor_compiler(ACTOR_G_CPP_FILES ${ACTOR_FILES})
    target_compile_definitions(fdbdoc PRIVATE NO_INTELLISENSE NDEBUG)
endif()

target_include_directories(fdbdoc
        PRIVATE
        ${Third_party_INCLUDE_DIRS}
        ${CMAKE_CURRENT_BINARY_DIR} # need this for ExtUtil.actor.h and gitVersion.h
        ${Boost_INCLUDE_DIRS}
        ${Flow_INCLUDE_DIRS}
        ${CMAKE_CURRENT_SOURCE_DIR}
        )

target_compile_features(fdbdoc PRIVATE cxx_std_11)
target_sources(fdbdoc PRIVATE ${ACTOR_G_CPP_FILES})

if(NOT TLS_DISABLED)
    find_library(LibSSL_LIBRARY NAMES ${CMAKE_STATIC_LIBRARY_PREFIX}ssl${CMAKE_STATIC_LIBRARY_SUFFIX} ssl)
    find_library(LibCRYPTO_LIBRARY NAMES ${CMAKE_STATIC_LIBRARY_PREFIX}crypto${CMAKE_STATIC_LIBRARY_SUFFIX} crypto)
    find_library(LibTLS_LIBRARY NAMES ${CMAKE_STATIC_LIBRARY_PREFIX}tls${CMAKE_STATIC_LIBRARY_SUFFIX} tls)
    set(TLS_LIBS
            ${FDB_RPC_LIBRARY}
            ${FDBTLS_LIBRARY}
            ${LibTLS_LIBRARY}
            ${LibSSL_LIBRARY}
            ${LibCRYPTO_LIBRARY})
else()
    set(TLS_LIBS)
endif()

target_link_libraries(fdbdoc
        PRIVATE
        ${Third_party_LIBS}
        ${CMAKE_THREAD_LIBS_INIT}
        ${Boost_LIBRARIES}
        ${Flow_LIBRARY}
        ${FdbFlow_LIBRARY}
        ${FDB_C_LIBRARY}
        ${TLS_LIBS})

if (APPLE)
    find_library(CoreFoundation CoreFoundation)
    find_library(IOKit IOKit)
    target_link_libraries(fdbdoc PRIVATE ${CoreFoundation} ${IOKit})
    target_compile_options(fdbdoc
            PRIVATE
                -Wno-undefined-var-template
                -Wno-unknown-warning-option
                -Wno-user-defined-warnings
                -Wno-constexpr-not-const # For the sake of Boost
                -msse4.2
            )
else()
    target_link_libraries(fdbdoc PRIVATE rt dl)
    target_link_libraries(fdbdoc PRIVATE -static-libstdc++ -static-libgcc)
endif()

set_target_properties(fdbdoc
        PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin"
        )
target_compile_options(fdbdoc
        PRIVATE
            -Werror
            -Wno-error=format
            -Wno-deprecated
            -Wreturn-type
            -fvisibility=hidden
            -fno-omit-frame-pointer
        )

install(TARGETS fdbdoc RUNTIME DESTINATION bin)
install(PROGRAMS ${FdbMonitor_EXECUTABLE_PATH} DESTINATION lib/foundationdb/document)
