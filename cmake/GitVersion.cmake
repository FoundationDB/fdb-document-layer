find_package(Git)

if(GIT_FOUND)
    execute_process(
            COMMAND ${GIT_EXECUTABLE} rev-parse --short HEAD
            OUTPUT_VARIABLE GIT_COMMIT_HASH
            OUTPUT_STRIP_TRAILING_WHITESPACE
    )
    message(STATUS "Git Commit hash: ${GIT_COMMIT_HASH}")

    if(BUILD_PRERELEASE)
        set(BUILD_VERSION "${PROJECT_VERSION_MAJOR}.${PROJECT_VERSION_MINOR}-PRERELEASE")
    else()
        set(BUILD_VERSION "${PROJECT_VERSION}")
    endif()
    set(BUILD_PACKAGE_NAME "${PROJECT_VERSION_MAJOR}.${PROJECT_VERSION_MINOR}")

    configure_file(
            ${CMAKE_CURRENT_SOURCE_DIR}/gitVersion.h.in
            ${CMAKE_CURRENT_BINARY_DIR}/gitVersion.h
            @ONLY
    )
else()
    message(FATAL_ERROR "Git not found")
endif()
