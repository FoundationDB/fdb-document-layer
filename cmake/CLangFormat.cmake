find_program(CLANG_FORMAT NAMES "clang-format" "clang-format-3.6")
if(CLANG_FORMAT)
    message(STATUS "Found clang-format at ${CLANG_FORMAT}")

    # For now, it just hard codes the source files list to globs. That works
    # fine until we have another directory in `src/`. We should ideally gather
    # this from SOURCE_FILES list. But, should filter the thirs_party sources.
    # Taking a quick route for now. We should deal with it sometime down the line.
    add_custom_target(format
            COMMENT "Running clang-format"
            COMMAND ${CLANG_FORMAT} -i -style=file -fallback-style=none
            ${CMAKE_SOURCE_DIR}/src/*.cpp ${CMAKE_SOURCE_DIR}/src/*.h)

    add_custom_target(check-format
            COMMENT "Checking clang-format"
            COMMAND ! ${CLANG_FORMAT} -style=file -fallback-style=none
            --output-replacements-xml
            ${CMAKE_SOURCE_DIR}/src/*.cpp ${CMAKE_SOURCE_DIR}/src/*.h
            | grep -q "replacement offset")
endif()

