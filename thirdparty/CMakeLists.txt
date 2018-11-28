add_subdirectory(bson-cpp ${CMAKE_BINARY_DIR}/src/third-party/bson-cpp)


# We do not want to use the CMakeLists.txt that comes with PCRE project source. So we need to do it here, instead of simply add_subdirectory
add_library(pcre
        STATIC
        "pcre/pcre_byte_order.c"
        "pcre/pcre_compile.c"
        "pcre/pcre_config.c"
        "pcre/pcre_dfa_exec.c"
        "pcre/pcre_exec.c"
        "pcre/pcre_fullinfo.c"
        "pcre/pcre_get.c"
        "pcre/pcre_globals.c"
        "pcre/pcre_maketables.c"
        "pcre/pcre_newline.c"
        "pcre/pcre_ord2utf8.c"
        "pcre/pcre_refcount.c"
        "pcre/pcre_string_utils.c"
        "pcre/pcre_study.c"
        "pcre/pcre_tables.c"
        "pcre/pcre_ucd.c"
        "pcre/pcre_valid_utf8.c"
        "pcre/pcre_version.c"
        "pcre/pcre_xclass.c"
        "pcre/pcre_chartables.c"
        "pcre/pcrecpp.cc"
        "pcre/pcre_scanner.cc"
        "pcre/pcre_stringpiece.cc"
        "pcre/pcreposix.c"
        "pcre/pcre.h"
        "pcre/pcrecpp.h"
        "pcre/pcrecpparg.h"
        "pcre/pcrecpp_internal.h"
        "pcre/pcreposix.h"
        "pcre/pcre_internal.h"
        "pcre/pcre_scanner.h"
        "pcre/pcre_stringpiece.h"
        "pcre/ucp.h"
        )


set(Pcre_INCLUDE_DIRS "${CMAKE_CURRENT_SOURCE_DIR}/pcre/")

target_include_directories(pcre
        PRIVATE
        "${CMAKE_CURRENT_SOURCE_DIR}/pcre/")

set_target_properties(pcre
        PROPERTIES
        ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/pcre/lib"
        LIBRARY_OUTPUT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/pcre/lib"
        RUNTIME_OUTPUT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/pcre/bin"
        )
target_compile_definitions(pcre PRIVATE HAVE_CONFIG_H)
target_compile_options(pcre PRIVATE -fvisibility=hidden)


set(Third_party_INCLUDE_DIRS
        ${Bsoncpp_INCLUDE_DIRS}
        ${Pcre_INCLUDE_DIRS}
        PARENT_SCOPE
        )

set(Third_party_LIBS
        bsoncpp
        pcre
        PARENT_SCOPE
        )
add_dependencies(fdbdoc bsoncpp pcre)
