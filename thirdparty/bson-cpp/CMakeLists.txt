add_library(bsoncpp
        STATIC
        "bsonobj.cpp"
        "lib/base64.cpp"
        "lib/md5.c"
        "lib/nonce.cpp"
        "oid.cpp"
        "bson-inl.h"
        "bson.h"
        "bsonassert.h"
        "bsonelement.h"
        "bsonmisc.h"
        "bsonobj.h"
        "bsonobjbuilder.h"
        "bsonobjiterator.h"
        "bsontypes.h"
        "bson_db.h"
        "inline_decls.h"
        "lib/atomic_int.h"
        "lib/base64.h"
        "lib/md5.h"
        "lib/md5.hpp"
        "lib/nonce.h"
        "oid.h"
        "ordering.h"
        "stringdata.h"
        "util/builder.h"
        "util/embedded_builder.h"
        "util/hex.h"
        "util/misc.h"
        "util/optime.h"
        "util/time_support.h"
        )

set(Bsoncpp_INCLUDE_DIRS
        ${CMAKE_CURRENT_LIST_DIR}
        PARENT_SCOPE
        )
target_include_directories(bsoncpp
        PRIVATE
        ${CMAKE_CURRENT_LIST_DIR}
        ${Boost_INCLUDE_DIRS})

set_target_properties(bsoncpp
        PROPERTIES
        ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/lib"
        LIBRARY_OUTPUT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/lib"
        RUNTIME_OUTPUT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/bin"
        )
target_compile_options(bsoncpp PRIVATE -fvisibility=hidden)
