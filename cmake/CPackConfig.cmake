# Common CPack packaging setup
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "FoundationDB Document Layer")
set(CPACK_PACKAGE_VERSION_MAJOR ${PROJECT_VERSION_MAJOR})
set(CPACK_PACKAGE_VERSION_MINOR ${PROJECT_VERSION_MINOR})
set(CPACK_PACKAGE_VERSION_PATCH ${PROJECT_VERSION_PATCH})
set(CPACK_RESOURCE_FILE_LICENSE "${CMAKE_CURRENT_SOURCE_DIR}/LICENSE")
set(CPACK_RESOURCE_FILE_README "${CMAKE_CURRENT_SOURCE_DIR}/README.md")
set(CPACK_PACKAGE_NAME fdb-document-layer)
set(CPACK_PACKAGE_HOMEPAGE_URL https://github.com/FoundationDB/fdb-document-layer)
set(CPACK_PACKAGE_VENDOR FoundationDB)
set(CPACK_OUTPUT_FILE_PREFIX packages)


set(CPACK_PACKAGE_DESCRIPTION "
The FoundationDB Document Layer is a stateless microserver which
supports the MongoDB® wire protocol and implements a subset of the
MongoDB® API. All stateful operations are delegated to the FoundationDB
Key-Value Store.

This package contains only the Document Layer. The FoundationDB Client
Library must be installed separately.

Disclaimer: MongoDB is a registered trademark of MongoDB, Inc.")

set(CPACK_GENERATOR TGZ)

include(CPack)
