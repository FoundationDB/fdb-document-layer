## Release notes

### 1.6.3

* [#38](https://github.com/FoundationDB/fdb-document-layer/issues/38) Fix for compound index selection in query planner
* [#46](https://github.com/FoundationDB/fdb-document-layer/issues/46) Fix for query bounds with compound indexes
* [#47](https://github.com/FoundationDB/fdb-document-layer/issues/47) Better errors on large keys
* [#51](https://github.com/FoundationDB/fdb-document-layer/issues/51) `delete` commands don't create collection any more
* [#54](https://github.com/FoundationDB/fdb-document-layer/issues/54) Fix for `buildInfo` command
* [#66](https://github.com/FoundationDB/fdb-document-layer/issues/66) Fix for crash on `dropIndex` command
* [#93](https://github.com/FoundationDB/fdb-document-layer/issues/93) Adding primary index to `listIndexes()` response

### 1.6.2

* [#32](https://github.com/FoundationDB/fdb-document-layer/issues/32) Fix for Ubuntu deb packages
* [#62](https://github.com/FoundationDB/fdb-document-layer/issues/62) Fix for broken handling of `OP_QUERY` commands with mixed case

### 1.6.1

* [#2](https://github.com/FoundationDB/fdb-document-layer/issues/2) Support mutual TLS authentication
* [#13](https://github.com/FoundationDB/fdb-document-layer/issues/13) Official Dockerfile and image in Docker Hub 
* [#25](https://github.com/FoundationDB/fdb-document-layer/pull/25) Fix for CentOS 7 packages
* [#36](https://github.com/FoundationDB/fdb-document-layer/pull/36) Fix for `ordered` option in update/delete commands
