## Release notes

### 1.6.4

* [#84](https://github.com/FoundationDB/fdb-document-layer/issues/84) Fix for `getIndexes()`
* [#99](https://github.com/FoundationDB/fdb-document-layer/issues/99) Fix for segfault on bulk insert errors
* [#106](https://github.com/FoundationDB/fdb-document-layer/issues/106) Fix for deadlock in unique index rebuild
* [#107](https://github.com/FoundationDB/fdb-document-layer/pull/107) Index rebuild `background` information persisted with index info
* [#115](https://github.com/FoundationDB/fdb-document-layer/pull/115) Fix `versionArray` in `buildInfo`
* [#117](https://github.com/FoundationDB/fdb-document-layer/issues/117) Added support to provide FDB data center to connect to
* [#124](https://github.com/FoundationDB/fdb-document-layer/pull/124) Fix for create index from mongo shell
* [#138](https://github.com/FoundationDB/fdb-document-layer/issues/138) Fix for metadata cache inconsistency, causing missing indexes

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
