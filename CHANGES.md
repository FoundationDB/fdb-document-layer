## Release notes


### 1.8.3

* [#214](http://github.com/FoundationDB/fdb-document-layer/pull/214) - Bump FoundationDB API version to 610. This is a workaround to avoid the 6.1 -> 6.2 upgrade issue being [fixed](https://github.com/apple/foundationdb/pull/2169) in FoundationDB.

### 1.8.2

* [#212](http://github.com/FoundationDB/fdb-document-layer/pull/212) - Avoids deadlock in connection handler over failures
* [#212](http://github.com/FoundationDB/fdb-document-layer/pull/212) - Add new connection rate metrics
* [#212](http://github.com/FoundationDB/fdb-document-layer/pull/212) - Publish CPU metrics at high priority, so we wont miss metrics under load

### 1.8.1

* [#209](https://github.com/FoundationDB/fdb-document-layer/pull/209) - CPU and memory usage metris. And Table/Index scan metrics
* [#210](https://github.com/FoundationDB/fdb-document-layer/pull/210) - Network thread CPU usage metrics

### 1.8.0

* [#94](https://github.com/FoundationDB/fdb-document-layer/issues/94) - Added `renameCollection` command
* [#129](https://github.com/FoundationDB/fdb-document-layer/issues/129) - Moved to FDB dependency 6.0 -> 6.1
* [#194](https://github.com/FoundationDB/fdb-document-layer/pull/194) - Fixed unnecessary memory copies with `DataKey` and `DataValue`.
* [#202](https://github.com/FoundationDB/fdb-document-layer/issues/202) - Fix for a memory leak on new connections

### 1.7.2

* [#185](https://github.com/FoundationDB/fdb-document-layer/pull/185) Fix for compound index selection query planner

### 1.7.1

* [#135](https://github.com/FoundationDB/fdb-document-layer/issues/135) Fix for None value handling in arrays
* [#175](https://github.com/FoundationDB/fdb-document-layer/pull/175) Fix the bug that command `dropIndex`'s error message misses some fields
* [#177](https://github.com/FoundationDB/fdb-document-layer/pull/177) Fix for `getMore` hangs on error
* [#180](https://github.com/FoundationDB/fdb-document-layer/pull/180) Fix `getDocLayerVersion` command
* [#182](https://github.com/FoundationDB/fdb-document-layer/pull/182) Fix the transaction timeout error for long running reads

### 1.7.0

* [#49](https://github.com/FoundationDB/fdb-document-layer/issues/49) Slow query logging
* [#145](https://github.com/FoundationDB/fdb-document-layer/pull/145) Added `connectionStatus` command
* [#150](https://github.com/FoundationDB/fdb-document-layer/pull/150) Removed explicit transactions. They will be added back with MongoDB v4.0 compatibility
* [#151](https://github.com/FoundationDB/fdb-document-layer/issues/151) Fixed `$addToSet` update operator to return `None` values properly with arrays
* [#154](https://github.com/FoundationDB/fdb-document-layer/pull/154) Better `update` command testing
* [#161](https://github.com/FoundationDB/fdb-document-layer/pull/161) Added more metrics
* [#165](https://github.com/FoundationDB/fdb-document-layer/pull/165) Fix `FlowLock` usage, which is causing transaction timeouts.
* [#168](https://github.com/FoundationDB/fdb-document-layer/pull/168) Deleted document count returned with correct type, `int` instead of `string`
* [#169](https://github.com/FoundationDB/fdb-document-layer/pull/169) DocLayer now returns error as `$err` as the drivers except


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
