# FoundationDB Document Layer

The FoundationDB Document Layer is a stateless microserver that exposes a document-oriented database API. The Document Layer speaks the MongoDB® wire protocol, allowing the use of the MongoDB® API via existing MongoDB® client bindings. All persistent data are stored in the FoundationDB Key-Value Store.

The Document Layer implements a subset of the MongoDB® API (v 3.0.0) with some [differences](https://foundationdb.github.io/fdb-document-layer/known-differences.html). This subset is mainly focused on CRUD operations, indexes and transactions. The Document Layer works with all the latest official MongoDB® drivers.

As the Document Layer is built on top of FoundationDB, it inherits the strong guarantees of FoundationDB. Causal
consistency and strong consistency are the default mode of operation.
Indexes are always consistent with the inserts. Shard keys are not
needed as data distribution is taken care by FoundationDB backend
automatically.

You can find more details at the documentation [here](https://foundationdb.github.io/fdb-document-layer)

## Developer Guide

The Document Layer is written in [Flow](https://github.com/apple/foundationdb/blob/master/flow/README.md) C++ just like FoundationDB.

### Dependencies

#### Boost

We depend on Boost 1.67 for Boost.DLL. Even though the DLL is a header only library, it depends on the non-header only libraries - filesystem and system. You can setup Boost as below:

```
curl -L -J -O https://dl.bintray.com/boostorg/release/1.67.0/source/boost_1_67_0.tar.gz && \
tar -xzf boost_1_67_0.tar.gz && \
cd boost_1_67_0 && \
./bootstrap.sh --prefix=./ && \
echo "using gcc : fdb : ${CXX} ;" >> ./tools/build/src/user-config.jam && \
cat ./tools/build/src/user-config.jam && \
./b2 toolset=gcc-fdb install --with-filesystem --with-system
```

and set the `BOOST_ROOT` environment variable. This is how CMake build picks the Boost packages.

#### FoundationDB

We depend on FoundationDB for the flow, fdb_flow and fdb_c libraries. CMake should get and build FoundationDB libraries from GitHub automatically as part of the build.

#### Mono

Flow actor files needs to be precompiled with Flow actor compiler which generates regular C++ code. Flow actor compiler needs Mono to run.

### Build

Build files are written in CMake. Its best to keep the build directory separate. CMake scripts get the FoundationDB sources from GitHub and build them. So, first ever build after you cloned the repo might take a while.

```
$ mkdir build && cd build && cmake .. && make
$ ./build/fdb_doc_layer -l 127.0.0.1:27017 -VV
0Using cluster file: /usr/local/etc/foundationdb/fdb.cluster
Connected to cluster.
FdbDocServer (1.5): listening on 127.0.0.1:27017

```

Note that, the Document Layer connects to the FoundationDB cluster to persist documents. If you don't provide any cluster file, it tries to find the cluster file in the default locations. If you have installed FoundationDB on your box, this should work just fine. Otherwise, one can pass the cluster file with the `-C` option.

### IDE Setup

CMake generates project files for different IDEs. Due to Flow actor compiler syntax, there could be quite a few false positive static analysis errors. CMake build scripts are written to make auto-complete work in IDEs, with bit of setup. CLion is one IDE that supports CMake build scripts as project files. To setup CLion for this project, follow the steps below:

* Open `CMakeLists.txt` as project file from CLion. This should generate make and project files from CMake scripts in `cmake-build-debug` directory.
* Set the CMake environment variables in CLion preferences
    * Set `BOOST_ROOT` to Boost installation path - Only if Boost is not installed in the default system directories
    * Set `IDE_BUILD` to `ON` - This makes CMake scripts behave slightly different to make auto-complete work with actor compiler key words
* In the top right, in the "Run/Debug configurations" box select "FoundationDB" and build. This would fetch and build the FoundationDB source

NOTE: This setup helps you to make auto-complete work in CLion. You won't be able to build from CLion itself. Binaries will still need to be built from the terminal using the command specified above.

## Contributing

Contributing to the FoundationDB Document Layer can be in contributions to the code base, sharing your experience and insights in the community on the Forums. Please see the [contributing guide](CONTRIBUTING.md) for more specifics.

## License and Trademarks
The FoundationDB Document Layer is under the [Apache License, Version 2.0](LICENSE).

FoundationDB is a registered trademark of Apple, Inc.. MongoDB is a registered trademark of MongoDB, Inc..

For additional information, see the [LICENSE](LICENSE) and [ACKNOWLEDGMENTS](ACKNOWLEDGEMENTS) files.
