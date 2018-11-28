FROM foundationdb/foundationdb-build:0.0.2
LABEL version=0.0.1

RUN mkdir -p /opt && \
    cd /opt && \
    curl -L -J https://cmake.org/files/v3.12/cmake-3.12.1-Linux-x86_64.tar.gz | tar -xz && \
    curl -L -J https://dl.bintray.com/boostorg/release/1.67.0/source/boost_1_67_0.tar.gz | tar -xz && \
    cd boost_1_67_0 && \
    ./bootstrap.sh --prefix=./ &&\
    CC=/usr/bin/gcc CXX=/usr/bin/g++ &&\
    echo "using gcc : fdb : ${CXX} ;" >> ./tools/build/src/user-config.jam &&\
    cat ./tools/build/src/user-config.jam &&\
    ./b2 toolset=gcc-fdb install --with-filesystem --with-system

ENV PATH=$PATH:/opt/cmake-3.12.1-Linux-x86_64/bin
