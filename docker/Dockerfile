FROM foundationdb/foundationdb-build:0.0.2
LABEL version=0.0.3
LABEL description="This image contains all the dependecies for FoundationDB Document Layer CI. \
For more details on how to use this image look at project README file at \
https://github.com/FoundationDB/fdb-document-layer"

RUN mkdir -p /opt && \
    cd /opt && \
    curl -L -J https://cmake.org/files/v3.12/cmake-3.12.1-Linux-x86_64.tar.gz | tar -xz && \
    curl -L -J https://dl.bintray.com/boostorg/release/1.67.0/source/boost_1_67_0.tar.gz | tar -xz && \
    cd boost_1_67_0 && \
    ./bootstrap.sh --prefix=./ &&\
    ./b2 install --with-filesystem --with-system

ENV PATH=$PATH:/opt/cmake-3.12.1-Linux-x86_64/bin

RUN pip install pymongo==3.6.1 python-dateutil PyYaml

RUN apt-get install -y dnsutils=1:9.9.5.dfsg-9ubuntu0.5
