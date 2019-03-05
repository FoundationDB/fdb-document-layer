FROM foundationdb/foundationdb-build:0.0.5
LABEL version=0.0.8
LABEL description="This image contains all the dependencies for FoundationDB Document Layer CI. \
For more details on how to use this image look at project README file at \
https://github.com/FoundationDB/fdb-document-layer"

RUN cd /opt/boost_1_67_0 && \
	./bootstrap.sh --prefix=./ &&\
	./b2 install --with-filesystem --with-system && \
	pip install pymongo==3.6.1 python-dateutil PyYaml==4.2b4 pytest coloredlogs==4.0.0 && \
	apt-get install -y dnsutils=1:9.9.5.dfsg-9ubuntu0.5

WORKDIR /tmp
RUN mkdir /opt/bin && \
    wget http://releases.llvm.org/7.0.1/clang+llvm-7.0.1-x86_64-linux-gnu-ubuntu-14.04.tar.xz && \
    tar -xvf clang+llvm-7.0.1-x86_64-linux-gnu-ubuntu-14.04.tar.xz && \
    cp clang+llvm-7.0.1-x86_64-linux-gnu-ubuntu-14.04/bin/clang-format /opt/bin/ && \
    rm -rf clang+llvm-7.0.1-x86_64-linux-gnu-ubuntu-14.04*

ENV PATH=$PATH:/opt/bin
