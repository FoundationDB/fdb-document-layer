FROM foundationdb/foundationdb-build:0.0.5
LABEL version=0.0.6
LABEL description="This image contains all the dependencies for FoundationDB Document Layer CI. \
For more details on how to use this image look at project README file at \
https://github.com/FoundationDB/fdb-document-layer"

RUN cd /opt/boost_1_67_0 && \
	./bootstrap.sh --prefix=./ &&\
	./b2 install --with-filesystem --with-system && \
	pip install pymongo==3.6.1 python-dateutil PyYaml pytest coloredlogs==4.0.0 && \
	apt-get install -y dnsutils=1:9.9.5.dfsg-9ubuntu0.5
