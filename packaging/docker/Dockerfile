# Dockerfile
#
# This source file is part of the FoundationDB open source project
#
# Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

FROM ubuntu:18.04

# Install dependencies

RUN apt-get update && \
	apt-get install -y curl=7.58.0-2ubuntu3.6 less && \
	rm -r /var/lib/apt/lists/*

# Install FoundationDB Document Layer Binaries

ARG FDB_DOC_VERSION
ARG FDB_WEBSITE=https://www.foundationdb.org

WORKDIR /var/fdb/tmp
ADD website /mnt/website
RUN curl $FDB_WEBSITE/downloads/$FDB_DOC_VERSION/ubuntu/installers/fdb-document-layer_$FDB_DOC_VERSION-1_amd64.deb -o fdb-document-layer_$FDB_DOC_VERSION-1_amd64.deb && \
    dpkg -x fdb-document-layer_$FDB_DOC_VERSION-1_amd64.deb /var/fdb/tmp && \
    rm fdb-document-layer_$FDB_DOC_VERSION-1_amd64.deb && \
    mv /var/fdb/tmp/usr/sbin/fdbdoc /usr/bin && \
    rm -rf /var/fdb/tmp

WORKDIR /var/fdb

# This Docker image is just packing 6.0 client libraries. Document Layer works with
# any FoundationDB server >= 5.1.0. If your server version is not 6.0, then you might
# have to add the correct version client library here.
ARG FDB_CLIENT_VERSION=6.0.18
RUN curl $FDB_WEBSITE/downloads/$FDB_CLIENT_VERSION/linux/libfdb_c_$FDB_CLIENT_VERSION.so -o /usr/lib/libfdb_c.so && \
	rm -rf /mnt/website

COPY fdbdoc.bash scripts/
RUN chmod u+x scripts/*.bash && mkdir -p logs

CMD /var/fdb/scripts/fdbdoc.bash

# Runtime Configuration Options
ENV FDB_DOC_PORT 27016
ENV FDB_NETWORKING_MODE container
