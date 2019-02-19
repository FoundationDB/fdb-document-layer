#!/bin/bash
#
# run-tests.bash
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
# MongoDB is a registered trademark of MongoDB, Inc.
#

set -ex

FDB_HOST_IP=$(dig +short ${FDB_HOST})

echo "docker:docker@${FDB_HOST_IP}:${FDB_PORT}" > fdb.cluster

FDB_NETWORK_OPTION_TRACE_ENABLE="" ./build/bin/fdbdoc -l 127.0.0.1:27000 -d test -VV > test.out 2> test.err &

cd test/correctness/
pytest --doclayer-port 27000 smoke/
