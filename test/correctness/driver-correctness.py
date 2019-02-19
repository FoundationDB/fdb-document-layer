#
# driver-correctness.py
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

import gen
import json
import random
import sys
import yaml


def default(obj):
    return str(obj)


def j(obj):
    return json.dumps(obj, default=default)


def push(lst, obj):
    lst.append(json.loads(json.dumps(obj, default=default)))
    return lst


def test_all(times=300):
    seed = random.random()
    sys.stderr.write("Seed: %s\n" % seed)
    tests = []

    gen.global_prng = random.Random(seed)

    push(tests, ['remove', [{}]])

    for i in (x for x in range(times * 10)):
        push(tests, ['insert', [gen.random_document(True)]])

    push(tests, ['find', [{}]])

    for i in range(0, times):
        push(tests, ['find', [gen.random_query(), {'batch_size': 2}]])

    return tests


print yaml.safe_dump({
    'host': 'localhost',
    'port': 8031,
    'database': 'test',
    'collection': 'test',
    'tests': test_all()
},
                     default_flow_style=False)
