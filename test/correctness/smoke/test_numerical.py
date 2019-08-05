#
# test_numerical.py
#
# This source file is part of the FoundationDB open source project
#
# Copyright 2013-2019 Apple Inc. and the FoundationDB project authors
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

import random
import sys


def check_wr_number(collection, number):
    id = random.randint(0, sys.maxsize)
    # Make sure we first delete record if it exists
    collection.delete_many({"_id": id})
    # update/insert if needed
    collection.update_many({"_id": id}, {"$set": {"number": number}}, upsert=True)
    # read it back
    results = collection.find({"_id": id})
    for element in results:
        value = element["number"]
        assert value == number, "Written " + str(number) + " and than Read " + str(value) + ". THAT IS UNACCEPTABLE !!!"


def test_min_int(fixture_collection):
    check_wr_number(fixture_collection, int(-1 - sys.maxsize))


def test_max_int(fixture_collection):
    check_wr_number(fixture_collection, int(sys.maxsize))


def test_min_dbl(fixture_collection):
    check_wr_number(fixture_collection, sys.float_info.min)


def test_max_dbl(fixture_collection):
    check_wr_number(fixture_collection, sys.float_info.max)
