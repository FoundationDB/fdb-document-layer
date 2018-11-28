#
# numerical_tests.py
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

#!/usr/bin/python

import random
import sys
import util


def check_wr_number(collection, number):
    id = random.randint(0, sys.maxint)
    # Make sure we first delete record if it exists
    collection.remove({"_id": id})
    # update/insert if needed
    collection.update({"_id": id}, {"$set": {"number": number}}, upsert=True)
    # read it back
    results = collection.find({"_id": id})
    for element in results:
        value = element["number"]
        if (value != number):
            print "Written " + str(number) + " and than Read " + str(value) + ". THAT IS UNACCEPTABLE !!!"
            return False
    print "Write and than Read of " + str(number) + " is OK"
    return True


def test_min_int():
    return ("MIN_INT", int(-1 - sys.maxint), check_wr_number)


def test_max_int():
    return ("MAX_INT", int(sys.maxint), check_wr_number)


def test_min_dbl():
    return ("MIN_DBL", sys.float_info.min, check_wr_number)


def test_max_dbl():
    return ("MAX_DBL", sys.float_info.max, check_wr_number)


tests = [globals()[f] for f in dir() if f.startswith("test_")]


def test(collection, t):
    (name, number, performer) = t
    sys.stdout.write("Testing %s..." % name)
    okay = performer(collection, number)
    if okay:
        print util.alert('PASS', 'okgreen')
        return True
    print util.alert('FAIL', 'fail')
    return False


def test_all(collection1, collection2):
    print "Numerical tests only use first collection specified"
    okay = True
    # thank go through all tests and execute them
    for t in tests:
        okay = test(collection1, t()) and okay
    return okay
