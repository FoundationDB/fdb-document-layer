#
# unit_tests.py
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

import importlib
import os
import util


def run_tests(collection1, collection2, names):
    okay = True
    for name in names:
        this_file = __file__[__file__.rfind('/') + 1:].replace('.pyc', '.py')
        if name.endswith('_tests.py') and name != this_file:
            print "\nRunning tests from module \"%s\"" % util.alert(name[:-3], 'okblue')
            m = importlib.import_module('unit.' + name[:-3])
            passed = m.test_all(collection1, collection2)
            okay = passed and okay
            print util.indent(util.alert("%s tests" % str(len(m.tests))), 1)
            print "end of \"%s\"" % util.alert(name[:-3], 'okgreen' if passed else 'fail')
    return okay


def test_all(collection1, collection2, **kwargs):
    if kwargs.get('sub_test'):
        names = ['%s_tests.py' % test.strip() for test in kwargs.get('sub_test').split(',')]
    else:
        names = os.listdir(os.path.dirname(os.path.abspath(__file__)))
    ret = run_tests(collection1, collection2, names)
    return ret
