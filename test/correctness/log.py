#!/usr/bin/python
#
# log.py
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
# MongoDB is a registered trademark of MongoDB, Inc.
#

from __future__ import print_function, division, unicode_literals, \
    absolute_import

import logging
import logging.handlers
import sys

import coloredlogs

MAX_BYTES = 20000000  # 20 MB


def setup_logger(name):
    """Set up stream and file handlers."""
    root = logging.getLogger(name)
    root.setLevel(logging.DEBUG)
    root.propagate = False

    stream_handler = coloredlogs.ColoredStreamHandler(
        stream=sys.stdout,
        show_name=False,
        show_hostname=False,
        level=logging.DEBUG)

    root.addHandler(stream_handler)

    return root
