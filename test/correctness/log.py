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
