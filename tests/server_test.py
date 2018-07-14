# -*- coding: UTF-8 -*-
################################################################################
#
# Copyright (c) 2018 Baidu.com, Inc. All Rights Reserved
#
################################################################################
"""
are/server_test.py test
"""

import time
import os
import sys
import unittest
import mock
import kazoo
import datetime
import urllib
import urllib2
from multiprocessing import Process

from kazoo.client import KazooClient

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(BASE_DIR + "/../ark/")

from are import state_service
from are import config

class TestArkServer(unittest.TestCase):
    """
    test ArkServer
    """

    @mock.patch.object(Process, "start")
    def test_start(self, mock_start):
        """
        test start
        """
        config.GuardianConfig.set({"ARK_SERVER_PORT": -1})
        mock_start.return_value = None
        s = state_service.ArkServer()
        s.start()


if __name__ == '__main__':
    unittest.main(verbosity = 2)
