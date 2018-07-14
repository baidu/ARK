# -*- coding: UTF-8 -*-
################################################################################
#
# Copyright (c) 2018 Baidu.com, Inc. All Rights Reserved
#
################################################################################
"""
are/config.py test
"""

import os
import sys
import unittest
import mock

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(BASE_DIR + "/../ark/")

from are import config


class TestConfig(unittest.TestCase):
    """
    测试config模块
    """

    def test_load_sys_env(self):
        """
        test load_sys_env
        """
        os.environ['TEST'] = '666'
        gc = config.GuardianConfig
        gc.load_sys_env()
        self.assertEqual(gc.get("TEST"), '666')

    def test_load_local_env(self):
        """
        test load_local_env
        """
        gc = config.GuardianConfig
        gc.load_local_env()
        self.assertEqual("ARK_SERVER_PORT" in gc.get_all(), True)


if __name__ == '__main__':
    unittest.main(verbosity=2)
