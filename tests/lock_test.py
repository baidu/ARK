# -*- coding: UTF-8 -*-
################################################################################
#
# Copyright (c) 2018 Baidu.com, Inc. All Rights Reserved
#
################################################################################
"""
are/common.py test
"""

import os
import sys
import unittest
import mock
import kazoo
from kazoo.client import KazooClient

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(BASE_DIR + "/../ark/")

from are.lock import Lock
from are import config

class TestLock(unittest.TestCase):
    """
    测试Lock模块
    """
    def setUp(self):
        """

        :return:
        """
        config.GuardianConfig.set({"STATE_SERVICE_HOSTS": "1.1.1.1:1",
                                   "GUARDIAN_ID":"111"})

    @mock.patch.object(kazoo.client.KazooClient, "start")
    def test_init(self, mock_start):
        """
        test init
        """
        mock_start.return_value = None
        l = Lock("test")
        self.assertIsNotNone(l)

    @mock.patch.object(kazoo.client.KazooClient, "start")
    @mock.patch.object(kazoo.recipe.lock, "Lock")
    def test_create(self, mock_Lock, mock_start):
        """
        test create
        """
        mock_start.return_value = None
        mock_Lock.return_value = "test"
        l = Lock("test")
        ret = l.create()
        self.assertIsNotNone(ret)

    @mock.patch.object(kazoo.client.KazooClient, "start")
    @mock.patch.object(kazoo.recipe.lock, "Lock")
    def test_delete(self, mock_Lock, mock_start):
        """
        test delete
        """
        mock_start.return_value = None
        mock_Lock.return_value = "test"
        l = Lock("test")
        l.create()
        ret = l.delete()
        self.assertIsNone(ret)

    @mock.patch.object(kazoo.client.KazooClient, "start")
    @mock.patch.object(kazoo.recipe.lock, "Lock")
    def test_release(self, mock_Lock, mock_start):
        """
        test release
        """
        mock_start.return_value = None
        mock_Lock.return_value = "test"
        l = Lock("test")
        l.create()
        ret = l.release()
        self.assertIsNone(ret)


if __name__ == '__main__':
    unittest.main(verbosity = 2)
