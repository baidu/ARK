# -*- coding: UTF-8 -*-
# @Time    : 2018/6/21 上午10:46
# @File    : context_test.py
"""
context test
"""
import mock
import unittest
import os
import sys
import pickle
import time

import kazoo

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(BASE_DIR + "/../ark/")

from are import context
from are import common
from are import exception
from are import client
from are import config


class TestGuardianContext(unittest.TestCase):
    """
    test guardian context
    """
    def setUp(self):
        """

        :return:
        """
        common.Singleton._instance = None

    def tearDown(self):
        """

        :return:
        """
        context.GuardianContext._context = None

    @mock.patch.object(client.ZkClient, "get_data")
    @mock.patch.object(kazoo.client.KazooClient, "start")
    @mock.patch.object(config.GuardianConfig, "get")
    def test_guardian_load(self, mock_get, mock_start, mock_get_data):
        """

        :return:
        """
        context.GuardianContext._context = None
        self.assertRaises(exception.EInvalidOperation,
                          context.GuardianContext.get_context)
        context.GuardianContext._context = 1
        self.assertEqual(1, context.GuardianContext.get_context())
        context.GuardianContext._context = None

        mock_start.return_value = None
        mock_get.return_value = "id1"
        mock_get_data.return_value = None
        ret = context.GuardianContext.load_context()
        self.assertIsInstance(ret, context.GuardianContext)

        g_ins = context.GuardianContext()
        guardian_str = pickle.dumps(g_ins)
        mock_get_data.return_value = guardian_str
        ret_2 = context.GuardianContext.load_context()
        self.assertIsInstance(ret_2, context.GuardianContext)
        context.GuardianContext._context = None

    @mock.patch.object(client.ZkClient, "get_data")
    @mock.patch.object(client.ZkClient, "save_data")
    @mock.patch.object(kazoo.client.KazooClient, "start")
    @mock.patch.object(config.GuardianConfig, "get")
    def test_save_context(self, mock_get, mock_start,
                                mock_save_data, mock_get_data):
        """

        :return:
        """
        mock_get.return_value = "id1"
        mock_start.return_value = None
        mock_save_data.return_value = None
        mock_get_data.return_value = None
        cont = context.GuardianContext()
        self.assertRaises(exception.EInvalidOperation, cont.save_context)
        cont.lock = True
        self.assertIsNone(cont.save_context())
        cont.update_lock(False)
        context.GuardianContext._context = None

    @mock.patch.object(config.GuardianConfig, "get")
    @mock.patch.object(context.GuardianContext, "save_context")
    def test_context_operation(self, mock_save_context, mock_get):
        """

        :return:
        """
        mock_get.return_value = "id1"
        mock_save_context.return_value = None
        cont = context.GuardianContext()
        operation = context.Operation("id1", {})
        cont.create_operation("id1", operation)
        ret_ope = cont.get_operation("id1")
        self.assertEqual(operation, ret_ope)
        operation2 = context.Operation("id1", {"a": "b"})
        cont.update_operation("id1", operation2)
        cont.delete_operation("id1")
        cont.update_extend({"a": "b"})
        ret_ext = cont.get_extend()
        self.assertDictEqual(ret_ext, {"a": "b"})
        cont.del_extend("a")
        ret_ext2 = cont.get_extend()
        self.assertDictEqual({}, ret_ext2)


class TestOperation(unittest.TestCase):
    """
    test operation
    """
    @mock.patch.object(context.Operation, "record_action")
    def test_operation(self, mock_process):
        """

        :return:
        """
        mock_process.return_value = None
        operation = context.Operation("id1", {})
        operation.append_period("SENSED")
        operation.add_action("node1")
        operation.update_action("node1", "FINISHED", int(time.time()))
        process = operation.actions.get_action("node1")
        self.assertEqual(process.name, "node1")
        self.assertRaises(exception.EMissingParam,
                          operation.actions.get_action, "node2")


if __name__ == "__main__":
    unittest.main()



