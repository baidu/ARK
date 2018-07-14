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

from are import graph

from are.exception import ENotImplement
from are.exception import ETypeMismatch
from are.exception import EMissingParam
from are.exception import EFailedRequest
from are.exception import EStatusMismatch
from are.exception import EInvalidOperation
from are.exception import EUnknownNode
from are.exception import EUnInited
from are.exception import ECheckFailed
from are.exception import EUnknownEvent
from are.exception import ENodeExist

class TestBaseGraph(unittest.TestCase):
    """
    test base graph
    """
    def setUp(self):
        """

        :return:
        """
        pass

    def tearDown(self):
        """

        :return:
        """
        pass

    def test_init(self):
        """
        test graph init
        """
        gh = graph.BaseGraph({"key": "value"})
        self.assertRaises(EUnInited, gh.prepare)
        node = "test"
        self.assertRaises(ETypeMismatch, gh.add_node, node)
        node = graph.Node("test1")
        self.assertIsNone(gh.add_node(node))
        self.assertRaises(EInvalidOperation, gh.add_node, node)
        node2 = graph.Node("test2")
        gh.add_node(node2)
        self.assertRaises(EUnknownNode, gh.get_node, "test3")
        ret = gh.get_node("test2")
        self.assertIsInstance(ret, graph.Node)
        gh._status = 3
        self.assertRaises(EStatusMismatch, gh.prepare)
        gh._status = 0
        self.assertIsNone(gh.prepare())
        self.assertRaises(EStatusMismatch, gh.pause)
        gh._status = 2
        self.assertIsNone(gh.pause())
        self.assertIsNone(gh.cancel())
        gh._status = 2
        self.assertRaises(EStatusMismatch, gh.resume)
        gh._status = 3
        self.assertIsNone(gh.resume())
        self.assertEqual(gh._status, 2)

        gh2 = graph.BaseGraph({"key": "value"})
        node = graph.Node("test3")
        gh2.add_node(node)
        

class TestStateMachine(unittest.TestCase):
    """
    test state machine
    """
    def setUp(self):
        """

        :return:
        """
        pass

    def tearDown(self):
        """

        :return:
        """
        pass

    @mock.patch.object(graph.State, "process")
    @mock.patch.object(graph.State, "check")
    def test_run(self, mock_check, mock_process):
        """
        test run
        """
        state1 = graph.State("test1")
        state2 = graph.State("test2")
        sm = graph.StateMachine({"key": "value"})
        sm.add_node(state1)
        sm.add_node(state2)
        sm.prepare()
        sm._nodes_process["test1"] = True
        self.assertRaises(ECheckFailed, sm.run_next)
        sm._nodes_process["test1"] = False
        mock_check.return_value = True
        mock_process.return_value = "test_err"
        self.assertRaises(EUnknownNode, sm.run_next)
        state3 = graph.State("test3")
        state4 = graph.State("test4")
        sm2 = graph.StateMachine({"key": "value"})
        sm2.add_node(state3)
        sm2.add_node(state4)
        sm2.prepare()
        mock_check.return_value = True
        mock_process.return_value = "test4"
        self.assertIsNone(sm2.run_next())

        state5 = graph.State("test5")
        state6 = graph.State("test6")
        sm3 = graph.StateMachine({"key": "value"})
        sm3.add_node(state5)
        sm3.add_node(state6)
        sm3.prepare()
        mock_check.return_value = True
        mock_process.return_value = "ARK_NODE_END"
        self.assertIsNone(sm3.run_next())

        state7 = graph.State("test7")
        state8 = graph.State("test8")
        sm4 = graph.StateMachine({"key": "value"})
        sm4.add_node(state7)
        sm4.add_node(state8)
        sm4.prepare()
        mock_check.return_value = True
        mock_process.return_value = "ARK_NODE_END"
        self.assertIsNone(sm4.run_next())

        state9 = graph.State("test9")
        state10 = graph.State("test10")
        sm5 = graph.StateMachine({"key": "value"})
        sm5.add_node(state9)
        sm5.add_node(state10)
        sm5.prepare()
        mock_check.return_value = False
        mock_process.return_value = "ARK_NODE_END"
        self.assertRaises(ECheckFailed, sm5.run_next)


class TestDepedencyFlow(unittest.TestCase):
    """
    test depedency flow
    """
    @mock.patch.object(graph.Node, "check")
    @mock.patch.object(graph.Node, "process")
    def test_run(self, mock_process, mock_check):
        """
        test run
        """
        node1 = graph.Node("test1")
        node2 = graph.Node("test2")
        dep1 = graph.DependencyFlow({"key": "value"})
        dep1.add_node(node1)
        dep1.add_node(node2)
        dep1.prepare()
        mock_process.return_value = "test1"
        mock_check.return_value = True
        self.assertIsNone(dep1.run_next())
        self.assertIsNone(dep1.run_next())

        node3 = graph.Node("test3")
        node4 = graph.Node("test4")
        dep2 = graph.DependencyFlow({"key": "value"})
        dep2.add_node(node3)
        dep2.add_node(node4)
        dep2.prepare()
        mock_process.return_value = "testabc"
        mock_check.return_value = True
        self.assertIsNone(dep2.run_next())
        mock_process.return_value = "ARK_NODE_END"
        self.assertIsNone(dep2.run_next())

        node5 = graph.Node("test5")
        node6 = graph.Node("test6")
        dep3 = graph.DependencyFlow({"key": "value"})
        dep3.add_node(node5)
        dep3.add_node(node6)
        dep3.prepare()
        mock_process.return_value = "testabc"
        mock_check.return_value = False
        self.assertIsNone(dep3.run_next())


class TestNode(unittest.TestCase):
    """
    test node
    """
    def setUp(self):
        """

        :return:
        """
        self.node = graph.Node("test", True)

    def tearDown(self):
        """

        :return:
        """
        pass

    def test_all(self):
        """
        test Node attribute
        """
        self.assertEqual("test", self.node.name)
        self.assertTrue(self.node.reentrance)
        self.assertRaises(ENotImplement, self.node.check, {}, {}, {})
        self.assertRaises(ENotImplement, self.node.process, {}, {}, {})


class TestState(unittest.TestCase):
    """
    test state
    """
    def setUp(self):
        """

        :return:
        """
        self.state = graph.State("test1")

    def tearDown(self):
        """

        :return:
        """
        pass

    def test_all(self):
        """
        test state attribute
        """
        ret = self.state.check({}, "test1", {})
        self.assertTrue(ret)
        ret = self.state.check({}, "test2", {})
        self.assertFalse(ret)


class TestUnit(unittest.TestCase):
    """
    test unit
    """
    def setUp(self):
        """

        :return:
        """
        self.unit = graph.Unit("name")
        pass

    def tearDown(self):
        """

        :return:
        """
        pass

if __name__ == "__main__":
    unittest.main()
