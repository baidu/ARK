# -*- coding: UTF-8 -*-
# @Time    : 2018/6/21 下午2:34
# @File    : executor_test.py
"""
executor test
"""
import mock
import unittest
import os
import sys

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(BASE_DIR + "/../ark/")

from are import exception
from are import executor
from are import framework
from are import graph


class DemoExecFuncSet(executor.BaseExecFuncSet):
    """
    test
    """
    def demo(self, params):
        """

        :param params:
        :return:
        """
        return {"code": 0}


class State1(graph.State):
    """
    state1
    """
    def process(self, session, current_node, nodes_process):
        """

        :param session:
        :param current_node:
        :param nodes_process:
        :return:
        """
        print "state1 executes finished"
        return "state2"


class State2(graph.State):
    """
    state1
    """

    def process(self, session, current_node, nodes_process):
        """

        :param session:
        :param current_node:
        :param nodes_process:
        :return:
        """
        print "state2 executes finished"
        return "ARK_NODE_END"


class TestBaseExecFuncSet(unittest.TestCase):
    """
    test base exec func set
    """
    def test_func_set(self):
        """

        :return:
        """
        demo_func = DemoExecFuncSet()
        func_list = demo_func.list_all()
        self.assertListEqual(func_list, ["demo"])

        ret = demo_func.exec_func("demo", {})
        self.assertDictEqual({"code": 0}, ret)


class TestMultiProcessExecutor(unittest.TestCase):
    """
    test multiprocess executor
    """
    @mock.patch.object(framework.Listener, "send")
    def test_multi_process_executor(self, mock_send):
        """

        :return:
        """
        mock_send.return_value = None
        exe = executor.MultiProcessExecutor()
        mes1 = framework.IDLEMessage()
        mes2 = framework.OperationMessage("DECIDED_MESSAGE", "id1", {})
        exe.on_execute_message(mes1)
        exe.on_execute_message(mes2)
        exe.on_execute_message(mes1)
        mes3 = framework.OperationMessage("SENSED_MESSAGE", "id1", {})
        self.assertRaises(exception.EUnknownEvent, exe.on_execute_message, mes3)
        exe._concerned_message_list = [
            "IDLE_MESSAGE", "DECIDED_MESSAGE", "SENSED_MESSAGE"]
        exe.on_execute_message(mes3)

        self.assertRaises(exception.ENotImplement, exe.execute_message, mes3)

    @mock.patch.object(framework.Listener, "send")
    def test_callback_executor(self, mock_send):
        """

        :return:
        """
        mock_send.return_value = None
        exe = executor.CallbackExecutor(DemoExecFuncSet())
        message = framework.OperationMessage("DECIDED_MESSAGE", "id1",
                                             {".inner_executor_key": "demo"})
        ret = exe.execute_message(message)
        self.assertDictEqual(ret, {"code": 0})

    def test_statemachine_executor(self):
        """

        :return:
        """
        nodes = [State1("state1"), State2("state2")]
        exe = executor.StateMachineExecutor(nodes)
        session = executor.StateMachineSession(id="id1")
        state_machine = graph.StateMachine(session)
        exe.graph_start(state_machine)


class TestStateMachineSession(unittest.TestCase):
    """
    test statemachine session
    """
    def test_create(self):
        """

        :return:
        """
        sts = executor.StateMachineSession(id="id1")
        self.assertEqual(sts.id, "id1")


if __name__ == "__main__":
    unittest.main()
