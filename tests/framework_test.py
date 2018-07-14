# -*- coding: UTF-8 -*-
# @Time    : 2018/6/20 下午6:55
# @File    : framework_test.py
"""
framework test
"""
import os
import sys
import mock
import unittest
import threading
import time
import kazoo

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(BASE_DIR + "/../ark/")


from are import framework
from are import exception
from are import config
from are import ha_master
from are import client
from are import common


class TestMessage(unittest.TestCase):
    """
    test message
    """
    def test_message(self):
        """

        :return:
        """
        mes = framework.Message("IDLE_MESSAGE")
        self.assertEqual("IDLE_MESSAGE", mes.name)

        mes = framework.IDLEMessage()
        self.assertEqual("IDLE_MESSAGE", mes.name)

        mes = framework.OperationMessage("SENSED_MESSAGE", "id1", {})
        self.assertEqual("SENSED_MESSAGE", mes.name)
        self.assertEqual("id1", mes.operation_id)
        self.assertDictEqual({}, mes.params)
        mes.params = {"a":"b"}
        self.assertDictEqual({"a":"b"}, mes.params)


class TestListener(unittest.TestCase):
    """
    test listener
    """
    def test_listener(self):
        """

        :return:
        """
        listener = framework.Listener()
        listener.register(["IDLE_MESSAGE"])
        self.assertListEqual(["IDLE_MESSAGE"], listener.list())
        listener.deregister(["IDLE_MESSAGE"])
        self.assertListEqual([], listener.list())
        self.assertIsNone(listener.active())
        self.assertIsNone(listener.inactive())
        self.assertRaises(exception.ENotImplement, listener.on_message,
                          "message")
        pumb = framework.MessagePump()
        listener.bind_pump(pumb)


class TestMessagePumb(unittest.TestCase):
    """
    test message pumb
    """
    def test_messagepumb(self):
        """

        :return:
        """
        listener1 = framework.Listener()
        listener1.register(["IDLE_MESSAGE", "SENSED_MESSAGE"])
        listener2 = framework.Listener()
        pumb = framework.MessagePump()
        pumb.add_listener(listener1)
        self.assertRaises(ValueError, pumb.del_listener, listener2)
        concern = pumb.list_listener_concern(listener1)
        self.assertItemsEqual(["IDLE_MESSAGE", "SENSED_MESSAGE"], concern)

        message_operation = framework.OperationMessage(
            "SENSED_MESSAGE", "id2", {})
        message_operation2 = framework.OperationMessage(
            "DECIDED_MESSAGE", "id3", {})
        thread = threading.Thread(
            target=self.__run_thread_assistant, args=(pumb, ))
        thread.daemon = True
        thread.start()

        time.sleep(1)
        pumb.send(message_operation)
        pumb.send(message_operation2)
        time.sleep(3)
        pumb._stop_tag = True
        time.sleep(2)

        sen = framework.BaseSensor()
        dec = framework.BaseDecisionMaker()
        exe = framework.BaseExecutor()
        assistant_sensor = "demo"
        pumb._listener_list = [sen, dec, exe]
        pumb.validate_listeners()
        pumb.add_listener(dec)
        self.assertRaises(exception.ETypeMismatch, pumb.validate_listeners)
        pumb._listener_list = [sen,dec, exe, assistant_sensor]
        self.assertRaises(exception.ETypeMismatch, pumb.validate_listeners)

    def __run_thread_assistant(self, pumb):
        """

        :return:
        """
        pumb.run_loop()


class TestGuardianFramework(unittest.TestCase):
    """
    test framework
    """
    def setUp(self):
        """

        :return:
        """
        common.Singleton._instance = None

    @mock.patch.object(config.GuardianConfig, "load_config")
    @mock.patch.object(kazoo.client.KazooClient, "start")
    @mock.patch.object(client.ZkClient, "exists")
    @mock.patch.object(client.ZkClient, "create_node")
    def test_init_environ(self, mock_create,
                          mock_exists, mock_start, mock_load):
        """

        :return:
        """
        mock_create.return_value = None
        mock_exists.return_value = False
        mock_start.return_value = None
        mock_load.return_value = None
        guardian_framework = framework.GuardianFramework()
        config.GuardianConfig.set({"GUARDIAN_ID": "GUARDIAN_ID",
                                   "STATE_SERVICE_HOSTS": "aaa:888",
                                   "ARK_SERVER_PORT": "888"})
        guardian_framework.init_environment()

    @mock.patch.object(kazoo.client.KazooClient, "start")
    @mock.patch.object(kazoo.client.KazooClient, "add_listener")
    @mock.patch.object(config.GuardianConfig, "load_config")
    @mock.patch.object(ha_master.HAMaster, "create_instance")
    @mock.patch.object(ha_master.HAMaster, "choose_master")
    @mock.patch.object(framework.GuardianFramework, "run_loop")
    @mock.patch.object(framework.GuardianFramework, "init_environment")
    def test_start(self, mock_init_env, mock_run, mock_choose_master,
                   mock_create_instance, mock_load_config,
                   mock_add_listener, mock_add_start):
        """

        :return:
        """
        mock_init_env.return_value = None
        mock_run.return_value = None
        mock_choose_master.return_value = None
        mock_create_instance.return_value = None
        mock_load_config.return_value = None
        mock_add_listener.return_value = None
        mock_add_start.return_value = None
        guardian_framework = framework.GuardianFramework()
        thread = threading.Thread(
            target=self.__start, args=(guardian_framework, ))
        thread.daemon = True
        thread.start()
        time.sleep(3)
        guardian_framework._run_tag = False
        time.sleep(1)

    def __start(self, guardian):
        """

        :param guardian:
        :return:
        """
        guardian.start()
#
#     @mock.patch.object(kazoo.client.KazooClient, "start")
#     @mock.patch.object(client.ZkClient, "get_data")
#     @mock.patch.object(client.ZkClient, "save_data")
#     def test_obtain_loader(self, mock_save_data, mock_get_data, mock_start):
#         """
#
#         :return:
#         """
#         mock_save_data.return_value = None
#         mock_get_data.return_value = None
#         mock_start.return_value = None
#         guardian = framework.GuardianFramework()
#         sen = framework.BaseSensor()
#         guardian.add_listener(sen)
#         print "guardian_context----------", guardian._context
#         guardian.obtain_leader()
#         guardian.on_persistence()
#         guardian.release_leader()
#
#
# class TestBaseListener(unittest.TestCase):
#     """
#     test base listener
#     """
#     @mock.patch.object(framework.BaseExecutor, "on_execute_message")
#     @mock.patch.object(framework.Listener, "send")
#     def test_base_listener(self, mock_send, mock_on_execute):
#         """
#
#         :return:
#         """
#         message = framework.IDLEMessage()
#         sen = framework.BaseSensor()
#         self.assertRaises(exception.ENotImplement, sen.on_message, message)
#         dec = framework.BaseDecisionMaker()
#         self.assertRaises(exception.ENotImplement, dec.on_message, message)
#         exe = framework.BaseExecutor()
#         mock_on_execute.return_value = None
#         self.assertIsNone(exe.on_message(message))
#         mock_on_execute.return_value = "ret"
#         self.assertRaises(exception.ETypeMismatch, exe.on_message, message)
#         mock_on_execute.return_value = {"a": "b"}
#         self.assertRaises(exception.EMissingParam, exe.on_message, message)
#         mock_on_execute.return_value = {"EXECUTOR_OPERATION_ID": "id1"}
#         mock_send.return_value = None
#         exe.on_message(message)
#
#     def test_exe_on_execute(self):
#         """
#
#         :return:
#         """
#         message = framework.IDLEMessage()
#         exe = framework.BaseExecutor()
#         self.assertRaises(exception.ENotImplement,
#                           exe.on_execute_message, message)


if __name__ == "__main__":
    unittest.main()
