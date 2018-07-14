# -*- coding: UTF-8 -*-
# @Time    : 2018/7/2 下午1:41
# @File    : hamaster_test.py
"""
hamaster test
"""
import os
import sys

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(BASE_DIR + "/../ark/")

import unittest
import mock
import kazoo
from are import config
from are import ha_master


class TestHAMaster(unittest.TestCase):
    """
    test master
    """

    @mock.patch.object(kazoo.client.KazooClient, "start")
    @mock.patch.object(config.GuardianConfig, "get")
    @mock.patch.object(kazoo.client.KazooClient, "add_listener")
    def test_init(self, mock_add, mock_get, mock_start):
        """
        test HAMaster init
        """
        mock_start.return_value = None
        mock_get.return_value = "127.0.0.1:666"
        mock_add.return_value = None
        def startfunc():
            """
            mock func
            """
            return "start"

        def stopfunc():
            """
            mock func
            """
            return "stop"

        ham = ha_master.HAMaster(startfunc, stopfunc)

    @mock.patch.object(kazoo.client.KazooClient, "create")
    @mock.patch.object(kazoo.client.KazooClient, "start")
    @mock.patch.object(config.GuardianConfig, "get")
    @mock.patch.object(kazoo.client.KazooClient, "add_listener")
    def test_create_instance(self, mock_add, mock_get, mock_start, mock_create):
        """
        test create_instance
        """
        mock_start.return_value = None
        mock_get.return_value = "127.0.0.1:666"
        mock_add.return_value = None

        def startfunc():
            """
            mock func
            """
            return "start"

        def stopfunc():
            """
            mock func
            """
            return "stop"

        ham = ha_master.HAMaster(startfunc, stopfunc)
        ham.create_instance()

    @mock.patch.object(kazoo.client.KazooClient, "get_children")
    @mock.patch.object(kazoo.client.KazooClient, "create")
    @mock.patch.object(kazoo.client.KazooClient, "start")
    @mock.patch.object(config.GuardianConfig, "get")
    @mock.patch.object(kazoo.client.KazooClient, "add_listener")
    def test_choose_master(self, mock_add, mock_get, \
            mock_start, mock_create, mock_get_children):
        """
        test choose_master
        """
        mock_get_children.return_value = ["test#test"]
        mock_start.return_value = None
        mock_get.return_value = "127.0.0.1:666"
        mock_add.return_value = None

        def startfunc():
            """
            mock func
            """
            return "start"

        def stopfunc():
            """
            mock func
            """
            return "stop"

        ham = ha_master.HAMaster(startfunc, stopfunc)
        ham.choose_master()

    @mock.patch.object(kazoo.client.KazooClient, "get_children")
    @mock.patch.object(kazoo.client.KazooClient, "create")
    @mock.patch.object(kazoo.client.KazooClient, "start")
    @mock.patch.object(config.GuardianConfig, "get")
    @mock.patch.object(kazoo.client.KazooClient, "add_listener")
    def test_choose_master2(self, mock_add, mock_get, \
            mock_start, mock_create, mock_get_children):
        """
        test choose_master
        """
        mock_get_children.return_value = ["test#test"]
        mock_start.return_value = None
        mock_get.return_value = "127.0.0.1:666"
        mock_add.return_value = None

        def startfunc():
            """
            mock func
            """
            return "start"

        def stopfunc():
            """
            mock func
            """
            return "stop"

        ham = ha_master.HAMaster(startfunc, stopfunc)
        ham.is_leader = True
        ham.choose_master()

    @mock.patch.object(kazoo.client.KazooClient, "get_children")
    @mock.patch.object(kazoo.client.KazooClient, "create")
    @mock.patch.object(kazoo.client.KazooClient, "start")
    @mock.patch.object(config.GuardianConfig, "get")
    @mock.patch.object(kazoo.client.KazooClient, "add_listener")
    def test_choose_master3(self, mock_add, mock_get, \
            mock_start, mock_create, mock_get_children):
        """
        test choose_master
        """
        mock_get_children.return_value = ["test#test"]
        mock_start.return_value = None
        mock_get.return_value = "127.0.0.1:666"
        mock_add.return_value = None

        def startfunc():
            """
            mock func
            """
            return "start"

        def stopfunc():
            """
            mock func
            """
            return "stop"

        ham = ha_master.HAMaster(startfunc, stopfunc)
        ham.choose_master()

    @mock.patch.object(kazoo.client.KazooClient, "get_children")
    @mock.patch.object(kazoo.client.KazooClient, "create")
    @mock.patch.object(kazoo.client.KazooClient, "start")
    @mock.patch.object(config.GuardianConfig, "get")
    @mock.patch.object(kazoo.client.KazooClient, "add_listener")
    def test_choose_master4(self, mock_add, mock_get, \
            mock_start, mock_create, mock_get_children):
        """
        test choose_master
        """
        mock_get_children.return_value = ["test#test"]
        mock_start.return_value = None
        mock_get.return_value = "127.0.0.1:666"
        mock_add.return_value = None

        def startfunc():
            """
            mock func
            """
            return "start"

        def stopfunc():
            """
            mock func
            """
            return "stop"

        ham = ha_master.HAMaster(startfunc, stopfunc)
        ham.is_leader = True
        ham.choose_master()

    @mock.patch.object(kazoo.client.KazooClient, "get_children")
    @mock.patch.object(kazoo.client.KazooClient, "create")
    @mock.patch.object(kazoo.client.KazooClient, "start")
    @mock.patch.object(config.GuardianConfig, "get")
    @mock.patch.object(kazoo.client.KazooClient, "add_listener")
    def test_state_listener(self, mock_add, mock_get, \
            mock_start, mock_create, mock_get_children):
        """
        test state_listener
        """
        mock_get_children.return_value = ["test#test"]
        mock_start.return_value = None
        mock_get.return_value = "127.0.0.1:666"
        mock_add.return_value = None

        def startfunc():
            """
            mock func
            """
            return "start"

        def stopfunc():
            """
            mock func
            """
            return "stop"

        ham = ha_master.HAMaster(startfunc, stopfunc)
        ham.state_listener(kazoo.client.KazooState.LOST)
        ham.state_listener(kazoo.client.KazooState.SUSPENDED)
        ham.state_listener(kazoo.client.KazooState.CONNECTED)
        ham.state_listener("UNKNOW")

    @mock.patch.object(kazoo.client.KazooClient, "get_children")
    @mock.patch.object(kazoo.client.KazooClient, "create")
    @mock.patch.object(kazoo.client.KazooClient, "start")
    @mock.patch.object(config.GuardianConfig, "get")
    @mock.patch.object(kazoo.client.KazooClient, "add_listener")
    def test_event_watcher(self, mock_add, mock_get, \
            mock_start, mock_create, mock_get_children):
        """
        test event_watcher
        """
        mock_get_children.return_value = ["test#test"]
        mock_start.return_value = None
        mock_get.return_value = "127.0.0.1:666"
        mock_add.return_value = None

        def startfunc():
            """
            mock func
            """
            return "start"

        def stopfunc():
            """
            mock func
            """
            return "stop"

        ham = ha_master.HAMaster(startfunc, stopfunc)

        class Event(object):
            """
            mock Event
            """
            def __init__(self, value):
                self.state = value
                self.type  = value

        e = Event("CONNECTED")
        ham.event_watcher(e)

        e = Event("UNKNOW")
        ham.event_watcher(e)
