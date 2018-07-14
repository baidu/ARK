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
import kazoo.client

from httplib import HTTPConnection

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(BASE_DIR + "/../ark/")

from are import client
from are.exception import EFailedRequest
from are.exception import EMissingParam
from are.exception import ETypeMismatch
from are import config


class Assistant(object):
    """
    assistant
    """
    def __init__(self, status, data):
        """

        """
        self.status = status
        self.data =data
        self.msg = ""

    def read(self):
        """

        :return:
        """
        return self.data


class TestBaseClient(unittest.TestCase):
    """
    test base client
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
        client.BaseClient._instance = None

    @mock.patch.object(HTTPConnection, "request")
    @mock.patch.object(HTTPConnection, "getresponse")
    def test_http_request(self, mock_getresponse, mock_request):
        """
        test http request
        """
        cli = client.BaseClient()
        self.assertRaises(EFailedRequest, cli.http_request, "test_host", 10000, "GET", "testurl")

        mock_getresponse.return_value = Assistant(404, '{"key": "value"}')
        mock_request.return_value = None
        self.assertRaises(EFailedRequest, cli.http_request, "test_host", 10000, "GET", "testurl")

        ret = cli.http_request("test_host", 10000, "GET", "testurl",
                               response_code=[404])
        self.assertIsInstance(ret, dict)

        mock_getresponse.return_value = Assistant(200, '{"key": "value"}')
        ret = cli.http_request("test_host", 10000, "GET", "testurl")
        self.assertIsInstance(ret, dict)

        mock_getresponse.return_value = Assistant(404, '{"key": "value"}')
        ret = cli.http_request("test_host", 10000, "GET", "testurl", response_code=[404],
                               response_json=False)
        self.assertIsInstance(ret, str)


class TestZkClient(unittest.TestCase):
    """
    test ZkClient
    """

    @mock.patch.object(kazoo.client.KazooClient, "start")
    @mock.patch.object(config.GuardianConfig, "get")
    def test_zkclient_init(self, mock_get, mock_start):
        """
        test ZkClient Init
        """

        mock_start.return_value = None
        mock_get.return_value = "127.0.0.1:666"
        client.ZkClient._init = False
        zkc = client.ZkClient()

        client.ZkClient._init = True
        zkc2 = client.ZkClient()
        client.ZkClient._init = False

    @mock.patch.object(kazoo.client.KazooClient, "start")
    @mock.patch.object(config.GuardianConfig, "get")
    def test_get_zkclient(self, mock_get, mock_start):
        """
        test ZkClient get_zkclient
        """
        mock_start.return_value = None
        mock_get.return_value = "127.0.0.1:666"
        zkc = client.ZkClient()
        print "type--------,", type(zkc)
        ret = zkc.get_zkclient()
        self.assertIsInstance(ret, kazoo.client.KazooClient)

    @mock.patch.object(kazoo.client.KazooClient, "get")
    @mock.patch.object(kazoo.client.KazooClient, "start")
    @mock.patch.object(config.GuardianConfig, "get")
    def test_get_data(self, mock_get, mock_start, mock_start_2):
        """
        test ZkClient get_data
        """
        mock_start_2.return_value = ("test", "test")
        mock_start.return_value = None
        mock_get.return_value = "127.0.0.1:666"
        zkc = client.ZkClient()
        self.assertEqual("test", zkc.get_data("test"))

    @mock.patch.object(kazoo.client.KazooClient, "set")
    @mock.patch.object(kazoo.client.KazooClient, "start")
    @mock.patch.object(config.GuardianConfig, "get")
    def test_save_data(self, mock_get, mock_start, mock_set):
        """
        test ZkClient save_data
        """
        mock_set.return_value = None
        mock_start.return_value = None
        mock_get.return_value = "127.0.0.1:666"
        zkc = client.ZkClient()
        zkc.save_data("test", "test")

    @mock.patch.object(kazoo.client.KazooClient, "create")
    @mock.patch.object(kazoo.client.KazooClient, "start")
    @mock.patch.object(config.GuardianConfig, "get")
    def test_create_data(self, mock_get, mock_start, mock_create):
        """
        test ZkClient create_node
        """
        mock_create.return_value = None
        mock_start.return_value = None
        mock_get.return_value = "127.0.0.1:666"
        zkc = client.ZkClient()
        zkc.create_node("/test", "test")


class TestESClient(unittest.TestCase):
    """
    test ESClient
    """
    def setUp(self):
        """

        :return:
        """
        config.GuardianConfig.set({"ARK_ES_PORT":"80"})

    def tearDown(self):
        """

        :return:
        """
        config.GuardianConfig.delete("ARK_ES_PORT")

    @mock.patch.object(config.GuardianConfig, "get")
    def test_init(self, mock_get):
        """
        test init
        """
        mock_get.return_value = "888"
        esc = client.ESClient("xxx", "xxx")

    @mock.patch.object(config.GuardianConfig, "get")
    @mock.patch.object(client.BaseClient, "http_request")
    def test_post_data(self, mock_http_request, mock_get):
        """
        test post_data
        """
        mock_get.return_value = "888"
        mock_http_request.return_value = "test"
        esc = client.ESClient("xxx", "xxx")
        self.assertEqual("test", esc.post_data("test"))

    @mock.patch.object(config.GuardianConfig, "get")
    @mock.patch.object(client.BaseClient, "http_request")
    def test_put_data(self, mock_http_request, mock_get):
        """
        test put_data
        """
        mock_get.return_value = "888"
        mock_http_request.return_value = "test"
        esc = client.ESClient("xxx", "xxx")
        self.assertEqual("test", esc.put_data("test", "test"))

    @mock.patch.object(config.GuardianConfig, "get")
    @mock.patch.object(client.BaseClient, "http_request")
    def test_get_data(self, mock_http_request, mock_get):
        """
        test put_data
        """
        mock_get.return_value = "888"
        mock_http_request.return_value = "test"
        esc = client.ESClient("xxx", "xxx")
        self.assertEqual("test", esc.get_data_with_uid("test"))

    @mock.patch.object(config.GuardianConfig, "get")
    @mock.patch.object(client.BaseClient, "http_request")
    def test_get_data_with_condition(self, mock_http_request, mock_get):
        """
        test get_data_with_condition
        """
        mock_get.return_value = "888"
        mock_http_request.return_value = "test"
        esc = client.ESClient("xxx", "xxx")
        self.assertEqual("test", esc.get_data_with_condition({"a":"b"}))


if __name__ == '__main__':
    unittest.main(verbosity = 2)
