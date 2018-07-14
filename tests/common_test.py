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

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(BASE_DIR + "/../ark/")

import are.common
from are import exception


class TestCommon(unittest.TestCase):
    """
    测试common模块
    """

    def test_singleton_new_branch1(self):
        """
        测试SingleDict
        """

        ret = are.common.Singleton()
        self.assertIsNotNone(ret)

    def test_singleton_new_branch2(self):
        """
        测试SingleDict
        """

        are.common.Singleton._instance = "test"
        ret = are.common.Singleton()
        self.assertEqual(ret, "test")

    def test_singledict_getitem(self):
        """
        测试SingleDict
        """

        are.common.SingleDict._instance = None
        sd = are.common.SingleDict({"test":1})
        self.assertEqual(sd.__getitem__("test"), 1)

    def test_singledict_getitem_exception(self):
        are.common.SingleDict._instance = None
        sd = are.common.SingleDict({"test":1})
        self.assertRaises(exception.EMissingParam("key [test2] not exist"))

    def test_singledict_new_branc1(self):
        """
        测试SingleDict
        """

        are.common.SingleDict._instance = None
        ret = are.common.SingleDict()
        self.assertIsNotNone(ret)

    def test_singledict_new_branc2(self):
        """
        测试SingleDict
        """

        are.common.SingleDict._instance = "test"
        ret = are.common.SingleDict()
        self.assertEqual(ret, "test")

    def test_opobject__init_(self):
        """
        测试OpObject模块
        """

        opo = are.common.OpObject("test_type", "test_name", "test_refer")
        self.assertIsNotNone(opo)

    def test_opobject_type(self):
        """
        测试OpObject模块
        """

        opo = are.common.OpObject("test_type", "test_name", "test_refer")
        self.assertEqual(opo.type, "test_type")

    def test_opobject_name(self):
        """
        测试OpObject模块
        """

        opo = are.common.OpObject("test_type", "test_name", "test_refer")
        self.assertEqual(opo.name, "test_name")

    def test_opobject_refer(self):
        """
        测试OpObject模块
        """

        opo = are.common.OpObject("test_type", "test_name", "test_refer")
        self.assertEqual(opo.refer, "test_refer")

    def test_stringutil_camel_to_underline(self):
        """
        测试stringutil模块
        """

        camel_format = "camelTestName"
        underline_format = "camel_test_name"
        ret = are.common.StringUtil.camel_to_underline(camel_format)
        self.assertEqual(ret, underline_format)

    def test_stringutil_underline_to_camel(self):
        """
        测试stringutil模块
        """

        camel_format = "camelTestName"
        underline_format = "camel_test_name"
        ret = are.common.StringUtil.underline_to_camel(underline_format)
        self.assertEqual(ret, camel_format)


if __name__ == '__main__':
    unittest.main(verbosity = 2)
