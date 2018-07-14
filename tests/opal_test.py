# -*- coding: UTF-8 -*-
# @Time    : 2018/6/21 下午4:35
# @File    : opal_test.py
"""
test opal
"""
import unittest
import os
import sys

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(BASE_DIR + "/../ark/")

from opal import job
from opal import entity
from opal.driver import driver_common
from are import exception


class TestOpal(unittest.TestCase):
    """
    test job
    """
    def test_job(self):
        """

        :return:
        """
        j = job.Job()
        j.add()
        j.get("id1")
        j.pause("id1")
        j.resume("id1")
        j.cancel("id1")


class TestEntity(unittest.TestCase):
    """
    test entity
    """
    def test_entity(self):
        """

        :return:
        """
        ent = entity.Entity("host", "localhost", "ark")
        ent.get_entity()
        self.assertRaises(exception.ENotImplement, ent.get_relate_entity, "")


class TestDriver(unittest.TestCase):
    """
    test driver
    """
    def test_driver_common(self):
        """

        :return:
        """

        self.assertRaises(ImportError, driver_common.DriverFactory().
                          create_driver, "ark", "sensor")
        driver_common.DriverFactory().create_driver("bdcloud", "execute")
        driver_common.DriverFactory().create_driver("base", "execute")


if __name__ == "__main__":
    unittest.main()
