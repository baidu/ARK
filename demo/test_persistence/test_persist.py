# -*- coding: UTF-8 -*-

import ark.are.config as config
import ark.are.common as common
import ark.are.log as log
import ark.are.exception as exception
import ark.are.persistence as persistence
import sys
import unittest
import time


def make_driver(driver_cls):
    """

    :param driver_cls:
    :return:
    :rtype: persistence.BasePersistence
    """
    if driver_cls == persistence.FilePersistence:
        driver_conf = {"STATE_SERVICE_HOSTS": "/tmp/persist"}
    elif driver_cls == persistence.RedisPersistence:
        driver_conf = {"STATE_SERVICE_HOSTS": "redis://127.0.0.1:6379/0"}
    elif driver_cls == persistence.ZkPersistence:
        driver_conf = {"STATE_SERVICE_HOSTS": "redis://127.0.0.1:6379/0"}
    else:
        raise exception.ETypeMismatch("Unknown persistence class")

    cfg = {"LOG_CONF_DIR": "./", "LOG_ROOT": "./log"}
    cfg.update(driver_conf)
    config.GuardianConfig.set(cfg)
    dv = driver_cls()
    return dv


class TestRedisPersist(common.ParametrizedTestCase):
    """

    """
    driver = None
    watch_list = {}

    def setUp(self):
        persist_cls = self.param
        self.driver = make_driver(persist_cls)
        time.sleep(0.5)
        self.watch_list = {}
        print("\n---env prepare over---\n")

    def tearDown(self):
        self.driver.disconnect()
        print("\n---env clean over---\n")

    def test_create(self):
        self.driver.create_node("/gur1/inst/inst_", "{}", True, True, True)
        result = self.driver.get_children("/gur1/inst")
        self.assertEqual(len(result), 1)
        if not self.driver.exists("/gur1/context"):
            self.driver.create_node("/gur1/context", '{"": ""}', False, False, True)
        else:
            self.driver.save_data("/gur1/context", '{"": ""}')
        result = self.driver.get_data("/gur1/context")
        self.assertEqual(result, '{"": ""}')
        result = self.driver.get_children("/gur1")
        self.assertEqual(len(result), 2)

    # 该方法默认在create之后执行
    def test_delete(self):
        if self.driver.exists("/gur1/context"):
            self.driver.delete_node("/gur1/context")
        self.assertFalse(self.driver.exists("/gur1/context"))
        self.driver.create_node("/gur1/context", '{"": ""}', False, False, True)
        result = self.driver.get_children("/gur1")
        self.assertEqual(len(result), 2)
        # 等待临时节点超时
        print("\n---waiting for 6 second for node-timeout---\n")
        time.sleep(6)
        result = self.driver.get_children("/gur1/inst")
        self.assertEqual(len(result), 0)
        self.driver.delete_node("/gur1")
        self.assertFalse(self.driver.exists("/gur1"))

    def test_watch(self):
        def watcher(event):
            self.watch_list[event.path] = event.type

        self.driver.get_children("/gur1/inst", watcher)
        result = self.driver.create_node("/gur1/inst/inst_", "{}", True, True, True)
        time.sleep(2)
        self.assertEqual(self.watch_list["/gur1/inst"], "CREATED")


if __name__ == '__main__':
    suite = unittest.TestSuite()
    suite.addTest(common.ParametrizedTestCase.parametrize(TestRedisPersist, param=persistence.RedisPersistence))
    unittest.TextTestRunner(verbosity=2).run(suite)
