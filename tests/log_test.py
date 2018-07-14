# -*- coding: UTF-8 -*-
################################################################################
#
# Copyright (c) 2018 Baidu.com, Inc. All Rights Reserved
#
################################################################################
"""
are/log test
"""
import os
import sys
import unittest
import mock
import logging

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(BASE_DIR + "/../ark/")

from are import log
from are import config
from are import exception


class TestLoggerContext(unittest.TestCase):
    """
    test logger context
    """
    def test_logger_context(self):
        """

        :return:
        """
        log.LoggerContext._instance = None
        os.environ["LOG_DIR"] = "../log"
        os.environ["LOG_CONF_DIR"] = "../conf/"
        os.environ["MATRIX_CONTAINER_ID"] = "1.1.2"
        os.environ["OPERATION_ID"] = "1.1.2"
        lc = log.LoggerContext()
        class InnerUse(object):
            """
            inner use
            """
            def __init__(self):
                self.name = "name"
                self.age = 1

        inner_obj = InnerUse()
        lf = log.LoggerFactory.get_logger(InnerUse)
        lf.tlog(10, "test tlog")
        lf.tlog(10, "test log %s %s", inner_obj.name, inner_obj)
        lf.tsetoid("DEBUG", "test set oid", oid="oid123456")
        lf.tsetoid(logging.DEBUG, "test set oid", oid="oid123456")
        lf.tsetoid("INFO", "test set oid", oid="oid123456")
        lf.tsetoid("WARNING", "test set oid", oid="oid123456")
        lf.tsetoid("ERROR", "test set oid", oid="oid123456")
        lf.tsetoid("CRITICAL", "test set oid", oid="oid123456")
        lf.tsetoid("CRITICAL", "test set oid %s", "1234", "oid123456")
        lf.tsetoid("FATAL", "test set oid", oid="oid123456")
        lf.tsetoid("NONE", "test set oid", oid="oid123456")

    def test_logger_handler(self):
        """
        测试新handelr
        :return:
        """
        nl = log.NullLogRecord()
        self.assertIsNone(nl.__name__)

        t_file = "t_handler"
        t_lock = "../data/t_handler.lock"
        e_handler = log.LimitTimedRotatingFileHandler(t_file, when="d",
                interval=1, maxBytes=100, backupCount=7)
        e_handler.acquire() 
        e_handler.release() 
        e_handler.close()
        self.assertEqual(len(e_handler.getFilesToDelete()), 0)
        e_handler.count = 2
        e_handler.doRollover()
        e_handler.limit = True
        e_handler.doRollover()

        if os.path.exists(t_file):
            os.remove(t_file)
        if os.path.exists(t_lock):
            os.remove(t_lock)

        t_file = "t_handler.log"
        t_lock = "../data/t_handler.lock"
        if os.path.exists(t_file):
            os.remove(t_file)
        if os.path.exists(t_lock):
            os.remove(t_lock)

        s_handler = log.LimitTimedRotatingFileHandler(t_file, when="s",
                interval=1, maxBytes=1024, backupCount=1)
        m_handler = log.LimitTimedRotatingFileHandler(t_file, when="m",
                interval=1, maxBytes=1024, backupCount=1)
        h_handler = log.LimitTimedRotatingFileHandler(t_file, when="h",
                interval=1, maxBytes=10240, backupCount=7)
        d_handler = log.LimitTimedRotatingFileHandler(t_file, when="d",
                interval=1, maxBytes=1, backupCount=7)
        self.assertRaises(ValueError, log.LimitTimedRotatingFileHandler, t_file, when="e")

        t_logger = logging.getLogger("t_test")
        t_logger.addHandler(d_handler)
        t_logger.warning("test text")
        t_logger.error("test text")
        self.assertTrue(os.path.exists(t_file))
        self.assertTrue(os.path.exists(t_lock))
        os.remove(t_file)
        os.remove(t_lock)


if __name__ == "__main__":
    unittest.main()


