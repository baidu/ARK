# -*- coding: UTF-8 -*-
# @Time    : 2018/5/24 下午3:49
# @File    : main.py
"""
guardian demo
"""
import os
import sys
sys.path.append(os.path.abspath(os.path.dirname(__file__) + '/' + '../lib/'))
from ark.assemble.localpull_keymapping import LocalPullKeyMappingGuardian
from ark.are.executor import BaseExecFuncSet
from ark.are import log


class DemoExecFuncSet(BaseExecFuncSet):
    """
    demo exec
    """
    def say_hello(self, params):
        """

        :param params:
        :return:
        """
        log.info("hello, event params:{}".format(params))
        return {}


def guardian_main(mode):
    """

    :return:
    """
    guardian = LocalPullKeyMappingGuardian(
        "../event", {"hello": "say_hello"}, "strategy", DemoExecFuncSet(), query_interval=0.1, max_queue=10)
    return guardian
